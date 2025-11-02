// enforce x86 or apple silicon for now sense our structures are alignemnt are curerntly handed by
// memory.
//
// see:
//
// https://stackoverflow.com/questions/69765617/why-does-x86-allows-for-unaligned-accesses-and-how-unaligned-accesses-can-be-de/69767603
//
// note that apple silicon has similar guarnatees

/*
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_vendor = "apple")
))]
compile_error!("This crate only supports x86, x86_64, and Apple Silicon (aarch64-apple)");
*/

// https://dirname.github.io/rust-std-doc/nomicon/lifetimes.html
extern crate alloc;

use alloc::alloc::{Layout, alloc_zeroed, dealloc};
use alloc::sync::Arc;
use core::marker::PhantomData;
use core::ptr::NonNull;
use core::slice;
use core::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ArenaError {
    #[error("arena out of memory")]
    OOM,
    #[error("undefined error occurred")]
    Undefined,
}

const ARENA_SIZE_BYTES: usize = 4096;

#[repr(C)]
struct RuntimeSizedByteArray<'a> {
    ptr: *mut u8,
    size: usize,
    _marker: PhantomData<&'a mut [u8]>,
}

impl<'a> RuntimeSizedByteArray<'a> {
    fn new(size: usize, bytes: &'a mut [u8]) -> Option<RuntimeSizedByteArray<'a>> {
        if bytes.len() == size {
            let ptr = bytes.as_mut_ptr();
            Some(RuntimeSizedByteArray {
                ptr,
                size,
                _marker: PhantomData,
            })
        } else {
            None
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.size) }
    }
}

unsafe impl<'a> Send for RuntimeSizedByteArray<'a> {}
unsafe impl<'a> Sync for RuntimeSizedByteArray<'a> {}

#[repr(C)]
pub struct RuntimeSizedNode<'a> {
    pub key: &'a mut [u8; 32],
    pub val: RuntimeSizedByteArray<'a>,
    size: u8,
}

impl<'a> RuntimeSizedNode<'a> {
    fn new(data: &'a mut [u8], val_size: usize) -> Option<RuntimeSizedNode<'a>> {
        if data.len() < 32 + val_size {
            return None;
        }

        let (key_bytes, after_key) = data.split_at_mut(32);
        let key: &'a mut [u8; 32] = key_bytes.try_into().ok()?;
        let (val_byte_array, _) = after_key.split_at_mut(val_size);
        let rtsba = RuntimeSizedByteArray::new(val_size, val_byte_array)?;

        Some(RuntimeSizedNode {
            key,
            val: rtsba,
            size: val_size as u8,
        })
    }
}

unsafe impl<'a> Send for RuntimeSizedNode<'a> {}
unsafe impl<'a> Sync for RuntimeSizedNode<'a> {}

#[repr(C)]
pub struct Arena<'a> {
    data: NonNull<u8>,
    layout: Layout,
    current_index: AtomicUsize,
    memory_usage: AtomicUsize,
    _marker: PhantomData<&'a mut u8>,
}

impl<'a> Arena<'a> {
    pub fn new() -> Arc<Self> {
        unsafe {
            let layout = Layout::from_size_align_unchecked(ARENA_SIZE_BYTES, 8);
            let ptr = alloc_zeroed(layout);

            if ptr.is_null() {
                panic!("allocation failed");
            }

            Arc::new(Arena {
                data: NonNull::new_unchecked(ptr),
                layout,
                current_index: AtomicUsize::new(0),
                memory_usage: AtomicUsize::new(0),
                _marker: PhantomData,
            })
        }
    }

    fn as_slice_mut(&self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.data.as_ptr(), ARENA_SIZE_BYTES) }
    }

    pub fn create_node(&'a self, val_size: usize) -> Result<RuntimeSizedNode<'a>, ArenaError> {
        let total_size = 32 + val_size;

        let start = self.current_index.fetch_add(total_size, Ordering::SeqCst);

        if start + total_size > ARENA_SIZE_BYTES {
            return Err(ArenaError::OOM);
        }

        self.memory_usage.fetch_add(total_size, Ordering::Relaxed);

        let slice = &mut self.as_slice_mut()[start..start + total_size];
        RuntimeSizedNode::new(slice, val_size).ok_or(ArenaError::Undefined)
    }

    pub fn allocate_slice(&'a self, size: usize) -> Result<&'a mut [u8], ArenaError> {
        let start = self.current_index.fetch_add(size, Ordering::SeqCst);

        if start + size > ARENA_SIZE_BYTES {
            return Err(ArenaError::OOM);
        }

        self.memory_usage.fetch_add(size, Ordering::Relaxed);

        Ok(&mut self.as_slice_mut()[start..start + size])
    }

    pub fn remaining_capacity(&self) -> usize {
        ARENA_SIZE_BYTES.saturating_sub(self.current_index.load(Ordering::Relaxed))
    }

    pub fn destroy(arena: Arc<Arena>) -> Result<(), Arc<Arena>> {
        match Arc::try_unwrap(arena) {
            Ok(arena) => {
                unsafe {
                    core::ptr::write_bytes(arena.data.as_ptr(), 0, ARENA_SIZE_BYTES);
                    dealloc(arena.data.as_ptr(), arena.layout);
                }
                Ok(())
            }
            Err(arc) => Err(arc),
        }
    }
}

impl Drop for Arena<'_> {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

unsafe impl Send for Arena<'_> {}
unsafe impl Sync for Arena<'_> {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_arena_allocation() {
        let arena = Arena::new();
        assert_eq!(arena.current_index.load(Ordering::Relaxed), 0);
        assert_eq!(arena.memory_usage.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_node_allocation_from_arena() {
        let arena = Arena::new();
        let node = arena.create_node(3).expect("failed to create node");
        assert_eq!(node.key.len(), 32);
        assert_eq!(node.val.as_slice(), &[0, 0, 0]);
    }

    #[test]
    fn test_concurrent_allocations() {
        use alloc::vec::Vec;
        extern crate std;
        use std::thread;

        let arena = Arena::new();
        let mut handles = Vec::new();
        for _ in 0..40 {
            let arena_clone = Arc::clone(&arena);
            let handle = thread::spawn(move || {
                let _ = arena_clone.create_node(8);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(arena.memory_usage.load(Ordering::Relaxed), 1600); // 32*40+(40*8)
    }

    #[test]
    fn test_oom_behavior() {
        let arena = Arena::new();
        let mut allocated = 0;
        while allocated < ARENA_SIZE_BYTES {
            if arena.create_node(8).is_err() {
                break;
            }
            allocated += 40; // 32 + 8
        }
        let result = arena.create_node(8);
        assert!(matches!(result, Err(ArenaError::OOM)));
    }
}

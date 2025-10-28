# log structured merge tree

## libraries

### arena

contiguous custom bump allocator for nodes with both keys and no alloc data stores

features
* contiguous
* `no_alloc` dynamically sized arrays and nodes

### wal (write ahead log)

write ahead log for sequence windows that batch write `.wal` files to disk

features
* transaction durablity guarantees through `crc32` cyclical redundancy checks
* transaction windowed aggregations
* transaction batch writes
* client acknowledgements that transactions have been succesfully flushed to disk

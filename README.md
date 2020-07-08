# crystalstore

TODO: Write a description here

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     crystalstore:
       github: threefoldtech/crystalstore
   ```

2. Run `shards install`

## Usage

```crystal
require "crystalstore"
```

## Usage

###### Initialize store library

- `store = Crystal::Store.new`
- `store = Crystal::Store.new block_size: 1024, max_no_blocks: 20000000, max_no_files: 20000000, max_file_name: 1000, mount_flags: 0, db_name: "mydb", db_namespace: "ns", db_unixsocket="/tmp/bcdb.sock"`

# crystalstore

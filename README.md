# crystalstore

A library that creates a virtual file system over [BCDB](https://github.com/threefoldtech/bcdb)
It can be used directly or through a fuse interface

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     crystalstore:
       github: crystaluniverse/crystalstore
   ```

2. Run `shards install`

## Usage

##### Download, compile & run 0-db (Backend for BCDB)
- `git clone git@github.com:threefoldtech/0-db.git`
- `cd 0-db && make`
- `./zdb --mode seq`

##### Download, compile & run BCDB (Backend for BCDB)
- Install [Rust programming language](https://www.rust-lang.org/tools/install)
- `git clone git@github.com:threefoldtech/bcdb.git`
- `cd bcdb && make`
- copy bcdb binary anywhere `cp bcdb/target/x86_64-unknown-linux-musl/release/bcdb .`
- download `tfuser` utility from [here](https://github.com/crystaluniverse/bcdb-client/releases/download/v0.1/tfuser)
- use `tfuser` to register your 3bot user to explorer and generate seed file `usr.seed` using `./tfuser id create --name {3bot_username.3bot} --email {email}`
- run bcdb : `./bcdb --seed-file user.seed `
- now you can talk to `bcdb` through http via unix socket `/tmp/bcdb.sock`

##### Use the library in your application

- **Initialize** use default settings or provide your own.

 - `store = CrystalStore::Store.new`
 - `store = CrystalStore::Store.new block_size: 1024_u64, max_no_blocks: 20000000_u64, max_no_files: 20000000_u64, max_filename: 1000_u64, mount_flags: 0_u64, db_name: "db", db_namespace: ns, db_unixsocket: "/tmp/bcdb.sock"`
 - note that initializing store with certain block size makes it permanent you can not re-initialize with different size , however if you re-initialize store instance with same settings used before no problem about that so you could do something like 
 ```
 store = CrystalStore::Store.new
 store = CrystalStore::Store.new
 ```

 - Complete list of API are in the file [crystalstore.cr](./src/crystalstore.cr)
 - Examples in the file [crystalstore_spec.cr](./spec/crystalstore_spec.cr)

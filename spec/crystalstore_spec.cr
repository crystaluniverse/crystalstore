require "./spec_helper"

require "uuid"

describe CrystalStore::Store do

  it "initialization" do
    store = CrystalStore::Store.new
    
    begin
      store = CrystalStore::Store.new
    rescue CrystalStore::InitializationError
      raise Exception.new "should not have raised exception"
    end

    ns = "#{UUID.random.to_s}"
    store = CrystalStore::Store.new block_size: 1024_u64, max_no_blocks: 20000000_u64, max_no_files: 20000000_u64, max_filename: 1000_u64, mount_flags: 0_u64, db_name: "db", db_namespace: ns, db_unixsocket: "/tmp/bcdb.sock"

    # same namespace, same block size no problem
    begin
      store = CrystalStore::Store.new block_size: 1024_u64, max_no_blocks: 20000000_u64, max_no_files: 20000000_u64, max_filename: 1000_u64, mount_flags: 0_u64, db_name: "db", db_namespace: ns, db_unixsocket: "/tmp/bcdb.sock"
    rescue CrystalStore::InitializationError
      raise Exception.new "should not have raised exception"
    end

    # same namespace, different block size .. A problem!
    begin
      store = CrystalStore::Store.new block_size: 1025_u64, max_no_blocks: 20000000_u64, max_no_files: 20000000_u64, max_filename: 1000_u64, mount_flags: 0_u64, db_name: "db", db_namespace: ns, db_unixsocket: "/tmp/bcdb.sock"
      raise Exception.new "should have raised exception"
    rescue CrystalStore::InitializationError
    end

    # shorter filenames .. A problem!
    begin
      store = CrystalStore::Store.new block_size: 1024_u64, max_no_blocks: 20000000_u64, max_no_files: 20000000_u64, max_filename: 100_u64, mount_flags: 0_u64, db_name: "db", db_namespace: ns, db_unixsocket: "/tmp/bcdb.sock"
      raise Exception.new "should have raised exception"
    rescue CrystalStore::InitializationError
    end

    # longer filenames, no problem!
    begin
      store = CrystalStore::Store.new block_size: 1024_u64, max_no_blocks: 20000000_u64, max_no_files: 20000000_u64, max_filename: 2000_u64, mount_flags: 0_u64, db_name: "db", db_namespace: ns, db_unixsocket: "/tmp/bcdb.sock"
    rescue CrystalStore::InitializationError
      raise Exception.new "should not have raised exception"
    end

  end

  it "works" do
    db_namespace = "a-#{UUID.random.to_s}"
    store = CrystalStore::Store.new db_namespace: db_namespace
    
    # creating /a/b should fail
    begin
      store.dir_create path: "/a/b"
      raise Exception.new "should have failed"
    rescue CrystalStore::FileNotFoundError
    end

    #creating /a
    store.dir_create path: "/a"

    #try creating same dir again should fail
    begin
      store.dir_create path: "/a"
      raise Exception.new "should have failed"
    rescue CrystalStore::FileExistsError
    end

    
    # create /a/b should succeed
    store.dir_create path: "/a/b"

    # create /a/b/c
    store.dir_create path: "/a/b/c"

    # deleted /a/b
    store.dir_delete("/a/b")

    # create /a/b again
    store.dir_create path: "/a/b"
    
    # create /m
    store.dir_create "/m"

    # create /n
    store.dir_create "/n"
 
    #  "copying"
    store.dir_copy("/n", "/m/n")

    store.dir_delete("/m/n")
    
    "copying"
    store.dir_copy("/n", "/m/n")

    # create /m/n/k
    store.dir_create path: "/m/n/k"

    store.dir_list("/m/n").dirs.size.should eq 1

    store.dir_list("/n").dirs.size.should eq 0

    store.dir_copy("/n", "/m/n", overwrite=true)

    store.dir_list("/m/n").dirs.size.should eq 0

    # create /aa
    store.dir_create path: "/aa"

    # moving
    store.dir_move("/aa", "/bb")
    
    # create /aa
    store.dir_create path: "/aa"

    #  moving
    store.dir_move("/aa", "/bb", ovewrite=true)

    store.dir_create path: "/aa"

    #  moving

    begin
      store.dir_move("/aa", "/bb", ovewrite=false)
      raise Exception.new "should have failed"
    rescue CrystalStore::FileExistsError
    end

    # File creation & reading
    name =  "/#{UUID.random.to_s}"
    store.file_create name, 0, 0, "text/html"
    file = store.file_open name, 0, 0
    
    block_size = 1024*1024
    data = "b" * 10*1024*1024
    
    io =  IO::Memory.new data
    IO.copy(io, file)
    
    file.close

    file = store.file_open name, 0, 0
    file.size.should eq data.size
    
    s = Bytes.new(file.size)
    file.read s
    data2 = String.new(s)

    data.should eq data2

    # links

    
end
end

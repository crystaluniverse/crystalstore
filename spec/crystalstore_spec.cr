require "./spec_helper"

require "uuid"

describe CrystalStore::Store do
  # TODO: Write tests

  it "works" do
    store = CrystalStore::Store.new
    # fm  = CrystalStore::FileMeta.new id: 1_u64, block_size:(1024*1024).to_u64, size: 100_u64, is_file: false
    # u = CrystalStore::User.new name: "a"
    # g = CrystalStore::Group.new name: "a"
    # f = CrystalStore::File.new name: "a"
    # d = CrystalStore::Dir.new name: "a"
    # puts d.dumps    
    
    # puts "creating /a"
    # store.dir_create path: "/a"
    
    # puts "try creating /a again"
    # begin
    #   store.dir_create path: "/a"
    #   raise Efileception.new "should have failed"
    # rescue efileception
    # end

    
    # puts "create /a/b"
    # store.dir_create path: "/a/b"

    # puts "create /a/b/c"
    # store.dir_create path: "/a/b/c"

    # puts "deleted /a/b"
    # store.dir_delete("/a/b")

    # store.dir_create path: "/a/b"
    # puts "created /a/b"


    #  # puts "create /m"
    #  store.dir_create path: "/m"

    #  # puts "create /n"
    #  store.dir_create path: "/n"
 
    #  puts "copying"
    #  store.dir_copy("/m", "/n")

    #  store.dir_delete("/m/n")
    
    #  store.dir_copy("/m", "/n")

    #  store.dir_copy("/m", "/n", overwrite=true)

     # puts "create /aa"
    #  store.dir_create path: "/aa"

    #  # puts "create /bb"
    #  store.dir_create path: "/bb"
 
    #  puts "moving"
    #  store.dir_move("/aa", "/bb")

    #  # puts "create /aa"
    #  store.dir_create path: "/aa"

    #  puts "moving"
    #  store.dir_move("/aa", "/bb", ovewrite=true)

    #  store.dir_create path: "/aa"

    #  puts "moving"
    #  store.dir_move("/aa", "/bb", ovewrite=false)

    # puts "creating"
    # # store.dir_create path: "/aa"
    # puts store.dir_stats path: "/s"
    # puts store.dir_list path: "/s"

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
end
end

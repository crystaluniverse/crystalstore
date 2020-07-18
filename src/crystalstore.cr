require "./models"

require "bcdb"

class CrystalStore::Store

  STORE_META_DATA_ID = 0
  ROOT_DIR_ID = 1

  property db : Bcdb::Client
  property block_size : UInt64
  property max_no_blocks : UInt64
  property max_no_files : UInt64
  property max_filename : UInt64
  property mount_flags : UInt64

  def initialize(
        @block_size=(1024 * 1024).to_u64,
        @max_no_blocks=(2^64).to_u64,
        @max_no_files=(2^64).to_u64,
        @max_filename=512_u64,
        @mount_flags=0_u64,
        @db_name = "db",
        @db_namespace="crystalstore",
        @db_unixsocket="/tmp/bcdb.sock")

    @db =  Bcdb::Client.new unixsocket: @db_unixsocket, db: @db_name, namespace: @db_namespace

    # Make sure Store meta is available, or create it
    begin
      store_meta = @db.get(0)
    rescue Bcdb::NotFoundError
      store = CrystalStore::StoreMeta.new id: 0_u64, block_size: @block_size, no_blocks: @max_no_blocks-1, no_files: @max_no_files-1, mount_flags: @mount_flags, max_filename: @max_filename
      @db.put(store.dumps)
    end

    # Make sure root dir exists, or create it
    begin
      store_meta = @db.get(1)
    rescue Bcdb::NotFoundError
      root_meta = CrystalStore::FileMeta.new id: 1.to_u64, block_size: @block_size, size: 4096_u64, is_file: false, mode: 0_i16
      root = CrystalStore::Dir.new name: "", meta: root_meta
      @db.put(root.dumps)
    end
  end

  def dir_create(path : String, mode : Int16=0o644)
    CrystalStore::Dir.mkdir(@db, path, mode)
  end

  def dir_delete(path : String)
    CrystalStore::Dir.rm(@db, path)
  end
  
  def dir_list(path : String)
    CrystalStore::Dir.ls(@db, path)
  end

  def dir_stats(path : String)
    CrystalStore::Dir.stats(@db, path)
  end

  def dir_copy(src : String, dest : String, overwrite : Bool = false)
    CrystalStore::Dir.cp(@db, src, dest, overwrite)
  end

  def dir_move(src : String, dest : String, overwrite : Bool = false)
    CrystalStore::Dir.mv(@db, src, dest, overwrite)
  end

  def file_create(path : String, mode : Int16, flags : Int32, content_type : String)
    CrystalStore::File.touch(@db, path, mode, flags, content_type)
  end

  def file_open(path : String, mode : Int16, flags : Int32)
    CrystalStore::File.open(@db, path, mode, flags)
  end

  def file_delete(path : String)
    CrystalStore::File.rm(@db, path)
  end

  def file_exists?(path : String)
    CrystalStore::File.exists?(@db, path)
  end

  def file_stats(path : String)
    CrystalStore::File.stats(@db, path)
  end

  def file_copy(src : String, dest : String)
    CrystalStore::File.cp(@db, src, dest)
  end

  def file_move(src : String, dest : String)
    CrystalStore::File.mv(@db, src, dest)
  end

  def file_truncate(path : String)
    CrystalStore::File.mv(@db, path)
  end

  def link(src : String, dest : String)
    CrystalStore::Link.link(@db, src, dest)
  end

  def symlink(src : String, dest : String)
    CrystalStore::Link.symlink(@db, src, dest)
  end

  def unlink(src : String)
    CrystalStore::Link.unlink(@db, path)
  end

  def readlink(path : String)
    CrystalStore::Link.readlink(@db, path)
  end

  def access()
    CrystalStore::Dir.access(@db, path)
  end

  def chmod()
    CrystalStore::Dir.chmod(@db, path)
  end

  def chown()
    CrystalStore::Dir.chown(@db, path)
  end
end

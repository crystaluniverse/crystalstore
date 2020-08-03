require "./models"

require "bcdb"

class CrystalStore::Store

  property db : Bcdb::Client
  property db_name : String
  property db_namespace : String
  property block_size : UInt64
  property max_no_blocks : UInt64
  property max_no_files : UInt64
  property max_filename : UInt64
  property mount_flags : UInt64

  def self.stats(db_name = "db", db_namespace="crystalstore", db_unixsocket="/tmp/bcdb.sock")
    db =  Bcdb::Client.new unixsocket: @db_unixsocket, db: @db_name, namespace: @db_namespace
    stats = nil
    begin
      store_meta = @db.get(0)
      CrystalStore::StoreMeta.loads(CrystalStore::StoreMeta.fetch(0))
    rescue Bcdb::NotFoundError
    end
    stats
  end

  def initialize(
        @block_size=(1024 * 1024).to_u64,
        @max_no_blocks=(2_u64**60_u64).to_u64,
        @max_no_files=(2_u64**60_u64).to_u64,
        @max_filename=512_u64,
        @mount_flags=0_u64,
        @db_name = "db",
        @db_namespace="crystalstore",
        @db_unixsocket="/tmp/bcdb.sock")
    
    @db =  Bcdb::Client.new unixsocket: @db_unixsocket, db: @db_name, namespace: @db_namespace

    # Make sure Store meta is available, or create it
    ids = @db.find({"store_namespace" => @db_namespace})
    
    if ids.size > 0
      store_meta = CrystalStore::StoreMeta.loads(CrystalStore::StoreMeta.fetch(@db, ids[0]))
      if store_meta.block_size != @block_size
        raise CrystalStore::InitializationError.new "Can not reinitialize with new block size current=#{store_meta.block_size} & provided=#{@block_size}"
      end

      if store_meta.max_filename > @max_filename
        raise CrystalStore::InitializationError.new "Can not reinitialize with shorter max_filename current=#{store_meta.max_filename} & provided=#{@max_filename}"
      end

      used_blocks = store_meta.no_blocks - store_meta.no_free_blocks

      if @max_no_blocks < used_blocks
        raise CrystalStore::InitializationError.new "Can not reinitialize with new max block number #{@max_no_blocks} that is < current number of used blocks #{used_blocks}"
      else
        store_meta.no_blocks = @max_no_blocks
        if @max_no_blocks > store_meta.no_blocks
          store_meta.no_free_blocks += (@max_no_blocks - store_meta.no_blocks)
          store_meta.no_available_blocks += (@max_no_blocks - store_meta.no_blocks)
        elsif  @max_no_blocks < store_meta.no_blocks
          store_meta.no_free_blocks -= (store_meta.no_blocks - @max_no_blocks)
          store_meta.no_available_blocks -= (store_meta.no_blocks - @max_no_blocks)
        end
      end

      used_files = store_meta.no_files - store_meta.no_free_files

      if @max_no_files < used_files
        raise CrystalStore::InitializationError.new "Can not reinitialize with new max files number #{@max_no_files} that is < current number of used files #{used_files}"
      else
        store_meta.no_files = @max_no_files
        if @max_no_files > store_meta.no_files
          store_meta.no_free_files += (@max_no_files - store_meta.no_files)
          store_meta.no_available_files += (@max_no_files - store_meta.no_files)
        elsif  @max_no_files < store_meta.no_files
          store_meta.no_free_files -= (store_meta.no_files - @max_no_files)
          store_meta.no_available_files -= (store_meta.no_files - @max_no_files)
        end
      end

    else
      store = CrystalStore::StoreMeta.new block_size: @block_size, no_blocks: @max_no_blocks, no_files: @max_no_files, mount_flags: @mount_flags, max_filename: @max_filename
      @db.put(store.dumps, {"store_namespace" => @db_namespace})
    end

    # Make sure root dir exists, or create it
    ids = @db.find({"root_namespace" => @db_namespace})
    if ids.size == 0
      root_meta = CrystalStore::FileMeta.new block_size: @block_size, size: 4096_u64, is_file: false, mode: 0_i16
      root = CrystalStore::Dir.new name: "", meta: root_meta
      db.put(root.dumps, {"root_namespace" => @db_namespace})
    end
  end

  def dir_create(path : String, mode : Int16=0o644, create_parents : Bool = false)
    CrystalStore::Dir.mkdir(@db, path, mode, create_parents)
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

  def dir_exists?(path : String)
    CrystalStore::Dir.exists?(@db, path)
  end

  def file_create(path : String, mode : Int16, flags : Int32, content_type : String, create_parents : Bool = false)
    CrystalStore::File.touch(@db, path, mode, flags, content_type, create_parents)
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

  def unlink(path : String)
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

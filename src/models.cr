require "msgpack"
require "time"
require "bcdb"
require "./errors"
require "./file"
require "json"

class CrystalStore::List
    include MessagePack::Serializable

    property size : UInt64
    property mode : Int16
    property last_modified : Int64
    property files : Array(CrystalStore::File)
    property dirs : Array(CrystalStore::DirPointer)
    
    def initialize(
        @size,
        @mode,
        @last_modified,
        @files=Array(CrystalStore::File).new,
        @dirs=Array(CrystalStore::DirPointer).new
    )

    end
end

# Model is the **Parent** of all models
class CrystalStore::Model
    include MessagePack::Serializable

    def dumps
        io = IO::Memory.new
        self.to_msgpack(io)
        io.to_s
    end

    def self.loads(msgpack : String?)
        self.from_msgpack(msgpack.not_nil!.to_slice)
    end

    def self.fetch(db : Bcdb::Client, id : UInt64)
        begin
            db.get(id.to_i32)["data"].as(String)
        rescue Bcdb::NotFoundError
            raise CrystalStore::FileNotFoundError.new "#{id.to_s} does not exist"
        end
    end

    def self.delete(db : Bcdb::Client, id : UInt64)
        item = self.loads(self.fetch(db, id))
        if item.meta.no_references == 0
            db.delete(id)
        end
    end
end

# CrystalStore::StoreMeta holds File system metadata
# it contains all resulting attributes from syscall `statvfs`
class CrystalStore::StoreMeta < CrystalStore::Model
    
    @[MessagePack::Field(key: "f_fsid")]
    property id : UInt64?

    @[MessagePack::Field(key: "f_bsize")]
    property block_size : UInt64

    @[MessagePack::Field(key: "f_frsize")]
    property fragment_size : UInt64

    @[MessagePack::Field(key: "f_blocks")]
    property no_blocks : UInt64

    @[MessagePack::Field(key: "f_bfree")]
    property no_free_blocks : UInt64

    @[MessagePack::Field(key: "f_bavail")]
    property no_available_blocks : UInt64

    @[MessagePack::Field(key: "f_files")]
    property no_files : UInt64

    @[MessagePack::Field(key: "f_ffree")]
    property no_free_files : UInt64

    @[MessagePack::Field(key: "f_favail")]
    property no_available_files : UInt64
    
    @[MessagePack::Field(key: "f_flag")]
    property mount_flags : UInt64

    @[MessagePack::Field(key: "f_namemax")]
    property max_filename : UInt64

    def initialize(@block_size, @no_blocks, @no_files,@mount_flags, @max_filename)
        @fragment_size = @block_size
        @no_free_blocks = @no_blocks
        @no_available_blocks = @no_blocks
        @no_free_files = @no_files
        @no_available_files = @no_files
    end

    def update_no_free_blocks(size : Int64)
        no_new_blocks = (size/@block_size).ceil.to_i64
        self.no_free_blocks += no_new_blocks
        self.no_available_blocks += no_new_blocks
    end

    def update_no_free_files(no_new_files : Int64)
        self.no_free_files += no_new_files
        self.no_available_files += no_new_files
    end

    def self.get (db : Bcdb::Client)
        ids = db.find({"store_namespace" => db.namespace})
        store = self.loads(db.get(ids[0])["data"].as(String))
        store.id = ids[0]
        store
    end
end

class CrystalStore::FileMeta < CrystalStore::Model

    @[MessagePack::Field(key: "st_ino")]
    property id : UInt64?

    property name : String?

    @[MessagePack::Field(key: "st_mode")]
    property mode : Int16

    @[MessagePack::Field(key: "st_rdev")]
    property device_id : UInt64 = 66309_u64

    @[MessagePack::Field(key: "st_nlink")]
    property no_hard_links : UInt64

    @[MessagePack::Field(key: "st_uid")]
    property uid : UInt64

    @[MessagePack::Field(key: "st_gid")]
    property gid : UInt64

    @[MessagePack::Field(key: "st_size")]
    property size : UInt64

    @[MessagePack::Field(key: "st_blksize")]
    property block_size : UInt64

    # No. of 512B blocks allocated
    # For historical reasons linux has 512B block size
    @[MessagePack::Field(key: "st_blocks")]
    property blocks_allocated : UInt64?

    @[MessagePack::Field(key: "st_atim")]
    property last_access : Int64

    @[MessagePack::Field(key: "st_mtim")]
    property last_modified : Int64

    @[MessagePack::Field(key: "st_ctim")]
    property last_status_chang : Int64

    property is_file : Bool
    property no_files : UInt64 = 0
    property no_references : UInt64 = 0
    property content_type : String = ""

    def initialize(@block_size, @size, @is_file, @id=nil, @name=nil, @mode=0_i16, @uid=0_u64, @gid=0_u64)
        now = Time.utc.to_unix

        @last_access = now
        @last_modified = now
        @last_status_chang = now

        @no_hard_links = 0_u64
        
        if @is_file
            @blocks_allocated = (@size/512).ceil.to_u64
        else
            @blocks_allocated = 0_u64
        end
    end
end

class CrystalStore::User < CrystalStore::Model
    @[MessagePack::Field(nilable: true)]
    property id : UInt64?

    property name : String
    property groups : Array(UInt64)

    def initialize(@name, @id=nil, @groups=Array(UInt64).new); end
end

class CrystalStore::Group < CrystalStore::Model
    @[MessagePack::Field(nilable: true)]
    property id : UInt64?

    property name : String
    property users : Array(UInt64)

    def initialize(@name, @id=nil, @users=Array(UInt64).new); end
end

class CrystalStore::DirPointer < CrystalStore::Model
    property id : UInt64

    property name : String
    property meta : CrystalStore::FileMeta
    
    def initialize (@id, @name, @meta); end
end

class CrystalStore::Link < CrystalStore::Model
    property id : UInt64
    property name : String
    property is_dir : Bool
    property is_symlink : Bool
    property meta : CrystalStore::FileMeta

    def initialize (@id, @name, @is_dir, @is_symlink, @meta); end

    def link(db : Bcdb::Client, src : String, dest : String)
    
    end
    
    def symlink(db : Bcdb::Client, src : String, dest : String)

    end

    def unlink(db : Bcdb::Client, src : String)

    end

    def readlink(db : Bcdb::Client, path : String)

    end
end

class CrystalStore::BlockMeta < CrystalStore::Model
    property id : UInt64
    property size : UInt64
    property md5 : String

    def initialize(@id, @size, @md5);end
end

class CrystalStore::Block < CrystalStore::Model
    property data : String

    def initialize(@data); end
end


class CrystalStore::File < CrystalStore::Model
    @[MessagePack::Field(nilable: true)]
    property id : UInt64?

    property name : String

    @[MessagePack::Field(nilable: true)]
    property meta : CrystalStore::FileMeta?

    property blocks : Array(CrystalStore::BlockMeta) =  Array(CrystalStore::BlockMeta).new
    
    def initialize (@name, @id=nil, @meta=nil); end

    def self.exists?(db : Bcdb::Client, path : String)
        basename = Path.new("/", path).basename
        begin
            parent, parent_parent = Dir.get_parents db: db, path: path
            if parent.file_exists?(basename)
                return true
            end
        rescue exception; 
        end
        return false
    end

    def self.touch(db : Bcdb::Client, path : String, mode : Int16, flags : Int32, content_type : String, create_parents : Bool = false)
        basename = Path.new("/", path).basename

        parent, parent_parent = Dir.get_parents db: db, path: path, create_parents: create_parents
        
        if parent.dir_exists?(basename) || parent.file_exists?(basename)
            raise CrystalStore::FileExistsError.new path
        end

        store = CrystalStore::StoreMeta.get db
        file_meta = CrystalStore::FileMeta.new block_size: store.block_size, size: 0_u64, is_file: true, mode: mode
        file_meta.content_type = content_type
        file = CrystalStore::File.new basename, meta: file_meta

        # create dir    
        id = db.put(file.dumps).to_u64

        # add meta to parent
        
        if parent.files.nil?
            parent.files = Hash(String, CrystalStore::File).new
        end

        parent.files.not_nil![basename] = file
          
        parent.meta.last_access = file_meta.last_access
        parent.meta.last_modified = file_meta.last_modified
        
        db.update(parent.id.not_nil!, parent.dumps)

        # update store meta
        store.update_no_free_files -1
        db.update(store.id.not_nil!, store.dumps)
    end

    def self.open(db : Bcdb::Client, path : String, mode : Int16, flags : Int32)
        CrystalStore::FileDescriptor.new db: db, path: path, mode: mode, flags: flags
    end

    def self.stats(db : Bcdb::Client, path : String)
        basename = Path.new("/", path).basename
        parent, parent_parent = CrystalStore::Dir.get_parents db: db, path: path
        
        if !parent.file_exists?(basename)
            raise CrystalStore::FileNotFoundError.new path
        end
        parent.files.not_nil![basename].meta.not_nil!.name = basename
        parent.files.not_nil![basename].meta
    end

    def self.cp(db : Bcdb::Client, src : String, dest : String, overwrite : Bool = false)
        # srcparent, srcparent_parent
        srcbasename = Path.new("/", src).basename
        destbasename = Path.new("/", dest).basename

        src_parent, src_parent_parent = CrystalStore::Dir.get_parents db: db, path: src
        dest_parent, dest_parent_parent = CrystalStore::Dir.get_parents db: db, path: dest

        if !src_parent.file_exists? srcbasename
            raise CrystalStore::FileNotFoundError.new src
        end

        
        # Get destination
        dest_dir = dest_parent

        # Get src  & meta data
        src_meta = src_parent.files.not_nil![srcbasename].meta

        store = CrystalStore::StoreMeta.get db

        if dest_dir.dir_exists? srcbasename
            if overwrite
                self.rm(db, "#{dest}/#{srcbasename}")
            else
                raise CrystalStore::FileExistsError.new srcbasename
            end
        end

        if dest_dir.file_exists? srcbasename
            if overwrite
                old_file = dest_dir.files.not_nil!.delete(srcbasename)
                store.update_no_free_files 1
                store.update_no_free_blocks old_file.not_nil!.meta.not_nil!.size.to_i64
            else
                raise CrystalStore::FileExistsError.new srcbasename
            end
        end

        if dest_dir.files.nil?
            dest_dir.files = Hash(String, CrystalStore::File).new
        end

        dest_dir.files.not_nil![destbasename] = src_parent.files.not_nil![srcbasename]

        # update store
        store.update_no_free_files(-1_i64 * src_meta.not_nil!.no_files.to_i64)
        store.update_no_free_blocks (-1_i64 * src_meta.not_nil!.size.to_i64)

        # update dest_dir
        now = Time.utc.to_unix
        dest_dir.meta.last_access = now
        dest_dir.meta.last_modified = now
        db.update(dest_dir.id.not_nil!, dest_dir.dumps)

        db.update(store.id.not_nil!, store.dumps)
    end

    def self.mv(db : Bcdb::Client, src : String, dest : String, overwrite : Bool = false)
        # srcparent, srcparent_parent
        src_path = Path.new("/", src)
        dest_path = Path.new("/", dest)

        srcbasename = src_path.basename
        destbasename = dest_path.basename
        
        rename = false
        if src_path.parent == dest_path.parent
            rename = true
        end
        src_parent, src_parent_parent = CrystalStore::Dir.get_parents db: db, path: src
        if rename
            dest_parent = src_parent
        else
            dest_parent, dest_parent_parent =  CrystalStore::Dir.get_parents db: db, path: dest
        end

        if !src_parent.file_exists? srcbasename
            raise CrystalStore::FileNotFoundError.new src
        end

        # Get destination
        dest_dir = dest_parent

        # Get src  & meta data
        src_meta = src_parent.files.not_nil![srcbasename].meta

        store = CrystalStore::StoreMeta.get db

        if !rename && dest_dir.dir_exists? destbasename
            if overwrite
                self.rm(db, dest)
            else
                raise CrystalStore::FileExistsError.new destbasename
            end
        end

        if ! rename && dest_dir.file_exists? destbasename
            if overwrite
                old_file = dest_dir.files.not_nil!.delete(destbasename)
                store.update_no_free_files 1
                store.update_no_free_blocks old_file.not_nil!.meta.not_nil!.size.to_i64
            else
                raise CrystalStore::FileExistsError.new destbasename
            end
        end

        if (rename && !overwrite) && (dest_dir.file_exists?(destbasename) || dest_dir.dir_exists?(destbasename))
            raise CrystalStore::FileExistsError.new destbasename
        end

        if dest_dir.files.nil?
            dest_dir.files = Hash(String, CrystalStore::File).new
        end

        dest_dir.files.not_nil![destbasename] = src_parent.files.not_nil![srcbasename]
        src_parent.files.not_nil!.delete(srcbasename)

        # update store
        store.update_no_free_files(-1_i64 * src_meta.not_nil!.no_files.to_i64)
        store.update_no_free_blocks (-1_i64 * src_meta.not_nil!.size.to_i64)
        db.update(store.id.not_nil!, store.dumps)

        # update dest_dir
        now = Time.utc.to_unix
        dest_dir.meta.last_access = now
        dest_dir.meta.last_modified = now
        db.update(dest_dir.id.not_nil!, dest_dir.dumps)

        # update src_parent
        src_parent.meta.last_access = now
        src_parent.meta.last_modified = now
        db.update(src_parent.id.not_nil!, src_parent.dumps)
    end

    def self.rm(db : Bcdb::Client, path : String)
        basename = Path.new("/", path).basename

        parent, parent_parent = CrystalStore::Dir.get_parents db: db, path: path
        
        if !parent.file_exists?(basename)
            raise CrystalStore::FileNotFoundError.new path
        end

        store = CrystalStore::StoreMeta.get db
        
        file = parent.files.not_nil![basename]

        parent.files.not_nil!.delete(basename)
        
        now = Time.utc.to_unix

        # update parent
        parent.meta.last_access = now
        parent.meta.last_modified = now
        db.update(parent.id.not_nil!, parent.dumps)

        # update parent meta in the dir pointer in parent parent
        if !parent_parent.nil?
            parent_parent = parent_parent.not_nil!
            parent_parent.dirs.not_nil![parent.name].meta.last_access = now
            parent_parent.dirs.not_nil![parent.name].meta.last_modified = now
            db.update(parent_parent.id.not_nil!, parent_parent.dumps)
        end

        # update store meta
        store.update_no_free_files file.meta.not_nil!.no_files.to_i64
        store.update_no_free_blocks file.meta.not_nil!.size.to_i64
        db.update(store.id.not_nil!, store.dumps)
    end

    def self.truncate(db : Bcdb::Client, path : String)
        basename = Path.new("/", path).basename

        parent, parent_parent = CrystalStore::Dir.get_parents db: db, path: path
        
        if !parent.file_exists?(basename)
            raise CrystalStore::FileNotFoundError.new path
        end

        store = CrystalStore::StoreMeta.get db
        

        file = parent.files.not_nil![basename]
        file.blocks = Array(CrystalStore::BlockMeta).new
        file.meta.size = 0
        
        now = Time.utc.to_unix

        # update parent
        parent.meta.last_access = now
        parent.meta.last_modified = now
        db.update(parent.id.not_nil!, parent.dumps)

        # update parent meta in the dir pointer in parent parent
        if !parent_parent.nil?
            parent_parent = parent_parent.not_nil!
            parent_parent.dirs.not_nil![parent.name].meta.last_access = now
            parent_parent.dirs.not_nil![parent.name].meta.last_modified = now
            db.update(parent_parent.id.not_nil!, parent_parent.dumps)
        end

        # update store meta
        store.update_no_free_files file.meta.no_files.to_i64
        store.update_no_free_blocks file.meta.size.to_i64
        db.update(store.id.not_nil!, store.dumps)
    end
end

class CrystalStore::Dir < CrystalStore::Model
    @[MessagePack::Field(nilable: true)]
    property id : UInt64?
    
    property name : String

    property meta : CrystalStore::FileMeta
    
    @[MessagePack::Field(nilable: true)]
    property dirs : Hash(String, CrystalStore::DirPointer)?

    @[MessagePack::Field(nilable: true)]
    property files : Hash(String, CrystalStore::File)?
    
    @[MessagePack::Field(nilable: true)]
    property links : Hash(String, CrystalStore::Link)?

    def initialize(@name, @meta, @id=nil, @dirs=nil, @files=nil, @links=nil); end
    
    def self.get_root(db : Bcdb::Client)
        ids = db.find({"root_namespace" => db.namespace})
        root = self.loads(self.fetch(db, ids[0]))
        root.id = ids[0]
        root
    end

    def self.get_parents(db : Bcdb::Client, path : String, create_parents : Bool = false)
        path_obj = Path.new("/", path)
        basename, path = path_obj.basename, path_obj.to_s
        dirnames = path_obj.parts

        if path == "/"
            raise CrystalStore::UnsupportedOperation.new "/ has no parent"
        end

        parent = self.get_root db
        
        parent_parent = nil

        dirnames.shift   # remove root
        dirnames.pop(1)  # remove last item
        current_path = "/"
        dirnames.each do |dirname|
            current_path += "#{dirname}/"
            parent_parent = parent
            parent_parent.id = parent.id.not_nil!
            if parent.dirs.nil? || !parent.dirs.not_nil!.has_key?(dirname)
                if create_parents
                    CrystalStore::Dir.mkdir(db, current_path, 644)
                    parent = CrystalStore::Dir.loads(self.fetch(db, parent.id.not_nil!))
                else
                    raise CrystalStore::FileNotFoundError.new path
                end
            end
            parent_pointer = parent.dirs.not_nil![dirname]
            parent_id = parent_pointer.id
           
            parent = CrystalStore::Dir.loads(self.fetch(db, parent_id))
            parent.id = parent_id
        end
        return parent, parent_parent
    end

    def file_exists?(name : String)
        if !self.files.nil? && self.files.not_nil!.has_key? name
            return true
        end
        return false
    end

    def dir_exists?(name : String)
        if !self.dirs.nil? && self.dirs.not_nil!.has_key? name
            return true
        end
        return false
    end

    def self.mkdir(db : Bcdb::Client, path : String, mode : Int16, create_parents : Bool = false)
        
        basename = Path.new("/", path).basename

        parent, parent_parent = self.get_parents db: db, path: path, create_parents: create_parents
        
        if parent.dir_exists?(basename) || parent.file_exists?(basename)
            raise CrystalStore::FileExistsError.new path
        end

        store = CrystalStore::StoreMeta.get db
        dir_meta = CrystalStore::FileMeta.new block_size: store.block_size, size: 0_u64, is_file: false, mode: mode
        dir = CrystalStore::Dir.new basename, meta: dir_meta

        # create dir    
        id = db.put(dir.dumps).to_u64

        # add meta to parent
        dir_pointer = CrystalStore::DirPointer.new id: id, name: basename, meta: dir_meta
        
        if parent.dirs.nil?
            parent.dirs = Hash(String, CrystalStore::DirPointer).new
        end

        parent.dirs.not_nil![basename] = dir_pointer
          
        parent.meta.last_access = dir_meta.last_access
        parent.meta.last_modified = dir_meta.last_modified
        
        db.update(parent.id.not_nil!, parent.dumps)

        # update parent meta in the dir pointer in parent parent
        if !parent_parent.nil?
            parent_parent = parent_parent.not_nil!
            parent_parent.dirs.not_nil![parent.name].meta.last_access = dir_meta.last_access
            parent_parent.dirs.not_nil![parent.name].meta.last_modified = dir_meta.last_modified
            db.update(parent_parent.id.not_nil!, parent_parent.dumps)
        end

        # update store meta
        store.update_no_free_files -1
        db.update(store.id.not_nil!, store.dumps)
    end

    def self.rm(db : Bcdb::Client, path : String)
        basename = Path.new("/", path).basename

        parent, parent_parent = self.get_parents db: db, path: path
        
        if !parent.dir_exists?(basename)
            raise CrystalStore::FileNotFoundError.new path
        end

        store = CrystalStore::StoreMeta.get db
        

        dir_pointer = parent.dirs.not_nil!.delete(basename)
        dir_pointer = dir_pointer.not_nil!
        dir_id = dir_pointer.id
        self.delete(db, dir_id)

        now = Time.utc.to_unix

        # update parent
        parent.meta.last_access = now
        parent.meta.last_modified = now
        db.update(parent.id.not_nil!, parent.dumps)

        # update parent meta in the dir pointer in parent parent
        if !parent_parent.nil?
            parent_parent = parent_parent.not_nil!
            parent_parent.dirs.not_nil![parent.name].meta.last_access = now
            parent_parent.dirs.not_nil![parent.name].meta.last_modified = now
            db.update(parent_parent.id.not_nil!, parent_parent.dumps)
        end

        # update store meta
        store.update_no_free_files dir_pointer.meta.no_files.to_i64
        store.update_no_free_blocks dir_pointer.meta.size.to_i64
        db.update(store.id.not_nil!, store.dumps)
    end

    def self.ls(db : Bcdb::Client, path : String)
        if path == "/"
            dest_dir = self.get_root db
        else
            basename = Path.new("/", path).basename

            parent, parent_parent = self.get_parents db: db, path: path
            
            if !parent.dir_exists?(basename)
                raise CrystalStore::FileNotFoundError.new path
            end
    
            # Get destination
            dest_pointer = parent.dirs.not_nil![basename]
            dest_dir = CrystalStore::Dir.loads(self.fetch(db, dest_pointer.id))
            dest_dir.id = dest_pointer.id
        end
        
        dest_dir = dest_dir.not_nil!
        files = Array(CrystalStore::File).new
        dirs = Array(CrystalStore::DirPointer).new

        if !dest_dir.dirs.nil? 
            dest_dir.dirs.not_nil!.each_key do |k|
                item = dest_dir.dirs.not_nil![k]
                item.meta.not_nil!.name = k
                dirs << item
            end
        end

        if !dest_dir.files.nil?
            dest_dir.files.not_nil!.each_key do |k|
                item = dest_dir.files.not_nil![k]
                item.meta.not_nil!.name = k
                files << item
            end
        end

        CrystalStore::List.new  size: dest_dir.meta.size,  mode: dest_dir.meta.mode, last_modified: dest_dir.meta.last_modified, files: files, dirs: dirs

    end

    
    # recursively get all files in a path
    private def self.ls_recursive(db : Bcdb::Client, path : String, all_files : Array(String) = Array(String).new, all_dirs : Array(String) = Array(String).new)
        list = self.ls(db, path)
        
        list.files.each do |f|
            all_files << Path.new(path, URI.decode(f.name)).to_s 
        end
        

        list.dirs.each do |d|
            p = Path.new(path, URI.decode(d.name)).to_s
            all_dirs << p
            self.ls_recursive(db, p, all_files, all_dirs)
        end

        {"files" =>  all_files, "dirs" => all_dirs}
    end

    def self.cp(db : Bcdb::Client, src : String, dest : String, overwrite : Bool = false)
        
        # srcparent, srcparent_parent
        srcbasename = Path.new("/", src).basename
        destbasename = Path.new("/", dest).basename

        src_parent, src_parent_parent = self.get_parents db: db, path: src
        dest_parent, dest_parent_parent = self.get_parents db: db, path: dest

        if !src_parent.dir_exists? srcbasename
            raise CrystalStore::FileNotFoundError.new src
        end

    
        # Get destination
        dest_dir = dest_parent

        store = CrystalStore::StoreMeta.get db

        if dest_dir.dir_exists? srcbasename
            if overwrite
                self.rm(db, dest)
            else
                raise CrystalStore::FileExistsError.new srcbasename
            end
        end

        if dest_dir.file_exists? srcbasename
            if overwrite
                old_file = dest_dir.files.not_nil!.delete(srcbasename)
                store.update_no_free_files 1
                store.update_no_free_blocks old_file.not_nil!.meta.not_nil!.size.to_i64
            else
                raise CrystalStore::FileExistsError.new srcbasename
            end
        end

        if dest_dir.dirs.nil?
            dest_dir.dirs = Hash(String, CrystalStore::DirPointer).new
        end

        list = self.ls_recursive db: db, path: src
        
        # create dest dir
        self.mkdir(db, dest, 777)
        
        list["dirs"].each do |d|
            path = d.sub(src, dest)
            self.mkdir(db, dest, 777)
        end

        list["files"].each do |f|
            src = f
            dest = f.sub(src, dest)
            CrystalStore::File.cp(db, src, dest)
        end
    end

    def self.mv(db : Bcdb::Client, src : String, dest : String, overwrite : Bool = false)
        
        # srcparent, srcparent_parent
        src_path = Path.new("/", src)
        dest_path = Path.new("/", dest)

        srcbasename = src_path.basename
        destbasename = dest_path.basename

        rename = false
        if src_path.parent == dest_path.parent
            rename = true
        end

        src_parent, src_parent_parent = self.get_parents db: db, path: src
        if rename
            dest_parent = src_parent
        else
            dest_parent, dest_parent_parent = self.get_parents db: db, path: dest
        end

        if !src_parent.dir_exists? srcbasename
            raise CrystalStore::FileNotFoundError.new src
        end

        # Get destination
        dest_dir = dest_parent

        store = CrystalStore::StoreMeta.get db

        if !rename && dest_dir.dir_exists? destbasename
            if overwrite
                self.rm(db, dest)
            else
                raise CrystalStore::FileExistsError.new destbasename
            end
        end
        
        if !rename && dest_dir.file_exists? destbasename
            if overwrite
                old_file = dest_dir.files.not_nil!.delete(destbasename)
                store.update_no_free_files 1
                store.update_no_free_blocks old_file.not_nil!.meta.not_nil!.size.to_i64
            else
                raise CrystalStore::FileExistsError.new destbasename
            end
        end

        if (rename && !overwrite) && (dest_dir.file_exists?(destbasename) || dest_dir.dir_exists?(destbasename))
            raise CrystalStore::FileExistsError.new destbasename
        end

        if dest_dir.dirs.nil?
            dest_dir.dirs = Hash(String, CrystalStore::DirPointer).new
        end

        dest_dir.dirs.not_nil![destbasename] = src_parent.dirs.not_nil![srcbasename]
        
        src_parent.dirs.not_nil!.delete(srcbasename)        
        now = Time.utc.to_unix

        # update src_dir
        src_parent.meta.last_access = now
        src_parent.meta.last_modified = now
        db.update(src_parent.id.not_nil!, src_parent.dumps)

        # update dest_dir
        dest_dir.meta.last_access = now
        dest_dir.meta.last_modified = now
        db.update(dest_dir.id.not_nil!, dest_dir.dumps)
    end

    def self.stats(db : Bcdb::Client, path : String)
        basename = Path.new("/", path).basename
        parent, parent_parent = self.get_parents db: db, path: path
        
        if !parent.dir_exists?(basename)
            raise CrystalStore::FileNotFoundError.new path
        end
        parent.dirs.not_nil![basename].meta.not_nil!.name = basename
        parent.dirs.not_nil![basename].meta

    end

    def self.access(db : Bcdb::Client, path : String)

    end

    def self.chmod(db : Bcdb::Client, path : String)

    end

    def self.chown(db : Bcdb::Client, path : String)

    end
end


require "./models"
require "digest"

class CrystalStore::FileDescriptor < IO::Memory
    property block_size : UInt64
    property path : String
    property filename : String
    property parent : CrystalStore::Dir
    property store : CrystalStore::StoreMeta
    property file : CrystalStore::File
    property mode : Int16
    property flags : Int32
    property db :  Bcdb::Client

    def initialize(@db, @path, @mode, @flags)
        basename = Path.new("/", path).basename
        parent, _ = CrystalStore::Dir.get_parents db: db, path: path
        
        if !parent.file_exists?(basename)
            raise CrystalStore::FileNotFoundError.new path
        end
        
        @parent = parent
        @store = CrystalStore::StoreMeta.get db
        @block_size = @store.block_size

        super @block_size

        @filename = Path.new("/", path).basename
        @file = @parent.files.not_nil![@filename]
        @bytesize = @file.meta.not_nil!.size.to_i32
    end

    private def update?
        return @pos > 0 && @pos < @bytesize
    end

    private def get_block_sizes(size)
        if size == 0
            return Array(Int32).new
        end
        no_blocks = (size/@block_size).ceil.to_i32
        sizes = Array(Int32).new(no_blocks) { @block_size.to_i32 }
        rem = size.remainder(@block_size)
        sizes[sizes.size-1] = rem
        sizes
    end

    private def read_block(idx)
        CrystalStore::Block.loads(CrystalStore::Block.fetch(@db, @file.blocks[idx].id)).data.to_slice
    end

    private def write_block(buffer, size, index=-1)
        data = Slice.new(buffer, size)
        data = String.new data
        md5 = Digest::MD5.hexdigest(data)
        # search for md5, if not there add block, if there use the id
        tags={"md5" =>  md5}
        res = @db.find(tags)
        if res.size == 0
            block_id = @db.put(CrystalStore::Block.new(data).dumps, tags)
        else
            block_id = res[0]
        end

        block_meta = CrystalStore::BlockMeta.new id: block_id, size: size.to_u64, md5: md5

        if index == -1
            @file.blocks << block_meta
        end
        @file.meta.not_nil!.size += size.to_u64

        now = Time.utc.to_unix
        @parent.meta.last_access = now
        @parent.meta.last_modified = now
        @db.update(@parent.id.not_nil!, @parent.dumps)

        @store.update_no_free_blocks (-1_i64 * size.to_i64)
        @db.update(0, @store.dumps)
    end

    def write(slice : Bytes) : Nil
        # if update, find targeted blocks, read. update with new data, create new blocks, save them
        check_writeable
        check_open

        count = slice.size

        return if count == 0

        if !update?
            sizes = self.get_block_sizes count
            sizes.each do |size|
                buffer = GC.malloc_atomic(size.to_u32).as(UInt8*)
                slice.copy_to(buffer, size)
                write_block buffer: buffer, size: size, index: -1
                @pos += size
                @bytesize += size
            end
        end
    end

    def write_byte(byte : UInt8)
        self.write(Slice.new(1) {byte})
    end

    def read(slice : Bytes)
        check_open
    
        count = slice.size

        count = Math.min(count, @bytesize - @pos)
        
        sizes = self.get_block_sizes count
        sizes.each_index do |idx|
            block = self.read_block idx
            slice.copy_from(block.to_unsafe, count=sizes[idx])
            @pos += size
        end
        count
    end

    def read_at(offset, bytesize)
        unless 0 <= offset <= @bytesize
          raise ArgumentError.new("Offset out of bounds")
        end
    
        if bytesize < 0
          raise ArgumentError.new("Negative bytesize")
        end
    
        unless 0 <= offset + bytesize <= @bytesize
          raise ArgumentError.new("Bytesize out of bounds")
        end
    
        old_writeable = @writeable
        old_resizeable = @resizeable
        io = IO::Memory.new(to_slice[offset, bytesize], writeable: false)
        begin
          @writeable = false
          @resizeable = false
          yield io
        ensure
          io.close
          @writeable = old_writeable
          @resizeable = old_resizeable
        end
    end
end
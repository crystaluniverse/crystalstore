
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
    getter buffer
    getter buffer_used_size

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
        @buffer =  GC.malloc_atomic(@block_size.to_u32).as(UInt8*)
        @buffer_used_size = 0
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
        if rem > 0
            sizes[sizes.size-1] = rem
        end
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
        @parent.files.not_nil![@filename] = @file
        now = Time.utc.to_unix
        @parent.meta.last_access = now
        @parent.meta.last_modified = now
        @db.update(@parent.id.not_nil!, @parent.dumps)

        #@store.update_no_free_blocks (-1_i64 * size.to_i64)
        @db.update(0, @store.dumps)
    end

    def flush
        if @buffer_used_size > 0
            write_block buffer: @buffer, size: @buffer_used_size, index: -1
            @pos += @buffer_used_size
            @bytesize += @buffer_used_size
            @buffer =  GC.malloc_atomic(@block_size.to_u32).as(UInt8*)
            @buffer_used_size = 0
        end
    end

    def close
        self.flush
        @closed = true
    end

    def write(slice : Bytes) : Nil
        # if update, find targeted blocks, read. update with new data, create new blocks, save them
        check_writeable
        check_open

        size = slice.size
        
        return if size == 0

        if size > @block_size
            raise CrystalStore::BlockSizeExceededErorr.new
        end

        slice = slice.to_unsafe

        if !update?
            if @buffer_used_size + size >= @block_size
                # fill buffer, write, update counters
                count = @block_size - @buffer_used_size
                slice.copy_to(@buffer + @buffer_used_size, count)
                write_block buffer: @buffer, size: @block_size, index: -1
                @pos += @block_size
                @bytesize += @block_size
                @buffer =  GC.malloc_atomic(@block_size.to_u32).as(UInt8*)
                (slice+count).copy_to(@buffer,  size-count)
                @pos += (size-count)
                @bytesize += (size-count)
                @buffer_used_size = (size-count)
            else
                slice.copy_to(@buffer + @buffer_used_size, size)
                @pos += (size)
                @bytesize += (size)
                @buffer_used_size += (size)
            end
        end
    end

    def write_byte(byte : UInt8)
        self.write(Slice.new(1) {byte})
    end

    def read(slice : Bytes)
        check_open
    
        size = slice.size
        slice = slice.to_unsafe
        
        count = Math.min(size, @bytesize - @pos)
        
        sizes = self.get_block_sizes count
        sizes.each_index do |idx|
            c = sizes[idx]
            block = self.read_block idx
            (slice+@pos).copy_from(block.to_unsafe, count=c)
            @pos += c
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

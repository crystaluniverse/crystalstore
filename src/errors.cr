class CrystalStore::FileNotFoundError < IO::Error 
    def initialize (@err : String); end
end

class CrystalStore::FileExistsError < IO::Error
    def initialize (@err : String); end
end

class CrystalStore::UnsupportedOperation < IO::Error
    def initialize(@err : String); end
end

class CrystalStore::PermissionDeniedErorr < IO::Error
    def initialize(@err : String = ""); end
end

class CrystalStore::BlockSizeExceededErorr < IO::Error
    def initialize(@err : String = ""); end
end

class CrystalStore::InitializationError < IO::Error
    def initialize(@err : String = ""); end
end

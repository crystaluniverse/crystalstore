require "./models"

class CrystalStore::Cache
    property cache : Hash(UInt64, CrystalStore::Dir) = Hash(UInt64, CrystalStore::Dir).new

    def set(key, value)
        @cache[key] = value
    end

    def get(key)
        @cache.[key]
    end

    def delete(key)
        @cache.delete(key)
    end

    def update(key, value)
        @cache[key] = value
    end

    def exists?(key)
        @cache.has_key?(key)
    end

end

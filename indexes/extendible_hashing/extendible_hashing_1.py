# Extendible Hashing Index
import struct
from ..core.record import Record
import os

BLOCK_FACTOR = 4
MAX_OVERFLOW = 1

# max overflow 1 bucket
class Bucket:
    HEADER_FORMAT = "iii"  # local_depth, size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, bucket_pos, local_depth=1, size=0, next_bucket=-1):
        self.local_depth = local_depth
        self.size = size
        self.next_bucket = next_bucket
        self.bucket_pos = bucket_pos

    def insert(self, offset, indexfile, global_depth, overflow_count=0):
        if self.size >= BLOCK_FACTOR:
            if self.local_depth < global_depth:
                # TODO: perform bucket split + redistribute
                return False
            else:
                if overflow_count < MAX_OVERFLOW:
                    return self.handle_overflow(offset, indexfile, global_depth, overflow_count)
                else:
                    return False  # insertion failed extend hashing glogal depth in other class
        # store offset in bucket
        indexfile.seek(self.bucket_pos + self.HEADER_SIZE + self.size * 4)
        indexfile.write(struct.pack("i", offset))
        # update header
        self.size += 1
        indexfile.seek(self.bucket_pos)
        indexfile.write(struct.pack(
            self.HEADER_FORMAT,
            self.local_depth,
            self.size,
            self.next_bucket
        ))
        return True

    def handle_overflow(self, offset, indexfile, global_depth, overflow_count):
        # traverse chain if it exists
        if self.next_bucket != -1:
            indexfile.seek(self.next_bucket)
            local_depth, size, next_bucket = struct.unpack(Bucket.HEADER_FORMAT, indexfile.read(Bucket.HEADER_SIZE))
            overflow_bucket = Bucket(self.next_bucket, local_depth, size, next_bucket)
            return overflow_bucket.insert(offset, indexfile, global_depth, overflow_count + 1)

        # allocate new overflow bucket
        indexfile.seek(0, os.SEEK_END)
        new_pos = indexfile.tell()
        new_bucket = Bucket(new_pos, local_depth=self.local_depth)
        indexfile.write(struct.pack(Bucket.HEADER_FORMAT, new_bucket.local_depth, 0, -1))

        # link to new overflow
        self.next_bucket = new_pos
        indexfile.seek(self.bucket_pos)
        indexfile.write(struct.pack(
            self.HEADER_FORMAT,
            self.local_depth,
            self.size,
            self.next_bucket
        ))

        # insert into new bucket
        return new_bucket.insert(offset, indexfile, global_depth, overflow_count + 1)

    def search(self, datafile, indexfile, key):
        indexfile.seek(self.bucket_pos)
        local_depth, size, next_bucket = struct.unpack(
            Bucket.HEADER_FORMAT, indexfile.read(Bucket.HEADER_SIZE)
        )
        for i in range(size):
            indexfile.seek(self.bucket_pos + Bucket.HEADER_SIZE + i * 4)
            offset = struct.unpack("i", indexfile.read(4))[0]
            datafile.seek(offset)
            record = Record.unpack(datafile.read(Record.RECORD_SIZE))
            if record.key == key:
                return record
        if next_bucket != -1:
            return Bucket(next_bucket).search(datafile, indexfile, key)
        return None


class ExtendableHashing:  # directory
    HEADER_FORMAT = "ii"  # globaldepth, num of elements
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"  # pointer to bucket
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, filename, indexname, global_depth=3, size=0):
        self.filename = filename
        self.indexname = indexname
        self.file = open(filename, 'r+b')   # data file
        create_new = not os.path.exists(indexname)
        self.indexfile = open(indexname, 'w+b' if create_new else 'r+b')  # index file
        self.size = size
        self.global_depth = global_depth

        if create_new:
            self.indexfile.seek(0)
            self.indexfile.write(struct.pack(
                self.HEADER_FORMAT,
                self.global_depth,
                self.size
            ))
            dir_entries = 2 ** self.global_depth
            dir_start = self.HEADER_SIZE
            bucket_space = Bucket.HEADER_SIZE + BLOCK_FACTOR * 4
            bucket0_pos = dir_start + dir_entries * self.DIR_SIZE
            bucket1_pos = bucket0_pos + bucket_space
            bucket0 = Bucket(bucket0_pos, local_depth=1)
            bucket1 = Bucket(bucket1_pos, local_depth=1)
            self.indexfile.seek(bucket0_pos)
            self.indexfile.write(struct.pack(
                Bucket.HEADER_FORMAT, bucket0.local_depth, 0, -1
            ))
            self.indexfile.seek(bucket1_pos)
            self.indexfile.write(struct.pack(
                Bucket.HEADER_FORMAT, bucket1.local_depth, 0, -1
            ))
            self.indexfile.seek(dir_start)
            for i in range(dir_entries):
                if i % 2 == 0:
                    self.indexfile.write(struct.pack(self.DIR_FORMAT, bucket0_pos))
                else:
                    self.indexfile.write(struct.pack(self.DIR_FORMAT, bucket1_pos))

    def get_dir_index(self, key):
        hash_val = hash(key)
        return hash_val % (2 ** self.global_depth)

    def get_bucket(self, key):
        dir_index = self.get_dir_index(key)
        self.indexfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, self.indexfile.read(self.DIR_SIZE))[0]
        self.indexfile.seek(bucket_pos)
        local_depth, size, next_bucket = struct.unpack(
            Bucket.HEADER_FORMAT, self.indexfile.read(Bucket.HEADER_SIZE)
        )
        return Bucket(bucket_pos, local_depth, size, next_bucket)

    def search(self, key):
        if self.size == 0:
            return None
        bucket = self.get_bucket(key)
        return bucket.search(self.file, self.indexfile, key)

    def insert(self, offset: int, key):
        if self.size == 0:
            self.size = 1
        bucket = self.get_bucket(key)
        success = bucket.insert(offset, self.indexfile, self.global_depth)
        if not success:
            # TODO: call split here when ready
            pass
        self.size += 1

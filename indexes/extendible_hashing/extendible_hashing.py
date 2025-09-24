# Extendible Hashing Index
import struct
from ..core.record import Record
import os
BLOCK_FACTOR = 4
RECORD_SIZE = 100


# max overflow 1 bucket
class Bucket:
    HEADER_FORMAT = "iii"  # local_depth, size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, local_depth=1, size=0, next_bucket=-1):
        self.local_depth = local_depth
        self.size = size
        self.next_bucket = next_bucket

    def insert(self, data: bytes, file, bucket_pos: int):
        # 1) check if size < block factor
        if self.size >= BLOCK_FACTOR:
            return False  # split or overflow
        # 2) write record
        file.seek(bucket_pos + self.HEADER_SIZE + self.size * RECORD_SIZE)
        file.write(data)

        self.size += 1

        file.seek(bucket_pos)

        file.write(struct.pack(
            self.HEADER_FORMAT,
            self.local_depth,
            self.size,
            self.next_bucket
        ))

        return True


    def search(self, file, bucket_pos: int, key: bytes):
        if self.size == 0:
            return None
        file.seek(bucket_pos)
        local_depth, size, next_bucket = struct.unpack(
            self.HEADER_FORMAT, file.read(self.HEADER_SIZE)
        )

        for i in range(size):
            file.seek(bucket_pos + self.HEADER_SIZE + i * RECORD_SIZE)
            record = file.read(RECORD_SIZE)
            record = Record.unpack(record)
            if record.key == key:
                return record

        #if not found
        # check overflow bucket if any
        if next_bucket != -1:
            return self.search(file, next_bucket, key)

        return None


class ExtendableHashing: #pointers
    HEADER_FORMAT = "ii"  # globaldepth, num of elements
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"  # pointer to bucket
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, filename, indexname, global_depth=3, size=0):
        self.filename = filename
        self.indexname = indexname
        self.file = open(filename, 'r+b')
        create_new = not os.path.exists(indexname)
        self.indexfile = open(indexname, 'w+b' if create_new else 'r+b')
        self.size = size
        self.global_depth = global_depth
        # create initial 2 buckets and have directories ending in 0 or 1 point to the respective 1,
        # write this in the indexfile
        if create_new:
            self.indexfile.seek(0)
            self.indexfile.write(struct.pack(
                self.HEADER_FORMAT,
                self.global_depth,
                self.size
            ))

            dir_entries = 2 ** self.global_depth
            dir_start = self.HEADER_SIZE

            # 2) create two initial buckets
            bucket0 = Bucket(local_depth=1, size=0, next_bucket=-1)
            bucket1 = Bucket(local_depth=1, size=0, next_bucket=-1)

            # space for each bucket (header + record slots)
            bucket_space = Bucket.HEADER_SIZE + BLOCK_FACTOR * RECORD_SIZE

            bucket0_pos = dir_start + dir_entries * self.DIR_SIZE
            bucket1_pos = bucket0_pos + bucket_space

            # write empty bucket0
            self.indexfile.seek(bucket0_pos)
            self.indexfile.write(struct.pack(
                Bucket.HEADER_FORMAT,
                bucket0.local_depth,
                bucket0.size,
                bucket0.next_bucket
            ))

            # write empty bucket1
            self.indexfile.seek(bucket1_pos)
            self.indexfile.write(struct.pack(
                Bucket.HEADER_FORMAT,
                bucket1.local_depth,
                bucket1.size,
                bucket1.next_bucket
            ))

            # init directory
            self.indexfile.seek(dir_start)

            for i in range(dir_entries):
                if i % 2 == 0:  # even last bit 0
                    self.indexfile.write(struct.pack(self.DIR_FORMAT, bucket0_pos))
                else:  # odd last bit
                    self.indexfile.write(struct.pack(self.DIR_FORMAT,bucket1_pos))
# Extendible Hashing Index
import struct
from ..core.record import Record
import os

BLOCK_FACTOR = 4


# max overflow 1 bucket
class Bucket:
    HEADER_FORMAT = "iii"  # local_depth, size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, local_depth=1, size=0, next_bucket=-1):
        self.local_depth = local_depth
        self.size = size
        self.next_bucket = next_bucket

    def insert(self, record: Record, datafile, indexfile, bucket_pos: int):
        # 1) check if size < block factor
        if self.size >= BLOCK_FACTOR:
            return False  # split or overflow

        # 2) write record to data file at the end
        datafile.seek(0, 2)
        offset = datafile.tell()
        datafile.write(record.pack())  # assume Record.pack() returns fixed 100B

        # 3) store offset in bucket
        indexfile.seek(bucket_pos + self.HEADER_SIZE + self.size * 4)
        indexfile.write(struct.pack("i", offset))

        # 4) update header
        self.size += 1
        indexfile.seek(bucket_pos)
        indexfile.write(struct.pack(
            self.HEADER_FORMAT,
            self.local_depth,
            self.size,
            self.next_bucket
        ))

        return True

    @staticmethod
    def search(datafile, indexfile, bucket_pos: int, key: bytes):
        # read header
        indexfile.seek(bucket_pos)
        local_depth, size, next_bucket = struct.unpack(
            Bucket.HEADER_FORMAT, indexfile.read(Bucket.HEADER_SIZE)
        )

        # scan pointers
        for i in range(size):
            indexfile.seek(bucket_pos + Bucket.HEADER_SIZE + i * 4)
            offset = struct.unpack("i", indexfile.read(4))[0]

            # go to data file and check record
            datafile.seek(offset)
            record = Record.unpack(datafile.read(Record.RECORD_SIZE))
            if record.key == key:
                return record

        # check overflow bucket
        if next_bucket != -1:
            return Bucket.search(datafile, indexfile, next_bucket, key)

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
            # write header
            self.indexfile.seek(0)
            self.indexfile.write(struct.pack(
                self.HEADER_FORMAT,
                self.global_depth,
                self.size
            ))

            dir_entries = 2 ** self.global_depth
            dir_start = self.HEADER_SIZE

            # make 2 initial buckets
            bucket0 = Bucket(local_depth=1)
            bucket1 = Bucket(local_depth=1)

            bucket_space = Bucket.HEADER_SIZE + BLOCK_FACTOR * 4

            bucket0_pos = dir_start + dir_entries * self.DIR_SIZE
            bucket1_pos = bucket0_pos + bucket_space

            # write empty headers
            self.indexfile.seek(bucket0_pos)
            self.indexfile.write(struct.pack(
                Bucket.HEADER_FORMAT, bucket0.local_depth, 0, -1
            ))
            self.indexfile.seek(bucket1_pos)
            self.indexfile.write(struct.pack(
                Bucket.HEADER_FORMAT, bucket1.local_depth, 0, -1
            ))

            # init directory
            self.indexfile.seek(dir_start)
            for i in range(dir_entries):
                if i % 2 == 0:
                    self.indexfile.write(struct.pack(self.DIR_FORMAT, bucket0_pos))
                else:
                    self.indexfile.write(struct.pack(self.DIR_FORMAT, bucket1_pos))

    def get_dir_index(self, key):
        hash_val = hash(key)
        return hash_val % (2 ** self.global_depth)

    def search(self, key):
        if self.size == 0:
            return None
        dir_index = self.get_dir_index(key)
        self.indexfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, self.indexfile.read(self.DIR_SIZE))[0]

        return Bucket.search(self.file, self.indexfile, bucket_pos, key)

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

    def search(self, datafile, bucketfile, key):
        bucketfile.seek(self.bucket_pos)
        local_depth, size, next_bucket = struct.unpack(
            Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE)
        )
        for i in range(size):
            bucketfile.seek(self.bucket_pos + Bucket.HEADER_SIZE + i * 4)
            offset = struct.unpack("i", bucketfile.read(4))[0]
            datafile.seek(offset)
            record = Record.unpack(datafile.read(Record.RECORD_SIZE))
            if record.key == key:
                return record
        if next_bucket != -1:
            bucketfile.seek(next_bucket)
            local_depth, size, next_bucket = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
            bucket = Bucket(bucket_pos=next_bucket, local_depth=local_depth, size=size, next_bucket=next_bucket)
            return bucket.search(datafile, bucketfile, key)
        return None

    def insert(self, offset, bucketfile, global_depth, overflow_count=0):
        if self.size >= BLOCK_FACTOR:
            if self.local_depth < global_depth:
                # TODO: perform bucket split + redistribute
                return False
            else:
                if overflow_count < MAX_OVERFLOW:
                    return self.handle_overflow(offset, bucketfile, global_depth, overflow_count)
                else:
                    return False  # insertion failed, global depth must be doubled in directory
        # store offset in bucket
        bucketfile.seek(self.bucket_pos + self.HEADER_SIZE + self.size * 4)
        bucketfile.write(struct.pack("i", offset))
        # update header
        self.size += 1
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(
            self.HEADER_FORMAT,
            self.local_depth,
            self.size,
            self.next_bucket
        ))
        return True
    
        # inside Bucket
    def delete(self, key, datafile, bucketfile):
        bucketfile.seek(self.bucket_pos)
        local_depth, size, next_bucket = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))

        for i in range(size):
            bucketfile.seek(self.bucket_pos + Bucket.HEADER_SIZE + i * 4)
            offset = struct.unpack("i", bucketfile.read(4))[0]

            if offset < 0:
                continue  # tombstone, skip

            datafile.seek(offset)
            record = Record.unpack(datafile.read(Record.RECORD_SIZE))
            if record.key == key:
                # mark as deleted by storing negative offset
                bucketfile.seek(self.bucket_pos + Bucket.HEADER_SIZE + i * 4)
                bucketfile.write(struct.pack("i", -offset))
                return True

        # check overflow bucket
        if next_bucket != -1:
            overflow_bucket = Bucket(next_bucket)
            return overflow_bucket.delete(key, datafile, bucketfile)

        return False



    def handle_overflow(self, offset, bucketfile, global_depth, overflow_count):
        # traverse chain if it exists
        if self.next_bucket != -1:
            bucketfile.seek(self.next_bucket)
            local_depth, size, next_bucket = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
            overflow_bucket = Bucket(self.next_bucket, local_depth, size, next_bucket)
            return overflow_bucket.insert(offset, bucketfile, global_depth, overflow_count + 1)

        # allocate new overflow bucket
        bucketfile.seek(0, os.SEEK_END)
        new_pos = bucketfile.tell()
        new_bucket = Bucket(new_pos, local_depth=self.local_depth)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, new_bucket.local_depth, 0, -1))

        # link to new overflow
        self.next_bucket = new_pos
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(
            self.HEADER_FORMAT,
            self.local_depth,
            self.size,
            self.next_bucket
        ))

        # insert into new bucket
        return new_bucket.insert(offset, bucketfile, global_depth, overflow_count + 1)


class ExtendableHashing:  # directory
    HEADER_FORMAT = "ii"  # globaldepth, num of elements
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"  # pointer to bucket
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, filename, dirname, bucketname, global_depth=3, size=0):
        self.filename = filename
        self.dirname = dirname
        self.bucketname = bucketname
        self.file = open(filename, 'r+b')   # data file
        create_new = not os.path.exists(dirname) or not os.path.exists(bucketname)
        self.dirfile = open(dirname, 'w+b' if create_new else 'r+b')  # directory file
        self.bucketfile = open(bucketname, 'w+b' if create_new else 'r+b')  # buckets file
        self.size = size
        self.global_depth = global_depth

        if create_new:
            # write header in directory file
            self.dirfile.seek(0)
            self.dirfile.write(struct.pack(
                self.HEADER_FORMAT,
                self.global_depth,
                self.size
            ))
            dir_entries = 2 ** self.global_depth
            dir_start = self.HEADER_SIZE

            # make 2 initial buckets
            bucket_space = Bucket.HEADER_SIZE + BLOCK_FACTOR * 4
            bucket0_pos = 0
            bucket1_pos = bucket0_pos + bucket_space
            bucket0 = Bucket(bucket0_pos, local_depth=1)
            bucket1 = Bucket(bucket1_pos, local_depth=1)

            # write empty headers to bucket file
            self.bucketfile.seek(bucket0_pos)
            self.bucketfile.write(struct.pack(
                Bucket.HEADER_FORMAT, bucket0.local_depth, 0, -1
            ))
            self.bucketfile.seek(bucket1_pos)
            self.bucketfile.write(struct.pack(
                Bucket.HEADER_FORMAT, bucket1.local_depth, 0, -1
            ))

            # init directory pointers
            self.dirfile.seek(dir_start)
            for i in range(dir_entries):
                if i % 2 == 0:
                    self.dirfile.write(struct.pack(self.DIR_FORMAT, bucket0_pos))
                else:
                    self.dirfile.write(struct.pack(self.DIR_FORMAT, bucket1_pos))

    def get_dir_index(self, key):
        hash_val = hash(key)
        return hash_val % (2 ** self.global_depth)

    def get_bucket(self, key):
        dir_index = self.get_dir_index(key)
        self.dirfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]
        self.bucketfile.seek(bucket_pos)
        local_depth, size, next_bucket = struct.unpack(
            Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE)
        )
        return Bucket(bucket_pos, local_depth, size, next_bucket)

    def search(self, key):
        if self.size == 0:
            return None
        bucket = self.get_bucket(key)
        return bucket.search(self.file, self.bucketfile, key)

    def insert(self, offset: int, key):
        if self.size == 0:
            self.size = 1
        bucket = self.get_bucket(key)
        success = bucket.insert(offset, self.bucketfile, self.global_depth)
        if not success:
            self.handle_splitting()
            pass
        self.size += 1

    def delete(self, key):
        bucket = self.get_bucket(key)
        deleted = bucket.delete(key, self.file, self.bucketfile)
        if deleted:
            self.size -= 1
        return deleted

    def handle_splitting(self, bucket, offset):
        # create new bucket at the end of the bucket file
        self.indexfile.seek(0, 2)
        new_pos = self.indexfile.tell()
        new_bucket = Bucket(new_pos, local_depth=bucket.local_depth + 1)
        self.indexfile.write(struct.pack(
            Bucket.HEADER_FORMAT, new_bucket.local_depth, 0, -1
        ))

        # increase local depth of old bucket and reset size (we'll redistribute)
        bucket.local_depth += 1
        self.indexfile.seek(bucket.bucket_pos)
        self.indexfile.write(struct.pack(
            Bucket.HEADER_FORMAT,
            bucket.local_depth,
            0,   # reset size
            -1   # reset overflow
        ))

        # collect all records, there is no overflow because we are splitting so no need to look for it
        records_to_redistribute = []
        for i in range(bucket.size):
            self.indexfile.seek(bucket.bucket_pos + Bucket.HEADER_SIZE + i * 4)
            records_to_redistribute.append(struct.unpack("i", self.indexfile.read(4))[0])
        records_to_redistribute.append(offset)

        # update directory entries
        dir_entries = 2 ** self.global_depth
        for i in range(dir_entries):
            self.indexfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
            pos = struct.unpack(self.DIR_FORMAT, self.indexfile.read(self.DIR_SIZE))[0]

            # only update entries that currently point to old bucket
            if pos == bucket.bucket_pos:
                # decide which bucket this directory entry should point to
                split_point = 2 ** (bucket.local_depth - 1)
                if i % (2 ** bucket.local_depth) < split_point:
                    self.indexfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    self.indexfile.write(struct.pack(self.DIR_FORMAT, bucket.bucket_pos))
                else:
                    self.indexfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    self.indexfile.write(struct.pack(self.DIR_FORMAT, new_pos))

        # 5. reinsert all records into correct bucket
        for rec_offset in records_to_redistribute:
            # get key of record from data file
            self.file.seek(rec_offset)
            record = Record.unpack(self.file.read(Record.RECORD_SIZE))
            bucket_to_insert = self.get_bucket(record.key)
            bucket_to_insert.insert(rec_offset, self.indexfile, self.global_depth)



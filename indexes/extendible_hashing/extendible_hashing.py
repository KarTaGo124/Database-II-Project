# Extendible Hashing Index
import struct
import os
from ..core.record import Record, IndexRecord

# --- Configuration ---
BLOCK_FACTOR = 4  # Max number of IndexRecords per bucket


class Bucket:
    HEADER_FORMAT = "ii"  # local_depth, size
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, bucket_pos, index_record_size, local_depth=1, size=0):
        self.bucket_pos = bucket_pos
        self.local_depth = local_depth
        self.size = size
        self.index_record_size = index_record_size  # Stores the size of one IndexRecord

    def get_all_records(self, bucketfile):
        """ Reads and returns all packed IndexRecord entries from this bucket. """
        records = []
        if self.size == 0:
            return records

        bucketfile.seek(self.bucket_pos + self.HEADER_SIZE)
        for _ in range(self.size):
            packed_record = bucketfile.read(self.index_record_size)
            # Avoid adding tombstone records
            if packed_record != (b'\x00' * self.index_record_size):
                records.append(packed_record)
        return records

    def insert(self, index_record: IndexRecord, bucketfile):
        """ Inserts a new IndexRecord into the bucket. """
        packed_record = index_record.pack()
        # Write the packed record to the first available slot
        bucketfile.seek(self.bucket_pos + self.HEADER_SIZE + (self.size * self.index_record_size))
        bucketfile.write(packed_record)

        # Update the bucket's header
        self.size += 1
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(self.HEADER_FORMAT, self.local_depth, self.size))
        return True

    def clear(self, bucketfile):
        """ Resets the bucket's size"""
        self.size = 0
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(self.HEADER_FORMAT, self.local_depth, 0))



class ExtendibleHashingSecondaryIndex:
    HEADER_FORMAT = "ii"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, field_name, field_type, field_size, primary_index, filename, global_depth=3):
        self.field_name = field_name
        self.field_type = field_type
        self.field_size = field_size
        self.primary_index = primary_index
        self.filename = filename
        self.dirname = filename.replace('.dat', '_dir.dat')
        self.bucketname = filename.replace('.dat', '_bucket.dat')
        self.global_depth = global_depth
        self.size = 0

        self.index_record_template = IndexRecord(field_type, field_size)
        self.index_record_size = self.index_record_template.RECORD_SIZE

        create_new = not os.path.exists(self.dirname) or not os.path.exists(self.bucketname)
        self.dirfile = open(self.dirname, 'w+b' if create_new else 'r+b')
        self.bucketfile = open(self.bucketname, 'w+b' if create_new else 'r+b')

        if create_new:
            self._initialize_directory()
        else:
            self._load_header()

    def _initialize_directory(self):
        self.dirfile.seek(0)
        self.dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, self.size))

        dir_entries = 2 ** self.global_depth
        bucket_space = Bucket.HEADER_SIZE + BLOCK_FACTOR * self.index_record_size
        bucket0_pos = 0
        bucket1_pos = bucket0_pos + bucket_space

        self.bucketfile.seek(bucket0_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, 1, 0, -1))
        self.bucketfile.seek(bucket1_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, 1, 0, -1))

        self.dirfile.seek(self.HEADER_SIZE)
        for i in range(dir_entries):
            if i % 2 == 0:
                self.dirfile.write(struct.pack(self.DIR_FORMAT, bucket0_pos))
            else:
                self.dirfile.write(struct.pack(self.DIR_FORMAT, bucket1_pos))

    def _load_header(self):
        self.dirfile.seek(0)
        self.global_depth, self.size = struct.unpack(self.HEADER_FORMAT, self.dirfile.read(self.HEADER_SIZE))

    def _update_header(self):
        self.dirfile.seek(0)
        self.dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, self.size))

    def _get_dir_index(self, value):
        if isinstance(value, str):
            hash_val = hash(value)
        else:
            hash_val = hash(str(value))
        return hash_val % (2 ** self.global_depth)

    def _get_bucket(self, value):
        dir_index = self._get_dir_index(value)
        self.dirfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]

        self.bucketfile.seek(bucket_pos)
        local_depth, size, next_bucket = struct.unpack(
            Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE)
        )
        return Bucket(bucket_pos, self.index_record_size, local_depth, size, next_bucket)

    def search(self, value):
        if self.size == 0:
            return []

        bucket = self._get_bucket(value)
        return bucket.search(self.bucketfile, value, self.primary_index, self.index_record_template)

    def insert(self, record):
        field_value = record.get_field_value(self.field_name)
        primary_key = record.get_key()

        index_record = IndexRecord(self.field_type, self.field_size)
        index_record.set_index_data(field_value, primary_key)

        bucket = self._get_bucket(field_value)
        success = bucket.insert(index_record, self.bucketfile, self.global_depth)

        if not success:
            if bucket.local_depth < self.global_depth:
                self._handle_bucket_split(bucket, index_record, field_value)
            else:
                self._handle_directory_doubling()
                self.insert(record)
                return

        self.size += 1
        self._update_header()

    def delete(self, record):
        field_value = record.get_field_value(self.field_name)
        primary_key = record.get_key()

        bucket = self._get_bucket(field_value)
        deleted = bucket.delete(primary_key, field_value, self.bucketfile, self.index_record_template)

        if deleted:
            self.size -= 1
            self._update_header()

        return deleted

    def _delete_from_bucket(self, bucket, primary_key):
        self.bucketfile.seek(bucket.bucket_pos)
        local_depth, size, next_bucket = struct.unpack(Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE))

        for i in range(size):
            self.bucketfile.seek(bucket.bucket_pos + Bucket.HEADER_SIZE + i * 4)
            stored_key = struct.unpack("i", self.bucketfile.read(4))[0]

            if stored_key == primary_key:
                self.bucketfile.seek(bucket.bucket_pos + Bucket.HEADER_SIZE + i * 4)
                self.bucketfile.write(struct.pack("i", -primary_key))
                return True

        if next_bucket != -1:
            overflow_bucket = Bucket(next_bucket)
            return self._delete_from_bucket(overflow_bucket, primary_key)

        return False

    def _handle_bucket_split(self, bucket, index_record, field_value):
        self.bucketfile.seek(0, os.SEEK_END)
        new_pos = self.bucketfile.tell()

        bucket.local_depth += 1
        new_bucket = Bucket(new_pos, self.index_record_size, local_depth=bucket.local_depth)

        self.bucketfile.seek(new_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, new_bucket.local_depth, 0, -1))

        all_index_records = bucket.get_all_index_records(self.bucketfile, self.index_record_template)
        all_index_records.append(index_record)

        self.bucketfile.seek(bucket.bucket_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, bucket.local_depth, 0, -1))

        self._update_directory_pointers(bucket, new_bucket)

        for idx_record in all_index_records:
            target_bucket = self._get_bucket(idx_record.index_value)
            target_bucket.insert(idx_record, self.bucketfile, self.global_depth)

        self._update_directory_pointers(bucket, new_bucket)

        for idx_record in all_index_records:
            target_bucket = self._get_bucket(idx_record.index_value)
            target_bucket.insert(idx_record, self.bucketfile, self.global_depth)

    def _update_directory_pointers(self, old_bucket, new_bucket):
        dir_entries = 2 ** self.global_depth

        for i in range(dir_entries):
            self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
            pos = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]

            if pos == old_bucket.bucket_pos:
                split_point = 2 ** (old_bucket.local_depth - 1)
                if i % (2 ** old_bucket.local_depth) < split_point:
                    self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    self.dirfile.write(struct.pack(self.DIR_FORMAT, old_bucket.bucket_pos))
                else:
                    self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    self.dirfile.write(struct.pack(self.DIR_FORMAT, new_bucket.bucket_pos))

    def _handle_directory_doubling(self):
        self.global_depth += 1
        old_entries = 2 ** (self.global_depth - 1)
        new_entries = 2 ** self.global_depth

        self.dirfile.seek(0, os.SEEK_END)
        for i in range(old_entries):
            self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
            bucket_pos = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]

            self.dirfile.seek(self.HEADER_SIZE + (old_entries + i) * self.DIR_SIZE)
            self.dirfile.write(struct.pack(self.DIR_FORMAT, bucket_pos))

        self._update_header()

    def drop_index(self):
        self.dirfile.close()
        self.bucketfile.close()

        removed_files = []
        try:
            os.remove(self.dirname)
            removed_files.append(self.dirname)
        except:
            pass
        try:
            os.remove(self.bucketname)
            removed_files.append(self.bucketname)
        except:
            pass

        return removed_files
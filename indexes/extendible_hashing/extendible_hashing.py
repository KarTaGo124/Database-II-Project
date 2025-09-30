import struct
import os
import hashlib
from ..core.record import Record, IndexRecord
from ..core.performance_tracker import PerformanceTracker, OperationResult

BLOCK_FACTOR = 8
MAX_OVERFLOW = 2


class Bucket:
    HEADER_FORMAT = "iiii"  # local_depth, allocated_slots, actual_size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, bucket_pos, index_record_size, local_depth=1, allocated_slots=0, actual_size=0, next_bucket=-1):
        self.bucket_pos = bucket_pos
        self.local_depth = local_depth
        self.allocated_slots = allocated_slots  # how many slots used
        self.actual_size = actual_size  # actual size
        self.next_bucket = next_bucket
        self.index_record_size = index_record_size

    def get_all_records(self, bucketfile):
        all_records = []
        current_bucket = self
        while current_bucket is not None:
            if current_bucket.allocated_slots > 0:
                bucketfile.seek(current_bucket.bucket_pos + self.HEADER_SIZE)
                for _ in range(current_bucket.allocated_slots):
                    packed_record = bucketfile.read(self.index_record_size)
                    if packed_record != (b'\x00' * self.index_record_size):
                        all_records.append(packed_record)
            if current_bucket.next_bucket != -1:
                bucketfile.seek(current_bucket.next_bucket)
                ld, slots, size, nb = struct.unpack(self.HEADER_FORMAT, bucketfile.read(self.HEADER_SIZE))
                current_bucket = Bucket(current_bucket.next_bucket, self.index_record_size, ld, slots, size, nb)
            else:
                current_bucket = None
        return all_records

    def insert(self, packed_record, bucketfile):
        tombstone = b'\x00' * self.index_record_size
        insert_position = None
        # First, look for tombstones in existing allocated slots
        for i in range(self.allocated_slots):
            slot_pos = self.bucket_pos + self.HEADER_SIZE + (i * self.index_record_size)
            bucketfile.seek(slot_pos)
            if bucketfile.read(self.index_record_size) == tombstone:
                insert_position = slot_pos
                break
        # If no tombstone found and we have space, allocate a new slot
        if insert_position is None:
            if self.allocated_slots < BLOCK_FACTOR:
                insert_position = self.bucket_pos + self.HEADER_SIZE + (self.allocated_slots * self.index_record_size)
                self.allocated_slots += 1
            else:
                return False  # Bucket is full
        bucketfile.seek(insert_position)
        bucketfile.write(packed_record)
        self.actual_size += 1
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(self.HEADER_FORMAT, self.local_depth, self.allocated_slots,
                                     self.actual_size, self.next_bucket))
        return True

    def delete(self, secondary_key, bucketfile, index_record_template):
        current_bucket = self
        tombstone = b'\x00' * self.index_record_size
        deleted_count = 0

        while current_bucket is not None:
            # Check all allocated slots
            for i in range(current_bucket.allocated_slots):
                record_pos = current_bucket.bucket_pos + self.HEADER_SIZE + (i * self.index_record_size)
                bucketfile.seek(record_pos)
                packed_record = bucketfile.read(self.index_record_size)
                if packed_record == tombstone:
                    continue
                try:
                    unpacked = IndexRecord.unpack(packed_record, index_record_template.value_type_size, "index_value")
                    stored_key = unpacked.index_value
                    if isinstance(stored_key, bytes):
                        stored_key = stored_key.decode('utf-8').strip('\x00').strip()
                    else:
                        stored_key = str(stored_key).strip()

                    if stored_key == secondary_key:
                        bucketfile.seek(record_pos)
                        bucketfile.write(tombstone)
                        current_bucket.actual_size -= 1
                        deleted_count += 1
                        bucketfile.seek(current_bucket.bucket_pos)
                        bucketfile.write(struct.pack(self.HEADER_FORMAT, current_bucket.local_depth,
                                                     current_bucket.allocated_slots, current_bucket.actual_size,
                                                     current_bucket.next_bucket))
                except struct.error:
                    continue

            if current_bucket.next_bucket != -1:
                bucketfile.seek(current_bucket.next_bucket)
                ld, slots, size, nb = struct.unpack(self.HEADER_FORMAT, bucketfile.read(self.HEADER_SIZE))
                current_bucket = Bucket(current_bucket.next_bucket, self.index_record_size, ld, slots, size, nb)
            else:
                break

        return deleted_count

class ExtendibleHashing:
    HEADER_FORMAT = "ii"  # global_depth, free_pointer
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, data_filename, index_field_name, index_field_type, index_field_size, is_primary=False):
        if is_primary:
            raise ValueError("ExtendibleHashing can only be used as secondary index")

        self.index_field_name = index_field_name
        self.dirname = f"{data_filename}_{index_field_name}.dir"
        self.bucketname = f"{data_filename}_{index_field_name}.bkt"
        self.index_record_template = IndexRecord(index_field_type, index_field_size)
        self.index_record_size = self.index_record_template.RECORD_SIZE
        self.performance = PerformanceTracker()
        create_new = not os.path.exists(self.dirname) or not os.path.exists(self.bucketname)
        self.dirfile = open(self.dirname, 'r+b' if not create_new else 'w+b')
        self.bucketfile = open(self.bucketname, 'r+b' if not create_new else 'w+b')
        if create_new:
            self._initialize_files()
        else:
            self.global_depth, self.free_pointer = self._read_header()

    def _hash_key(self, key):
        if isinstance(key, str):
            key = key.encode('utf-8')
        elif not isinstance(key, bytes):
            key = str(key).encode('utf-8')
        return int(hashlib.md5(key).hexdigest(), 16)

    def _get_bucket_from_key(self, key):
        hash_val = self._hash_key(key)
        dir_index = hash_val % (2 ** self.global_depth)
        self.dirfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]
        self.bucketfile.seek(bucket_pos)
        ld, slots, size, nb = struct.unpack(Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE))
        return Bucket(bucket_pos, self.index_record_size, ld, slots, size, nb)

    def _append_new_bucket(self, local_depth):
        if self.free_pointer == -1:
            self.bucketfile.seek(0, 2)
            new_pos = self.bucketfile.tell()
        else:
            new_pos = self.free_pointer
            self.bucketfile.seek(new_pos)
            _, _, _, self.free_pointer = struct.unpack(Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE))
            self._write_header()

        self.bucketfile.seek(new_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, local_depth, 0, 0, -1))
        return new_pos

    def search(self, key):
        self.performance.start_operation()

        bucket = self._get_bucket_from_key(key)
        packed_records = bucket.get_all_records(self.bucketfile)
        matching_pks = []
        disk_reads = 1

        for packed_rec in packed_records:
            try:
                unpacked = IndexRecord.unpack(packed_rec, self.index_record_template.value_type_size, "index_value")
                value = unpacked.index_value
                if isinstance(value, bytes):
                    value = value.decode('utf-8').strip('\x00').strip()
                else:
                    value = str(value).strip()
                if value == key:
                    matching_pks.append(unpacked.primary_key)
            except struct.error:
                continue

        self.performance.reads = disk_reads
        self.performance.writes = 0
        return self.performance.end_operation(matching_pks)

    def insert(self, data_record: Record):
        self.performance.start_operation()

        secondary_key = getattr(data_record, self.index_field_name, None)
        if secondary_key is None:
            secondary_key = data_record.__dict__.get(self.index_field_name)
        primary_key = data_record.get_key()
        index_entry = IndexRecord(self.index_record_template.value_type_size[0][1],
                                  self.index_record_template.value_type_size[0][2])
        index_entry.set_index_data(index_value=secondary_key, primary_key=primary_key)
        packed_record = index_entry.pack()

        head_bucket = self._get_bucket_from_key(secondary_key)
        current_bucket = head_bucket
        overflow_count = 0
        disk_reads = 1
        disk_writes = 0

        while True:
            if current_bucket.insert(packed_record, self.bucketfile):
                disk_writes += 1
                self.performance.reads = disk_reads
                self.performance.writes = disk_writes
                return self.performance.end_operation(True)
            if current_bucket.next_bucket != -1:
                overflow_count += 1
                disk_reads += 1
                self.bucketfile.seek(current_bucket.next_bucket)
                ld, slots, size, nb = struct.unpack(Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE))
                current_bucket = Bucket(current_bucket.next_bucket, self.index_record_size, ld, slots, size, nb)
            else:
                break

        if overflow_count < MAX_OVERFLOW:
            new_bucket_pos = self._append_new_bucket(head_bucket.local_depth)
            current_bucket.next_bucket = new_bucket_pos
            self.bucketfile.seek(current_bucket.bucket_pos)
            self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, current_bucket.local_depth,
                                              current_bucket.allocated_slots, current_bucket.actual_size,
                                              current_bucket.next_bucket))
            new_bucket = Bucket(new_bucket_pos, self.index_record_size, head_bucket.local_depth, 0, 0, -1)
            new_bucket.insert(packed_record, self.bucketfile)
            disk_writes += 3
        else:
            self._split_bucket(head_bucket, index_entry)
            disk_writes += 2

        self.performance.reads = disk_reads
        self.performance.writes = disk_writes
        return self.performance.end_operation(True)

    def delete(self, secondary_key):
        self.performance.start_operation()

        bucket = self._get_bucket_from_key(secondary_key)
        deleted_count = bucket.delete(secondary_key, self.bucketfile, self.index_record_template)
        disk_reads = 1
        disk_writes = 1 if deleted_count > 0 else 0

        self.performance.reads = disk_reads
        self.performance.writes = disk_writes
        return self.performance.end_operation(deleted_count > 0)

    def _split_bucket(self, head_bucket: Bucket, new_index_record: IndexRecord):
        if head_bucket.local_depth == self.global_depth:
            self._double_directory()

        all_records_packed = head_bucket.get_all_records(self.bucketfile)
        all_records_packed.append(new_index_record.pack())

        # Free overflow buckets
        next_pos = head_bucket.next_bucket
        while next_pos != -1:
            self.bucketfile.seek(next_pos)
            _, _, _, next_in_chain = struct.unpack(Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE))
            self.free_bucket(next_pos)
            next_pos = next_in_chain

        # Reset head bucket
        head_bucket.local_depth += 1
        head_bucket.allocated_slots = 0
        head_bucket.actual_size = 0
        head_bucket.next_bucket = -1
        self.bucketfile.seek(head_bucket.bucket_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, head_bucket.local_depth, 0, 0, -1))

        # Create new bucket
        new_bucket_pos = self._append_new_bucket(local_depth=head_bucket.local_depth)
        self._update_directory_pointers(head_bucket.bucket_pos, new_bucket_pos, head_bucket.local_depth)

        # Re-distribute all records
        for packed_rec in all_records_packed:
            unpacked = Record.unpack(packed_rec, self.index_record_template.value_type_size, "index_value")
            self.insert(unpacked)

    def _read_header(self):
        self.dirfile.seek(0)
        return struct.unpack(self.HEADER_FORMAT, self.dirfile.read(self.HEADER_SIZE))

    def _write_header(self):
        self.dirfile.seek(0)
        self.dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, self.free_pointer))

    def free_bucket(self, bucket_pos):
        self.bucketfile.seek(bucket_pos)
        next_free = self.free_pointer
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, 0, 0, 0, next_free))
        self.free_pointer = bucket_pos
        self._write_header()

    def _initialize_files(self, initial_depth=3):
        self.global_depth = initial_depth
        self.free_pointer = -1
        self._write_header()
        bucket0_pos = self._append_new_bucket(local_depth=1)
        bucket1_pos = self._append_new_bucket(local_depth=1)
        self.dirfile.seek(self.HEADER_SIZE)
        for i in range(2 ** self.global_depth):
            bucket_pos = bucket0_pos if (i % 2 == 0) else bucket1_pos
            self.dirfile.write(struct.pack(self.DIR_FORMAT, bucket_pos))

    def _double_directory(self):
        self.dirfile.seek(0)
        global_depth, _ = self._read_header()
        dir_size = 2 ** global_depth

        self.dirfile.seek(self.HEADER_SIZE)
        buf = self.dirfile.read(dir_size * self.DIR_SIZE)
        current_dir = [entry[0] for entry in struct.iter_unpack(self.DIR_FORMAT, buf)]

        self.global_depth += 1
        self._write_header()

        self.dirfile.seek(self.HEADER_SIZE)
        for entry in current_dir:
            self.dirfile.write(struct.pack(self.DIR_FORMAT, entry))
            self.dirfile.write(struct.pack(self.DIR_FORMAT, entry))

    def _update_directory_pointers(self, old_bucket_pos, new_bucket_pos, new_local_depth):
        dir_size = 2 ** self.global_depth
        for i in range(dir_size):
            self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
            current_ptr = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]
            if current_ptr == old_bucket_pos:
                bit_position = new_local_depth - 1
                if (i >> bit_position) & 1:  # If bit is 1, point to new bucket
                    self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    self.dirfile.write(struct.pack(self.DIR_FORMAT, new_bucket_pos))

    def drop_index(self):
        removed_files = []

        try:
            self.close()

            files_to_remove = [self.dirname, self.bucketname]

            for file_path in files_to_remove:
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        removed_files.append(file_path)
                    except OSError:
                        pass
        except Exception:
            pass

        return removed_files

    def close(self):
        self.dirfile.close()
        self.bucketfile.close()
import struct
import os
import hashlib
from ..core.record import IndexRecord
from ..core.performance_tracker import PerformanceTracker

BLOCK_FACTOR = 8
MAX_OVERFLOW = 2


class Bucket:
    HEADER_FORMAT = "iiii" # local_depth, allocated_slots, actual_size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, bucket_pos, index_record_size, local_depth=1, num_slots=0, num_records=0, next_overflow_bucket=-1, bucketfile=None, performance=None):
        self.bucket_pos = bucket_pos
        self.local_depth = local_depth
        self.num_slots = num_slots
        self.num_records = num_records
        self.next_overflow_bucket = next_overflow_bucket
        self.index_record_size = index_record_size
        self.elements = [] #memory loaded elements
        self.performance = performance

        current_bucket = self
        if current_bucket.num_records > 0:  # if there are records, search them in all slots. slots != records
            bucketfile.seek(current_bucket.bucket_pos + Bucket.HEADER_SIZE)
            for _ in range(current_bucket.num_slots):
                packed_record = bucketfile.read(self.index_record_size)
                if packed_record != (b'\x00' * self.index_record_size):
                    self.elements.append(IndexRecord.unpack(packed_record,self.index_record_size, "index_value"))
            self.performance.track_read()

    def is_full(self):
        return self.num_records >= BLOCK_FACTOR

    def has_space(self):
        return self.num_records < BLOCK_FACTOR

    def get_next_overflow(self, bucketfile):
        if self.next_overflow_bucket != -1:
            bucketfile.seek(self.next_overflow_bucket)
            ld, num_slots, num_records, next_overflow = struct.unpack(Bucket.HEADER_FORMAT,bucketfile.read(Bucket.HEADER_SIZE))
            self.performance.track_read()
            return Bucket(self.next_overflow_bucket, self.index_record_size, ld, num_slots,
                                    num_records, next_overflow)
        return None

    def search(self, secondary_value):
        self.performance.start_operation()
        matching_pks = []
        for e in self.elements:
            try:
                value = e.index_value
                print(value)
                value = str(value).strip()
                if value == secondary_value:
                    matching_pks.append(e.primary_key)
            except struct.error:
                continue
        return matching_pks


class ExtendibleHashing:
    HEADER_FORMAT = "ii" #global_depth, free pointer
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"#bucket pointer
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, data_filename, index_field_name, index_field_type, index_field_size, is_primary=False):
        if is_primary:
            raise ValueError("ExtendibleHashing can only be used as secondary index")

        self.index_field_name = index_field_name
        self.dirname = f"{data_filename}.dir"
        self.bucketname = f"{data_filename}.bkt"
        self.index_record_template = IndexRecord(index_field_type, index_field_size)
        self.index_record_size = self.index_record_template.RECORD_SIZE
        self.performance = PerformanceTracker()

        if not os.path.exists(self.dirname) or not os.path.exists(self.bucketname):
            self._initialize_files()

        self.global_depth, self.first_free_bucket_pos = self._read_header()

    def _hash_key(self, key):
        if isinstance(key, str):
            key = key.encode('utf-8')
        elif not isinstance(key, bytes):
            key = str(key).encode('utf-8')
        return int(hashlib.md5(key).hexdigest(), 16)

    def _read_header(self):
        with open(self.dirname, 'rb') as dirfile:
            return struct.unpack(self.HEADER_FORMAT, dirfile.read(self.HEADER_SIZE))

    def _write_header(self):
        with open(self.dirname, 'r+b') as dirfile:
            dirfile.seek(0)
            dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, self.first_free_bucket_pos))

    def search(self, secondary_value):
        self.performance.start_operation()

        with open(self.dirname, 'rb') as dirfile, open(self.bucketname, 'rb') as bucketfile:
            bucket = self._get_bucket_from_key(secondary_value, dirfile, bucketfile)
            matching_pk =  bucket.search(secondary_value)
            while bucket is not None:
                bucket = bucket.get_next_overflow(bucketfile)
                matching_pk = matching_pk + bucket.search(secondary_value)

            # all_records = []
            # current_bucket = bucket
            #
            # # Leer bucket principal y cadena de overflow
            # while current_bucket is not None:
            #     if current_bucket.num_records > 0: #if there are records, search them in all slots. slots != records
            #         bucketfile.seek(current_bucket.bucket_pos + Bucket.HEADER_SIZE)
            #         for _ in range(current_bucket.num_slots):
            #             packed_record = bucketfile.read(self.index_record_size)
            #             if packed_record != (b'\x00' * self.index_record_size):
            #                 all_records.append(packed_record)
            #         self.performance.track_read()
            #
            #     if current_bucket.next_overflow_bucket != -1:
            #         bucketfile.seek(current_bucket.next_overflow_bucket)
            #         ld, num_slots, num_records, next_overflow = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
            #         self.performance.track_read()
            #         current_bucket = Bucket(current_bucket.next_overflow_bucket, self.index_record_size, ld, num_slots, num_records, next_overflow)
            #     else:
            #         current_bucket = None

            # matching_pks = []
            # search_value = secondary_value
            # if isinstance(search_value, bytes):
            #     search_value = search_value.decode('utf-8').strip('\x00').strip()
            # else:
            #     search_value = str(search_value).strip()
            # for packed_rec in all_records:
            #     try:
            #         unpacked = IndexRecord.unpack(packed_rec, self.index_record_template.value_type_size, "index_value")
            #         value = unpacked.index_value
            #         if isinstance(value, bytes):
            #             value = value.decode('utf-8').strip('\x00').strip()
            #         else:
            #             value = str(value).strip()
            #         if value == search_value:
            #             matching_pks.append(unpacked.primary_key)
            #     except struct.error:
            #         continue

            return self.performance.end_operation(matching_pks)

    def insert(self, index_record: IndexRecord):
        self.performance.start_operation()

        secondary_value = index_record.index_value
        if secondary_value is None:
            return self.performance.end_operation(True)

        # if isinstance(secondary_value, bytes):
        #     secondary_value = secondary_value.decode('utf-8').strip('\x00').strip()

        with open(self.dirname, 'r+b') as dirfile, open(self.bucketname, 'r+b') as bucketfile:
            self._insert_index_record(index_record, dirfile, bucketfile)
            return self.performance.end_operation(True)

    def delete(self, secondary_value, primary_key=None):
        self.performance.start_operation()

        with open(self.dirname, 'r+b') as dirfile, open(self.bucketname, 'r+b') as bucketfile:
            bucket = self._get_bucket_from_key(secondary_value, dirfile, bucketfile)
            deleted_pks = self._delete_from_bucket_chain(bucket, secondary_value, primary_key, bucketfile)

            # Liberar bucket si está completamente vacío
            if len(deleted_pks) > 0 and bucket.num_records == 0:
                self._handle_empty_bucket(bucket, dirfile, bucketfile)

            if primary_key is None:
                return self.performance.end_operation(deleted_pks)
            else:
                return self.performance.end_operation(len(deleted_pks) > 0)

    def _get_bucket_from_key(self, key, dirfile, bucketfile):
        hash_val = self._hash_key(key)
        dir_index = hash_val % (2 ** self.global_depth)
        dirfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
        self.performance.track_read()

        bucketfile.seek(bucket_pos)
        local_depth, num_slots, num_records, next_overflow = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
        self.performance.track_read()

        return Bucket(bucket_pos, self.index_record_size, local_depth, num_slots, num_records, next_overflow)

    def _insert_index_record(self, index_record, dirfile, bucketfile):
        packed_record = index_record.pack()
        head_bucket = self._get_bucket_from_key(index_record.index_value, dirfile, bucketfile)
        current_bucket = head_bucket
        overflow_count = 0

        # Intentar insertar en cadena de buckets existente
        while True:
            if current_bucket.has_space():
                success = self._insert_in_bucket(current_bucket, packed_record, bucketfile)
                if success:
                    return

            if current_bucket.next_overflow_bucket != -1:
                overflow_count += 1
                bucketfile.seek(current_bucket.next_overflow_bucket)
                ld, num_slots, num_records, next_overflow = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
                self.performance.track_read()
                current_bucket = Bucket(current_bucket.next_overflow_bucket, self.index_record_size, ld, num_slots, num_records, next_overflow)
            else:
                break

        # Sin espacio - dividir bucket o crear overflow
        if head_bucket.local_depth < self.global_depth:
            self._split_bucket(head_bucket, index_record, dirfile, bucketfile)
        else:
            if overflow_count < MAX_OVERFLOW:
                new_bucket_pos = self._append_new_bucket(head_bucket.local_depth, bucketfile)
                current_bucket.next_overflow_bucket = new_bucket_pos
                bucketfile.seek(current_bucket.bucket_pos)
                bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, current_bucket.local_depth,
                                            current_bucket.num_slots, current_bucket.num_records,
                                            current_bucket.next_overflow_bucket))
                self.performance.track_write()
                new_bucket = Bucket(new_bucket_pos, self.index_record_size, head_bucket.local_depth, 0, 0, -1)
                self._insert_in_bucket(new_bucket, packed_record, bucketfile)
            else:
                self._double_directory(dirfile)
                self._split_bucket(head_bucket, index_record, dirfile, bucketfile)

    def _insert_in_bucket(self, bucket, packed_record, bucketfile):
        insert_position = None

        # Reutilizar tombstones si existen
        if bucket.num_slots > bucket.num_records:
            tombstone = b'\x00' * self.index_record_size
            for i in range(bucket.num_slots):
                slot_pos = bucket.bucket_pos + Bucket.HEADER_SIZE + (i * self.index_record_size)
                bucketfile.seek(slot_pos)
                if bucketfile.read(self.index_record_size) == tombstone:
                    self.performance.track_read()
                    insert_position = slot_pos
                    break
                self.performance.track_read()

        if insert_position is None:
            if bucket.num_slots < BLOCK_FACTOR:
                insert_position = bucket.bucket_pos + Bucket.HEADER_SIZE + (bucket.num_slots * self.index_record_size)
                bucket.num_slots += 1
            else:
                return False

        bucketfile.seek(insert_position)
        bucketfile.write(packed_record)
        self.performance.track_write()

        bucket.num_records += 1
        bucketfile.seek(bucket.bucket_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, bucket.local_depth, bucket.num_slots,
                                     bucket.num_records, bucket.next_overflow_bucket))
        self.performance.track_write()
        return True

    def _delete_from_bucket_chain(self, bucket, secondary_value, primary_key, bucketfile):
        current_bucket = bucket
        tombstone = b'\x00' * self.index_record_size
        deleted_pks = []

        if isinstance(secondary_value, bytes):
            search_value = secondary_value.decode('utf-8').strip('\x00').strip()
        else:
            search_value = str(secondary_value).strip()

        while current_bucket is not None:
            for i in range(current_bucket.num_slots):
                record_pos = current_bucket.bucket_pos + Bucket.HEADER_SIZE + (i * self.index_record_size)
                bucketfile.seek(record_pos)
                packed_record = bucketfile.read(self.index_record_size)
                self.performance.track_read()

                if packed_record == tombstone:
                    continue

                try:
                    unpacked = IndexRecord.unpack(packed_record, self.index_record_template.value_type_size, "index_value")
                    stored_key = unpacked.index_value
                    if isinstance(stored_key, bytes):
                        stored_key = stored_key.decode('utf-8').strip('\x00').strip()
                    else:
                        stored_key = str(stored_key).strip()

                    should_delete = False
                    if stored_key == search_value:
                        if primary_key is None:
                            should_delete = True
                        elif unpacked.primary_key == primary_key:
                            should_delete = True

                    if should_delete:
                        bucketfile.seek(record_pos)
                        bucketfile.write(tombstone)
                        self.performance.track_write()

                        current_bucket.num_records -= 1
                        deleted_pks.append(unpacked.primary_key)

                        bucketfile.seek(current_bucket.bucket_pos)
                        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, current_bucket.local_depth,
                                                     current_bucket.num_slots, current_bucket.num_records,
                                                     current_bucket.next_overflow_bucket))
                        self.performance.track_write()

                        if primary_key is not None:
                            return deleted_pks

                except struct.error:
                    continue

            if current_bucket.next_overflow_bucket != -1:
                bucketfile.seek(current_bucket.next_overflow_bucket)
                ld, num_slots, num_records, next_overflow = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
                self.performance.track_read()
                current_bucket = Bucket(current_bucket.next_overflow_bucket, self.index_record_size, ld, num_slots, num_records, next_overflow)
            else:
                break

        return deleted_pks

    def _handle_empty_bucket(self, empty_bucket, dirfile, bucketfile):
        self._redirect_directory_entries(empty_bucket.bucket_pos, dirfile)
        self.free_bucket(empty_bucket.bucket_pos, bucketfile)

    def _redirect_directory_entries(self, empty_bucket_pos, dirfile):
        dir_size = 2 ** self.global_depth
        replacement_pos = None

        for i in range(dir_size):
            dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
            pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
            self.performance.track_read()

            if pos != empty_bucket_pos:
                replacement_pos = pos
                break

        if replacement_pos:
            for i in range(dir_size):
                dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
                self.performance.track_read()

                if pos == empty_bucket_pos:
                    dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    dirfile.write(struct.pack(self.DIR_FORMAT, replacement_pos))
                    self.performance.track_write()

    def free_bucket(self, bucket_pos, bucketfile):
        bucketfile.seek(bucket_pos)
        next_free = self.first_free_bucket_pos
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, 0, 0, 0, next_free))
        self.performance.track_write()

        self.first_free_bucket_pos = bucket_pos
        self._write_header()

    def _append_new_bucket(self, local_depth, bucketfile):
        # Reutilizar bucket de free list o crear nuevo
        if self.first_free_bucket_pos == -1:
            bucketfile.seek(0, 2)
            new_pos = bucketfile.tell()
        else:
            new_pos = self.first_free_bucket_pos
            bucketfile.seek(new_pos)
            _, _, _, self.first_free_bucket_pos = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
            self.performance.track_read()
            self._write_header()

        bucketfile.seek(new_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, local_depth, 0, 0, -1))
        self.performance.track_write()

        tombstone = b'\x00' * self.index_record_size
        bucketfile.write(tombstone * BLOCK_FACTOR)
        self.performance.track_write()

        return new_pos

    def _split_bucket(self, head_bucket, new_index_record, dirfile, bucketfile):
        if head_bucket.local_depth == self.global_depth:
            self._double_directory(dirfile)

        all_records_packed = self._get_all_records_from_bucket(head_bucket, bucketfile)
        all_records_packed.append(new_index_record.pack())

        # Liberar buckets de overflow
        next_pos = head_bucket.next_overflow_bucket
        while next_pos != -1:
            bucketfile.seek(next_pos)
            _, _, _, next_in_chain = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
            self.performance.track_read()
            self.free_bucket(next_pos, bucketfile)
            next_pos = next_in_chain

        # Reiniciar bucket principal
        head_bucket.local_depth += 1
        head_bucket.num_slots = 0
        head_bucket.num_records = 0
        head_bucket.next_overflow_bucket = -1
        bucketfile.seek(head_bucket.bucket_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, head_bucket.local_depth, 0, 0, -1))
        self.performance.track_write()

        # Crear nuevo bucket y redistribuir
        new_bucket_pos = self._append_new_bucket(local_depth=head_bucket.local_depth, bucketfile=bucketfile)
        self._update_directory_pointers(head_bucket.bucket_pos, new_bucket_pos, head_bucket.local_depth, dirfile)

        for packed_rec in all_records_packed:
            unpacked = IndexRecord.unpack(packed_rec, self.index_record_template.value_type_size, "index_value")
            secondary_value = unpacked.index_value
            if isinstance(secondary_value, bytes):
                secondary_value = secondary_value.decode('utf-8').strip('\x00').strip()
            self._insert_index_record(unpacked, secondary_value, dirfile, bucketfile)

    def _get_all_records_from_bucket(self, bucket, bucketfile):
        all_records = []
        current_bucket = bucket
        tombstone = b'\x00' * self.index_record_size

        while current_bucket is not None:
            if current_bucket.num_slots > 0:
                bucketfile.seek(current_bucket.bucket_pos + Bucket.HEADER_SIZE)
                for _ in range(current_bucket.num_slots):
                    packed_record = bucketfile.read(self.index_record_size)
                    if packed_record != tombstone:
                        all_records.append(packed_record)
                self.performance.track_read()

            if current_bucket.next_overflow_bucket != -1:
                bucketfile.seek(current_bucket.next_overflow_bucket)
                ld, num_slots, num_records, next_overflow = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
                self.performance.track_read()
                current_bucket = Bucket(current_bucket.next_overflow_bucket, self.index_record_size, ld, num_slots, num_records, next_overflow)
            else:
                current_bucket = None

        return all_records

    def _double_directory(self, dirfile):
        dirfile.seek(self.HEADER_SIZE)
        dir_size = 2 ** self.global_depth
        buf = dirfile.read(dir_size * self.DIR_SIZE)
        self.performance.track_read()

        current_dir = [entry[0] for entry in struct.iter_unpack(self.DIR_FORMAT, buf)]

        self.global_depth += 1
        self._write_header()

        dirfile.seek(self.HEADER_SIZE)
        for entry in current_dir:
            dirfile.write(struct.pack(self.DIR_FORMAT, entry))
            dirfile.write(struct.pack(self.DIR_FORMAT, entry))
        self.performance.track_write()

    def _update_directory_pointers(self, old_bucket_pos, new_bucket_pos, new_local_depth, dirfile):
        dir_size = 2 ** self.global_depth
        for i in range(dir_size):
            dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
            current_ptr = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
            self.performance.track_read()

            if current_ptr == old_bucket_pos:
                bit_position = new_local_depth - 1
                if (i >> bit_position) & 1:
                    dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    dirfile.write(struct.pack(self.DIR_FORMAT, new_bucket_pos))
                    self.performance.track_write()

    def _initialize_files(self, initial_depth=3):
        self.global_depth = initial_depth
        self.first_free_bucket_pos = -1

        with open(self.dirname, 'w+b') as dirfile:
            dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, self.first_free_bucket_pos))

            with open(self.bucketname, 'w+b') as bucketfile:
                bucket0_pos = self._append_new_bucket_init(bucketfile, 1)
                bucket1_pos = self._append_new_bucket_init(bucketfile, 1)

            dirfile.seek(self.HEADER_SIZE)
            for i in range(2 ** self.global_depth):
                bucket_pos = bucket0_pos if (i % 2 == 0) else bucket1_pos
                dirfile.write(struct.pack(self.DIR_FORMAT, bucket_pos))

    def _append_new_bucket_init(self, bucketfile, local_depth):
        bucketfile.seek(0, 2)
        new_pos = bucketfile.tell()
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, local_depth, 0, 0, -1))
        tombstone = b'\x00' * self.index_record_size
        bucketfile.write(tombstone * BLOCK_FACTOR)
        return new_pos

    def drop_index(self):
        removed_files = []
        for file_path in [self.dirname, self.bucketname]:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    removed_files.append(file_path)
                except OSError:
                    pass
        return removed_files
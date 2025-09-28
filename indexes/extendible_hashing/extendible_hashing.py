# Extendible Hashing Index with Overflow Chaining (Corrected)
import struct
import os
import hashlib
from ..core.record import Record, IndexRecord

# --- Configuration ---
BLOCK_FACTOR = 4  # Max number of IndexRecords per bucket
MAX_OVERFLOW = 2  # Max number of overflow buckets before a split is forced


class Bucket:
    HEADER_FORMAT = "iii"  # local_depth, size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, bucket_pos, index_record_size, local_depth=1, size=0, next_bucket=-1):
        self.bucket_pos = bucket_pos
        self.local_depth = local_depth
        self.size = size
        self.next_bucket = next_bucket
        self.index_record_size = index_record_size

    def get_all_records_from_chain(self, bucketfile):
        """Reads and returns all records from this bucket AND its entire overflow chain."""
        all_records = []
        current_bucket = self

        while current_bucket is not None:
            if current_bucket.size > 0:
                bucketfile.seek(current_bucket.bucket_pos + self.HEADER_SIZE)
                for _ in range(current_bucket.size):
                    packed_record = bucketfile.read(self.index_record_size)
                    if packed_record != (b'\x00' * self.index_record_size):
                        all_records.append(packed_record)

            # Move to the next bucket in the chain
            if current_bucket.next_bucket != -1:
                bucketfile.seek(current_bucket.next_bucket)
                ld, s, nb = struct.unpack(self.HEADER_FORMAT, bucketfile.read(self.HEADER_SIZE))
                current_bucket = Bucket(current_bucket.next_bucket, self.index_record_size, ld, s, nb)
            else:
                current_bucket = None

        return all_records

    def insert(self, packed_record, bucketfile):
        """Inserts a packed record into this specific bucket."""
        # Ensure packed_record is bytes
        if hasattr(packed_record, 'pack'):
            packed_record = packed_record.pack()

        tombstone = b'\x00' * self.index_record_size
        insert_position = None

        # Check existing slots for tombstones to reuse
        for i in range(self.size):
            slot_pos = self.bucket_pos + self.HEADER_SIZE + (i * self.index_record_size)
            bucketfile.seek(slot_pos)
            existing_data = bucketfile.read(self.index_record_size)

            if existing_data == tombstone:
                insert_position = slot_pos
                break

        # If no tombstone slot found and we have space, add at the end
        if insert_position is None:
            if self.size < BLOCK_FACTOR:
                insert_position = self.bucket_pos + self.HEADER_SIZE + (self.size * self.index_record_size)
                self.size += 1
            else:
                # Bucket is full and no tombstones available
                return False

        # Write the packed record to the determined position
        bucketfile.seek(insert_position)
        bucketfile.write(packed_record)

        # Update the bucket's header (FIXED: include all 3 values)
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(self.HEADER_FORMAT, self.local_depth, self.size, self.next_bucket))
        return True

    def delete(self, secondary_key, bucketfile, index_record_template):
        """Deletes a record from this bucket or any bucket in its overflow chain."""
        current_bucket = self
        tombstone = b'\x00' * self.index_record_size
        deleted_count = 0

        while current_bucket is not None:
            for i in range(current_bucket.size):
                record_pos = current_bucket.bucket_pos + self.HEADER_SIZE + (i * self.index_record_size)
                bucketfile.seek(record_pos)
                packed_record = bucketfile.read(self.index_record_size)

                if packed_record == tombstone:
                    continue

                try:
                    unpacked = IndexRecord.unpack(packed_record, index_record_template.value_type_size, "index_value")
                    stored_key = unpacked.index_value
                    if isinstance(stored_key, bytes):
                        stored_key = stored_key.decode('utf-8').strip('\x00')

                    if stored_key == secondary_key:
                        bucketfile.seek(record_pos)
                        bucketfile.write(tombstone)
                        deleted_count += 1
                except struct.error:
                    continue

            # Move to the next bucket in the chain
            if current_bucket.next_bucket != -1:
                bucketfile.seek(current_bucket.next_bucket)
                ld, s, nb = struct.unpack(self.HEADER_FORMAT, bucketfile.read(self.HEADER_SIZE))
                current_bucket = Bucket(current_bucket.next_bucket, self.index_record_size, ld, s, nb)
            else:
                return deleted_count

        return deleted_count


class ExtendableHashing:
    HEADER_FORMAT = "iii"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, data_filename, index_field_name, index_field_type, index_field_size):
        self.dirname = f"{data_filename}_{index_field_name}.dir"
        self.bucketname = f"{data_filename}_{index_field_name}.bkt"
        self.index_record_template = IndexRecord(index_field_type, index_field_size)
        self.index_record_size = self.index_record_template.RECORD_SIZE

        create_new = not os.path.exists(self.dirname) or not os.path.exists(self.bucketname)
        self.dirfile = open(self.dirname, 'r+b' if not create_new else 'w+b')
        self.bucketfile = open(self.bucketname, 'r+b' if not create_new else 'w+b')

        if create_new:
            self._initialize_files()
        else:
            self.global_depth, _ = self._read_header()

    def _hash_key(self, key):
        """Deterministic hash function that returns non-negative values."""
        if isinstance(key, str):
            key = key.encode('utf-8')
        elif not isinstance(key, bytes):
            key = str(key).encode('utf-8')
        return int(hashlib.md5(key).hexdigest(), 16)

    def _get_bucket_from_key(self, key):
        """Hashes a key to find and return the HEAD Bucket object of a chain."""
        hash_val = self._hash_key(key)
        dir_index = hash_val % (2 ** self.global_depth)

        self.dirfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]

        self.bucketfile.seek(bucket_pos)
        ld, s, nb = struct.unpack(Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE))
        return Bucket(bucket_pos, self.index_record_size, ld, s, nb)

    def _append_new_bucket(self, local_depth):
        """Adds a new, empty bucket to the end of the bucket file."""
        self.bucketfile.seek(0, 2)  # Seek to end
        new_pos = self.bucketfile.tell()
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, local_depth, 0, -1))
        return new_pos

    def search(self, key):
        """Finds all primary keys by searching the bucket and its entire overflow chain."""
        bucket = self._get_bucket_from_key(key)
        packed_records = bucket.get_all_records_from_chain(self.bucketfile)

        matching_pks = []
        for packed_rec in packed_records:
            try:
                unpacked = IndexRecord.unpack(packed_rec, self.index_record_template.value_type_size, "index_value")
                value = unpacked.index_value
                if isinstance(value, bytes):
                    value = value.decode('utf-8').strip('\x00')
                if value == key:
                    matching_pks.append(unpacked.primary_key)
            except struct.error:
                continue
        return matching_pks

    def insert(self, index_record: IndexRecord):
        """Inserts an IndexRecord, handles overflow by chaining, and splits only when necessary."""
        # Extract the secondary key from the IndexRecord
        secondary_key = index_record.index_value
        if isinstance(secondary_key, bytes):
            secondary_key = secondary_key.decode('utf-8').strip('\x00')
        packed_record = index_record.pack()
        head_bucket = self._get_bucket_from_key(secondary_key)
        current_bucket = head_bucket
        overflow_count = 0

        # Traverse the chain to find a spot
        while True:
            if current_bucket.insert(packed_record, self.bucketfile):
                return True  # Successfully inserted

            # If insert failed and there's another bucket, move to it
            if current_bucket.next_bucket != -1:
                overflow_count += 1
                self.bucketfile.seek(current_bucket.next_bucket)
                ld, s, nb = struct.unpack(Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE))
                current_bucket = Bucket(current_bucket.next_bucket, self.index_record_size, ld, s, nb)
            else:
                break  # Reached the end of the chain

        # If we are here, the entire chain is full.
        if overflow_count < MAX_OVERFLOW:
            # Create new overflow bucket
            new_bucket_pos = self._append_new_bucket(head_bucket.local_depth)

            # Link the last bucket in the chain to the new one
            current_bucket.next_bucket = new_bucket_pos
            self.bucketfile.seek(current_bucket.bucket_pos)
            self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, current_bucket.local_depth,
                                              current_bucket.size, current_bucket.next_bucket))

            # Insert the record into the new bucket
            new_bucket = Bucket(new_bucket_pos, self.index_record_size, head_bucket.local_depth)
            return new_bucket.insert(packed_record, self.bucketfile)
        else:
            # We've exceeded max overflows, so we must split
            return self._split_bucket(head_bucket, index_record)

    def delete(self, secondary_key):
        """Deletes a specific record by searching the entire bucket chain."""
        bucket = self._get_bucket_from_key(secondary_key)
        return bucket.delete(secondary_key, self.bucketfile, self.index_record_template)

    def _split_bucket(self, head_bucket: Bucket, new_index_record: IndexRecord):
        """Splits a bucket and its entire overflow chain."""
        if head_bucket.local_depth == self.global_depth:
            self._double_directory()

        # Get all records from the ORIGINAL bucket and its ENTIRE overflow chain
        all_records_packed = head_bucket.get_all_records_from_chain(self.bucketfile)
        all_records_packed.append(new_index_record.pack())

        # Update old bucket: increment depth, reset size and next pointer
        head_bucket.local_depth += 1
        self.bucketfile.seek(head_bucket.bucket_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, head_bucket.local_depth, 0, -1))

        # Create the new sibling bucket
        new_bucket_pos = self._append_new_bucket(local_depth=head_bucket.local_depth)

        # Update directory pointers
        self._update_directory_pointers(head_bucket.bucket_pos, new_bucket_pos, head_bucket.local_depth)

        # Re-distribute ALL records from the old chain + the new record
        # Now we work consistently with IndexRecord objects throughout
        for packed_rec in all_records_packed:
            try:
                unpacked = IndexRecord.unpack(packed_rec, self.index_record_template.value_type_size, "index_value")

                # Recursive call with IndexRecord - clean and consistent!
                self.insert(unpacked)

            except struct.error:
                continue

        return True

    def _read_header(self):
        self.dirfile.seek(0)
        return struct.unpack(self.HEADER_FORMAT, self.dirfile.read(self.HEADER_SIZE))

    def _write_header(self):
        self.dirfile.seek(0)
        dir_size = 2 ** self.global_depth
        self.dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, dir_size))

    def _initialize_files(self, initial_depth=3):
        """Sets up the initial directory and two empty buckets."""
        self.global_depth = initial_depth
        self._write_header()

        bucket0_pos = self._append_new_bucket(local_depth=1)
        bucket1_pos = self._append_new_bucket(local_depth=1)

        self.dirfile.seek(self.HEADER_SIZE)
        for i in range(2 ** self.global_depth):
            bucket_pos = bucket0_pos if (i % 2 == 0) else bucket1_pos
            self.dirfile.write(struct.pack(self.DIR_FORMAT, bucket_pos))

    def _double_directory(self):
        """Doubles the size of the directory file."""
        # Read header to know how many entries exist
        self.dirfile.seek(0)
        global_depth, dir_size = struct.unpack(self.HEADER_FORMAT, self.dirfile.read(self.HEADER_SIZE))

        # Read exactly dir_size entries
        self.dirfile.seek(self.HEADER_SIZE)
        buf = self.dirfile.read(dir_size * self.DIR_SIZE)
        # Unpack into list of ints (bucket positions)
        current_dir = [entry[0] for entry in struct.iter_unpack(self.DIR_FORMAT, buf)]

        self.global_depth += 1
        self._write_header()

        self.dirfile.seek(self.HEADER_SIZE)
        # Duplicate all existing entries
        for entry in current_dir:
            self.dirfile.write(struct.pack(self.DIR_FORMAT, entry[0]))
            self.dirfile.write(struct.pack(self.DIR_FORMAT, entry[0]))

    def _update_directory_pointers(self, old_bucket_pos, new_bucket_pos, new_local_depth):
        """Updates directory pointers after a bucket split."""
        dir_size = 2 ** self.global_depth
        stride = 2 ** new_local_depth
        split_pattern = 2 ** (new_local_depth - 1)

        for i in range(dir_size):
            self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
            current_ptr = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]

            if current_ptr == old_bucket_pos:
                if (i % stride) >= split_pattern:
                    # This entry should now point to the new bucket
                    self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    self.dirfile.write(struct.pack(self.DIR_FORMAT, new_bucket_pos))

    def close(self):
        """Closes all file handlers."""
        self.dirfile.close()
        self.bucketfile.close()
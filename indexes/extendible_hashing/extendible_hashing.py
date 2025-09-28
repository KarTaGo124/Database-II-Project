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
        """ Inserts a new IndexRecord into the bucket, reusing tombstone slots if available. """
        packed_record = index_record.pack()
        tombstone = b'\x00' * self.index_record_size

        # Find the first available slot (either tombstone or at the end)
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
            elif insert_position is None:
            # Bucket is full and no tombstones available
                return False

        # Write the packed record to the determined position
        bucketfile.seek(insert_position)
        bucketfile.write(packed_record)

        # Update the bucket's header
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(self.HEADER_FORMAT, self.local_depth, self.size))
        return True

    def delete_record(self, secondary_key, primary_key, bucketfile, index_record_template):
        """ Deletes a specific IndexRecord from the bucket by marking it as tombstone. """
        if self.size == 0:
            return False

        tombstone = b'\x00' * self.index_record_size
        found = False

        for i in range(self.size):
            record_pos = self.bucket_pos + self.HEADER_SIZE + (i * self.index_record_size)
            bucketfile.seek(record_pos)
            packed_record = bucketfile.read(self.index_record_size)

            # skip deleted records
            if packed_record == tombstone:
                continue

            try:
                # Unpack and check if this is the record to delete
                unpacked = IndexRecord.unpack(packed_record, index_record_template.value_type_size, "index_value")

                stored_secondary_key = unpacked.index_value
                if isinstance(stored_secondary_key, bytes):
                    stored_secondary_key = stored_secondary_key.decode('utf-8').strip('\x00')

                # Check if both secondary key and primary key match
                if stored_secondary_key == secondary_key and unpacked.primary_key == primary_key:
                    # Mark as tombstone
                    bucketfile.seek(record_pos)
                    bucketfile.write(tombstone)
                    found = True
                    break

            except struct.error:
                continue

        return found

    def clear(self, bucketfile):
        """ Resets the bucket's size"""
        self.size = 0
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(self.HEADER_FORMAT, self.local_depth, 0))


class ExtendableHashing:
    """ Manages the directory and buckets for the extendible hashing index. """
    HEADER_FORMAT = "ii"  # global_depth, directory_size
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"  # pointer to a bucket
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, data_filename, index_field_name, index_field_type, index_field_size):
        # File and index configuration
        self.dirname = f"{data_filename}_{index_field_name}.dir"
        self.bucketname = f"{data_filename}_{index_field_name}.bkt"
        self.index_record_template = IndexRecord(index_field_type, index_field_size)
        self.index_record_size = self.index_record_template.RECORD_SIZE

        # Open files or create them if they don't exist
        create_new = not os.path.exists(self.dirname) or not os.path.exists(self.bucketname)
        self.dirfile = open(self.dirname, 'r+b' if not create_new else 'w+b')
        self.bucketfile = open(self.bucketname, 'r+b' if not create_new else 'w+b')

        if create_new:
            self._initialize_files()
        else:
            self.global_depth, _ = self._read_header()

    def _read_header(self):
        self.dirfile.seek(0)
        return struct.unpack(self.HEADER_FORMAT, self.dirfile.read(self.HEADER_SIZE))

    def _write_header(self):
        self.dirfile.seek(0)
        dir_size = 2 ** self.global_depth
        self.dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, dir_size))

    def _initialize_files(self, initial_depth=3):
        """ Sets up the initial directory and two empty buckets. """
        self.global_depth = initial_depth
        self._write_header()

        # Create first two buckets
        bucket0_pos = self._append_new_bucket(local_depth=1)
        bucket1_pos = self._append_new_bucket(local_depth=1)

        # Point directory entries to the new buckets
        self.dirfile.seek(self.HEADER_SIZE)
        for i in range(2 ** self.global_depth):
            bucket_pos = bucket0_pos if (i % 2 == 0) else bucket1_pos
            self.dirfile.write(struct.pack(self.DIR_FORMAT, bucket_pos))

    def _get_bucket_from_key(self, key):
        """ Hashes a key to find and return the corresponding Bucket object. """
        hash_val = hash(key)
        dir_index = hash_val % 2 ** self.global_depth

        # Find the bucket's position from the directory
        self.dirfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]

        # Read the bucket's header and return a Bucket object
        self.bucketfile.seek(bucket_pos)
        local_depth, size = struct.unpack(Bucket.HEADER_FORMAT, self.bucketfile.read(Bucket.HEADER_SIZE))
        return Bucket(bucket_pos, self.index_record_size, local_depth, size)

    def _append_new_bucket(self, local_depth):
        """ Adds a new, empty bucket to the end of the bucket file. """
        self.bucketfile.seek(0, 2)
        new_pos = self.bucketfile.tell()

        new_bucket = Bucket(new_pos, self.index_record_size, local_depth)
        # Write header for the new bucket
        self.bucketfile.seek(new_bucket.bucket_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, new_bucket.local_depth, 0))
        return new_pos

    def search(self, key):
        """ Finds all primary keys associated with a secondary key. """
        bucket = self._get_bucket_from_key(key)
        packed_records = bucket.get_all_records(self.bucketfile)

        matching_pks = []
        for packed_rec in packed_records:
            try:
                # Unpack the record using the class template
                unpacked = IndexRecord.unpack(packed_rec, self.index_record_template.value_type_size, "index_value")

                value = unpacked.index_value
                if isinstance(value, bytes):  # Decode if it's a CHAR/string type
                    value = value.decode('utf-8').strip('\x00')

                if value == key:
                    matching_pks.append(unpacked.primary_key)
            except struct.error:
                continue  # Skip malformed or empty (tombstone) records

        return matching_pks

    def insert(self, data_record: Record):
        """ Inserts a new index entry based on a full data record. """
        secondary_key = getattr(data_record, self.index_record_template.key_field)
        primary_key = data_record.get_key()

        # Create the specific IndexRecord for this entry
        index_entry = IndexRecord(self.index_record_template.value_type_size[0][1],
                                  self.index_record_template.value_type_size[0][2])
        index_entry.set_index_data(index_value=secondary_key, primary_key=primary_key)

        bucket = self._get_bucket_from_key(secondary_key)

        # Try to insert in the bucket (it will reuse tombstone slots or add at end if space)
        # we dont check size bc size no longer size, it just tracks last position to insert if space
        if bucket.insert(index_entry, self.bucketfile):
            return ;

        # If insertion failed, bucket is truly full (no tombstones and size == BLOCK_FACTOR)
        self._split_bucket(bucket, index_entry)


    def delete(self, secondary_key):
        """
        Deletes index entries based on secondary key """
        bucket = self._get_bucket_from_key(secondary_key)
        # Delete all records with matching secondary key
        deleted_count = 0

        # First, find all matching primary keys
        matching_pks = self.search(secondary_key)

        # Delete each matching record
        for pk in matching_pks:
            if bucket.delete_record(secondary_key, pk, self.bucketfile, self.index_record_template):
                deleted_count += 1

        return deleted_count

    def _split_bucket(self, bucket: Bucket, new_record: IndexRecord):
        """ Handles the logic of splitting a full bucket. """
        # First, check if we need to double the directory
        if bucket.local_depth == self.global_depth:
            self._double_directory()

        # Get all records from the old bucket, plus the new one
        all_records_packed = bucket.get_all_records(self.bucketfile)
        all_records_packed.append(new_record.pack())

        # Increment local depth of the old bucket and clear it
        bucket.local_depth += 1
        bucket.clear(self.bucketfile)
        self.bucketfile.seek(bucket.bucket_pos)
        self.bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, bucket.local_depth, 0, -1))

        # Create the new sibling bucket
        new_bucket_pos = self._append_new_bucket(local_depth=bucket.local_depth)

        # Update directory pointers to the new bucket
        self._update_directory_pointers(bucket.bucket_pos, new_bucket_pos, bucket.local_depth)

        # Re-distribute all records
        for packed_rec in all_records_packed:
            unpacked = IndexRecord.unpack(packed_rec, self.index_record_template.value_type_size, "index_value")
            key = unpacked.index_value
            if isinstance(key, bytes):
                key = key.decode('utf-8').strip('\x00')

            # Re-insert the unpacked record, which will now go to the correct bucket
            self.insert(unpacked)  # Recursive call to insert handles finding the right bucket now

    def _double_directory(self):
        """ Doubles the size of the directory file. """
        self.dirfile.seek(self.HEADER_SIZE)
        current_dir = list(struct.iter_unpack(self.DIR_FORMAT, self.dirfile.read()))

        self.global_depth += 1
        self._write_header()

        self.dirfile.seek(self.HEADER_SIZE)
        # Duplicate all existing entries
        for entry in current_dir:
            self.dirfile.write(struct.pack(self.DIR_FORMAT, entry[0]))
            self.dirfile.write(struct.pack(self.DIR_FORMAT, entry[0]))

    def _update_directory_pointers(self, old_bucket_pos, new_bucket_pos, local_depth):
        """ Updates directory entries after a split to point to the new bucket. """
        dir_size = 2 ** self.global_depth
        for i in range(dir_size):
            # Use the local depth to find which entries to update
            # This hash check mirrors the split condition
            if (i >> (local_depth - 1)) & 1:
                self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                current_ptr = struct.unpack(self.DIR_FORMAT, self.dirfile.read(self.DIR_SIZE))[0]
                # Only update pointers that still point to the *old* bucket
                if current_ptr == old_bucket_pos:
                    self.dirfile.seek(self.HEADER_SIZE + i * self.DIR_SIZE)
                    self.dirfile.write(struct.pack(self.DIR_FORMAT, new_bucket_pos))

    def close(self):
        """ Closes all file handlers. """
        self.dirfile.close()
        self.bucketfile.close()
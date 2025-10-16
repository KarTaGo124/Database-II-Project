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
    def __init__(self, bucket_pos, index_record_size, local_depth=1, num_slots=0, num_records=0, next_overflow_bucket=-1,  performance=None, bucketfile=None, index_record_template=None):
        self.bucket_pos = bucket_pos
        self.local_depth = local_depth
        self.num_slots = num_slots
        self.num_records = num_records
        self.next_overflow_bucket = next_overflow_bucket
        self.index_record_template = index_record_template
        self.index_record_size = index_record_size
        self.elements = [] #memory loaded elements
        self.elements_pos = [] #pos of elements in bucket
        self.performance = performance

        current_bucket = self
        tombstone = b'\x00' * self.index_record_size
        if current_bucket.num_records > 0:  # if there are records, search them in all slots. slots != records
            bucketfile.seek(current_bucket.bucket_pos + Bucket.HEADER_SIZE)
            for i in range(current_bucket.num_slots):
                packed_record = bucketfile.read(self.index_record_size)
                if packed_record != tombstone:
                    self.elements.append(IndexRecord.unpack(packed_record,self.index_record_template.value_type_size, "index_value"))
                    self.elements_pos.append(current_bucket.bucket_pos + Bucket.HEADER_SIZE + (i * self.index_record_size))
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
                                    num_records, next_overflow,self.performance,bucketfile,self.index_record_template)
        return None

    def add_overflow(self, overflow_bucket_pos, bucketfile):
        self.next_overflow_bucket = overflow_bucket_pos
        bucketfile.seek(self.bucket_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, self.local_depth,
                                     self.num_slots, self.num_records,
                                     self.next_overflow_bucket))
        self.performance.track_write()
    def search(self, secondary_value, pk = None):
        self.performance.start_operation()
        matching_pks = []
        for e in self.elements:
            value = e.index_value
            if isinstance(value, bytes):
                value = value.decode('utf-8').strip('\x00').strip()
            else:
                value = str(value).strip()
            if value == secondary_value:
                matching_pks.append(e.primary_key)
        return matching_pks

    def insert(self, index_record: IndexRecord,bucketfile):
        self.performance.start_operation()
        if self.is_full():
            return self.performance.end_operation(False)
        else:
            insert_position = None
            #reutilizar tombstones disponibles para insertar
            if self.num_slots > self.num_records:
                tombstone = b'\x00' * self.index_record_size
                for i in range(self.num_slots):
                    slot_pos = self.bucket_pos + Bucket.HEADER_SIZE + (i * self.index_record_size)
                    bucketfile.seek(slot_pos)
                    if bucketfile.read(self.index_record_size) == tombstone:
                        self.performance.track_read()
                        insert_position = slot_pos
                        break
                    self.performance.track_read()
            #aniadir al siguiente slot del ultimo si no hay disponible antes,
            #si hay espacio todaviaa
            if insert_position is None:
                if self.num_records < BLOCK_FACTOR:
                    insert_position = self.bucket_pos + Bucket.HEADER_SIZE + (self.num_records * self.index_record_size)
                    self.num_slots += 1
                else:
                    return False

            bucketfile.seek(insert_position)
            bucketfile.write(index_record.pack())
            self.elements.append(index_record)
            self.elements_pos.append(insert_position) #saved pos in bucket
            self.performance.track_write()

            self.num_records += 1
            bucketfile.seek(self.bucket_pos)
            bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, self.local_depth, self.num_slots,
                                         self.num_records, self.next_overflow_bucket))
            self.performance.track_write()
            return self.performance.end_operation(True)

    def delete(self, bucketfile, key,pk = None):
        tombstone = b'\x00' * self.index_record_size
        deleted_pks = []
        current_bucket = self
        i = 0
        while current_bucket is not None:
            for unpacked in current_bucket.elements:
                stored_key = unpacked.index_value
                should_delete = False
                if stored_key == key:
                    if pk is None:
                        should_delete = True
                    elif unpacked.primary_key == pk:
                        should_delete = True

                if should_delete:
                    bucketfile.seek(self.elements_pos[i])
                    bucketfile.write(tombstone)
                    self.performance.track_write()
                    current_bucket.num_records -= 1
                    deleted_pks.append(unpacked.primary_key)
                    bucketfile.seek(current_bucket.bucket_pos)
                    bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, current_bucket.local_depth,
                                                 current_bucket.num_slots, current_bucket.num_records,
                                                 current_bucket.next_overflow_bucket))
                    self.performance.track_write()

                if pk is not None:
                    return deleted_pks
                i += 1 #element

            if current_bucket.next_overflow_bucket != -1:
                current_bucket = current_bucket.get_next_overflow(bucketfile)
            else:
                break

        return deleted_pks


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
            matching_pk =  []
            while bucket is not None:
                matching_pk = matching_pk + bucket.search(secondary_value)
                bucket = bucket.get_next_overflow(bucketfile)
            return self.performance.end_operation(matching_pk)

    def insert(self, index_record: IndexRecord):
        self.performance.start_operation()

        secondary_value = index_record.index_value
        if secondary_value is None:
            return self.performance.end_operation(True)

        with open(self.dirname, 'r+b') as dirfile, open(self.bucketname, 'r+b') as bucketfile:
            self._insert_index_record(index_record, dirfile, bucketfile)
            return self.performance.end_operation(True)

    def delete(self, secondary_value, primary_key=None):
        self.performance.start_operation()

        with open(self.dirname, 'r+b') as dirfile, open(self.bucketname, 'r+b') as bucketfile:
            bucket = self._get_bucket_from_key(secondary_value, dirfile, bucketfile)
            old_num_records = bucket.num_records
            deleted_pks = bucket.delete(bucketfile,secondary_value, primary_key)

            # Liberar bucket si está completamente vacío
            if bucket.num_records == 0:
                # Verificar si el overflow contiene registros válidos
                next_bucket = bucket.get_next_overflow(bucketfile)
                if next_bucket is not None and next_bucket.num_records > 0:
                    self._overflow_to_main_bucket(bucket, dirfile, bucketfile)
                else:
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

        return Bucket(bucket_pos, self.index_record_size, local_depth, num_slots, num_records, next_overflow,self.performance,bucketfile,self.index_record_template)

    def _insert_index_record(self, index_record, dirfile, bucketfile):
        value = index_record.index_value
        if isinstance(value, bytes):
            value = value.decode('utf-8').strip('\x00').strip()
        else:
            value = str(value).strip()
        head_bucket = self._get_bucket_from_key(value, dirfile, bucketfile)
        current_bucket = head_bucket
        overflow_count = 0

        # Intentar insertar en cadena de buckets existente
        while True:
            if current_bucket.has_space():
                success = current_bucket.insert(index_record,bucketfile)
                if success:
                    return True

            if current_bucket.next_overflow_bucket != -1:
                overflow_count += 1
                current_bucket = current_bucket.get_next_overflow(bucketfile)
            else:
                break

        # Sin espacio - dividir bucket o crear overflow
        if head_bucket.local_depth < self.global_depth:
            return self._split_bucket(head_bucket, index_record, dirfile, bucketfile)
        else:
            if overflow_count < MAX_OVERFLOW:
                overflow_bucket_pos = self._append_new_bucket(head_bucket.local_depth, bucketfile)
                current_bucket.add_overflow(overflow_bucket_pos,bucketfile)
                current_bucket = current_bucket.get_next_overflow(bucketfile)
                current_bucket.insert(index_record, bucketfile)
                return True
            else:
                self._double_directory(dirfile)
                return self._split_bucket(head_bucket, index_record, dirfile, bucketfile)

    def _handle_empty_bucket(self, empty_bucket, dirfile, bucketfile):
        self._redirect_directory_entries(empty_bucket, dirfile,bucketfile)
        self.free_bucket(empty_bucket.bucket_pos, bucketfile)

    def _redirect_directory_entries(self, empty_bucket, dirfile, bucketfile):
        dir_size = 2 ** self.global_depth
        header_offset = self.HEADER_SIZE

        empty_index = None
        for i in range(dir_size):
            dirfile.seek(header_offset + i * self.DIR_SIZE)
            pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
            self.performance.track_read()
            if pos == empty_bucket.bucket_pos:
                empty_index = i
                break

        if empty_index is None:
            return  # no directory entry found — nothing to redirect

        mask = 1 << (empty_bucket.local_depth - 1)
        sibling_index = empty_index ^ mask

        dirfile.seek(header_offset + sibling_index * self.DIR_SIZE)
        sibling_pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
        self.performance.track_read()

        for i in range(dir_size):
            dirfile.seek(header_offset + i * self.DIR_SIZE)
            pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
            self.performance.track_read()

            if pos == empty_bucket.bucket_pos:
                dirfile.seek(header_offset + i * self.DIR_SIZE)
                dirfile.write(struct.pack(self.DIR_FORMAT, sibling_pos))
                self.performance.track_write()

        if sibling_pos != empty_bucket.bucket_pos:
            bucketfile.seek(sibling_pos)
            ld, num_slots, num_records, next_overflow = struct.unpack(
                Bucket.HEADER_FORMAT,
                bucketfile.read(Bucket.HEADER_SIZE)
            )
            self.performance.track_read()

            if ld == empty_bucket.local_depth:
                new_local_depth = ld - 1
                bucketfile.seek(sibling_pos)
                bucketfile.write(struct.pack(
                    Bucket.HEADER_FORMAT,
                    new_local_depth,
                    num_slots,
                    num_records,
                    next_overflow
                ))
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
        all_records_packed.append(new_index_record)

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
        tombstone = b'\x00' * self.index_record_size
        bucketfile.seek(head_bucket.bucket_pos + Bucket.HEADER_SIZE)
        bucketfile.write(tombstone * BLOCK_FACTOR)
        self.performance.track_write()

        # Crear nuevo bucket y redistribuir
        new_bucket_pos = self._append_new_bucket(local_depth=head_bucket.local_depth, bucketfile=bucketfile)
        self._update_directory_pointers(head_bucket.bucket_pos, new_bucket_pos, head_bucket.local_depth, dirfile)

        for index_record in all_records_packed:
            secondary_value = index_record.index_value
            self._insert_index_record(index_record, dirfile, bucketfile)

    def _get_all_records_from_bucket(self, bucket, bucketfile):
        all_records = []
        current_bucket = bucket
        while current_bucket is not None:
           all_records = all_records + current_bucket.elements
           current_bucket = current_bucket.get_next_overflow(bucketfile)

        return all_records

    def _double_directory(self, dirfile):
        # Leer el directorio actual completo
        dir_size = 2 ** self.global_depth
        dirfile.seek(self.HEADER_SIZE)
        buf = dirfile.read(dir_size * self.DIR_SIZE)
        self.performance.track_read()

        # Desempaquetar las entradas actuales
        current_dir = [entry[0] for entry in struct.iter_unpack(self.DIR_FORMAT, buf)]

        # Duplicar la profundidad global
        self.global_depth += 1
        self._write_header()  # actualiza encabezado con nueva global_depth

        # Crear una nueva lista duplicada 00,01   10 11
        new_dir = []
        for entry in current_dir:
            new_dir.append(entry)
            new_dir.append(entry)

        dirfile.seek(self.HEADER_SIZE)
        dirfile.write(struct.pack(self.DIR_FORMAT * len(new_dir), *new_dir))
        self.performance.track_write()

    def _update_directory_pointers(self, old_bucket_pos, new_bucket_pos, new_local_depth, dirfile):
        dir_size = 2 ** self.global_depth
        entry_size = self.DIR_SIZE
        header = self.HEADER_SIZE

        for i in range(dir_size):
            dirfile.seek(header + i * entry_size)
            current_ptr = struct.unpack(self.DIR_FORMAT, dirfile.read(entry_size))[0]
            self.performance.track_read()

            if current_ptr == old_bucket_pos:
                # Extraer el bit que corresponde al nuevo nivel
                bit = (i >> (new_local_depth - 1)) & 1
                if bit == 1:
                    dirfile.seek(header + i * entry_size)
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

    def _overflow_to_main_bucket(self, curr, dirfile, bucketfile):
        next_bucket = curr.get_next_overflow(bucketfile)
        while next_bucket.num_records > 0:
            for rec in next_bucket.elements:
                next_bucket.delete(bucketfile,rec.index_value)
                curr.insert(rec, bucketfile)
            curr = next_bucket
            if curr.next_overflow_bucket != -1:
                next_bucket = curr.get_next_overflow(bucketfile)
            else:
                break
        if curr.num_records == 0:
            self._handle_empty_bucket(curr, dirfile, bucketfile)





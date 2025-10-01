import os
import struct
from ..core.record import Record, IndexRecord
from ..core.performance_tracker import PerformanceTracker

BLOCK_FACTOR = 30
ROOT_INDEX_BLOCK_FACTOR = 50
LEAF_INDEX_BLOCK_FACTOR = 50
CONSOLIDATION_THRESHOLD = BLOCK_FACTOR // 3


class SecondaryPage:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, records=None, next_page=-1, block_factor=BLOCK_FACTOR, record_size=None):
        self.records = records if records else []
        self.next_page = next_page
        self.block_factor = block_factor
        self.record_size = record_size
        self.SIZE_OF_PAGE = self.HEADER_SIZE + self.block_factor * self.record_size if record_size else None

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_page)
        record_data = b''.join(r.pack() for r in self.records)
        record_data += b'\x00' * (self.record_size * (self.block_factor - len(self.records)))
        return header_data + record_data

    @staticmethod
    def unpack(data: bytes, block_factor=BLOCK_FACTOR, record_size=None, field_type=None, field_size=None):
        if len(data) < SecondaryPage.HEADER_SIZE:
            return SecondaryPage([], -1, block_factor, record_size)

        try:
            size, next_page = struct.unpack(SecondaryPage.HEADER_FORMAT, data[:SecondaryPage.HEADER_SIZE])
        except:
            return SecondaryPage([], -1, block_factor, record_size)

        offset = SecondaryPage.HEADER_SIZE
        records = []
        for i in range(size):
            if offset + record_size > len(data):
                break
            record_data = data[offset: offset + record_size]
            if len(record_data) < record_size:
                break
            try:
                # Create list_of_types for IndexRecord
                list_of_types = [
                    ("index_value", field_type, field_size),
                    ("primary_key", "INT", 4)
                ]
                record = IndexRecord.unpack(record_data, list_of_types, "index_value")
                records.append(record)
            except:
                break
            offset += record_size
        return SecondaryPage(records, next_page, block_factor, record_size)

    def insert_sorted(self, record: IndexRecord):
        left, right = 0, len(self.records)
        while left < right:
            mid = (left + right) // 2
            mid_key = self.records[mid].get_key()
            new_key = record.get_key()

            if isinstance(mid_key, bytes) and isinstance(new_key, str):
                mid_key = mid_key.decode('utf-8').rstrip('\x00')
            elif isinstance(mid_key, str) and isinstance(new_key, bytes):
                new_key = new_key.decode('utf-8').rstrip('\x00')

            if mid_key < new_key:
                left = mid + 1
            else:
                right = mid
        self.records.insert(left, record)

    def is_full(self):
        return len(self.records) >= self.block_factor

    def remove_record(self, key_value, primary_key=None):
        indices_to_remove = []
        for i, record in enumerate(self.records):
            if record.get_key() == key_value:
                if primary_key is None or record.primary_key == primary_key:
                    indices_to_remove.append(i)

        for idx in reversed(indices_to_remove):
            del self.records[idx]

        return len(indices_to_remove) > 0

    def is_empty(self):
        return len(self.records) == 0


class SecondaryFreeListStack:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    ENTRY_FORMAT = 'i'
    ENTRY_SIZE = struct.calcsize(ENTRY_FORMAT)
    STACK_SIZE = 100

    def __init__(self, filename):
        self.filename = filename
        self.SIZE_OF_FREE_LIST = self.HEADER_SIZE + self.STACK_SIZE * self.ENTRY_SIZE

    def push_free_page(self, page_number):
        if not os.path.exists(self.filename):
            with open(self.filename, "wb") as file:
                header = struct.pack(self.HEADER_FORMAT, 1)
                entry = struct.pack(self.ENTRY_FORMAT, page_number)
                padding = b'\x00' * (self.ENTRY_SIZE * (self.STACK_SIZE - 1))
                file.write(header + entry + padding)
            return

        with open(self.filename, "r+b") as file:
            count_data = file.read(self.HEADER_SIZE)
            count = struct.unpack(self.HEADER_FORMAT, count_data)[0]

            if count < self.STACK_SIZE:
                file.seek(self.HEADER_SIZE + count * self.ENTRY_SIZE)
                file.write(struct.pack(self.ENTRY_FORMAT, page_number))

                file.seek(0)
                file.write(struct.pack(self.HEADER_FORMAT, count + 1))

    def pop_free_page(self):
        if not os.path.exists(self.filename):
            return None

        with open(self.filename, "r+b") as file:
            count_data = file.read(self.HEADER_SIZE)
            count = struct.unpack(self.HEADER_FORMAT, count_data)[0]

            if count == 0:
                return None

            file.seek(self.HEADER_SIZE + (count - 1) * self.ENTRY_SIZE)
            page_data = file.read(self.ENTRY_SIZE)
            page_number = struct.unpack(self.ENTRY_FORMAT, page_data)[0]

            file.seek(0)
            file.write(struct.pack(self.HEADER_FORMAT, count - 1))

            return page_number

    def get_free_count(self):
        if not os.path.exists(self.filename):
            return 0

        with open(self.filename, "rb") as file:
            count_data = file.read(self.HEADER_SIZE)
            return struct.unpack(self.HEADER_FORMAT, count_data)[0]

    def is_empty(self):
        return self.get_free_count() == 0


class ISAMSecondaryBase:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DATA_START_OFFSET = HEADER_SIZE

    def __init__(self, field_name: str, primary_isam, filename=None):
        self.field_name = field_name
        self.primary_isam = primary_isam

        if filename is None:
            filename = f"{field_name}_secondary.dat"

        self.filename = filename

        # Get directory from main filename to put related files in same folder
        import os
        base_dir = os.path.dirname(filename)
        base_name = field_name

        self.root_index_filename = os.path.join(base_dir, f"{base_name}_root.dat")
        self.leaf_index_filename = os.path.join(base_dir, f"{base_name}_leaf.dat")
        self.free_list_filename = os.path.join(base_dir, f"{base_name}_free_list.dat")

        # Clean existing files to ensure fresh index
        self._clean_existing_files()

        self.block_factor = BLOCK_FACTOR
        self.root_index_block_factor = ROOT_INDEX_BLOCK_FACTOR
        self.leaf_index_block_factor = LEAF_INDEX_BLOCK_FACTOR

        self.free_list_stack = SecondaryFreeListStack(self.free_list_filename)
        self.performance = PerformanceTracker()

    def _clean_existing_files(self):
        files_to_clean = [
            self.filename,
            self.root_index_filename,
            self.leaf_index_filename,
            self.free_list_filename
        ]

        for file_path in files_to_clean:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except OSError:
                    pass

    # Operaciones principales

    def insert(self, record: Record):
        self.performance.start_operation()

        try:
            secondary_value = getattr(record, self.field_name)
            primary_key = record.get_key()

            index_record = self._create_index_record(secondary_value, primary_key)

            if not os.path.exists(self.filename):
                self._create_initial_files(index_record)
                return self.performance.end_operation(True)

            search_key = self._get_search_key(secondary_value)
            target_leaf_page_num = self._find_target_leaf_page(search_key, track_reads=True)
            target_data_page_num = self._find_target_data_page(search_key, target_leaf_page_num, track_reads=True)

            with open(self.filename, "r+b") as file:
                page = self._read_page(file, target_data_page_num)

                if not page.is_full():
                    page.insert_sorted(index_record)
                    self._write_page(file, target_data_page_num, page)
                else:
                    self._handle_page_overflow(file, target_data_page_num, page, index_record, target_leaf_page_num)

            return self.performance.end_operation(True)
        except Exception as e:
            return self.performance.end_operation(False)

    def search(self, secondary_value):
        self.performance.start_operation()

        primary_keys = []

        if not os.path.exists(self.filename):
            return self.performance.end_operation(primary_keys)

        search_key = self._get_search_key(secondary_value)
        target_leaf_page_num = self._find_target_leaf_page(search_key, track_reads=True)
        target_data_page_num = self._find_target_data_page(search_key, target_leaf_page_num, track_reads=True)

        current_page_num = target_data_page_num

        with open(self.filename, "rb") as file:
            while current_page_num is not None:
                try:
                    page = self._read_page(file, current_page_num)

                    for index_record in page.records:
                        if hasattr(index_record, 'index_value'):
                            if self._values_equal(index_record.index_value, secondary_value):
                                primary_keys.append(index_record.primary_key)
                            elif self._value_greater(index_record.index_value, secondary_value):
                                break

                    current_page_num = page.next_page if page.next_page != -1 else None
                except:
                    break

        return self.performance.end_operation(primary_keys)

    def range_search(self, start_value, end_value):
        self.performance.start_operation()
        primary_keys = []

        if not os.path.exists(self.filename):
            return self.performance.end_operation(primary_keys)

        search_key = self._get_search_key(start_value)
        target_leaf_page_num = self._find_target_leaf_page(search_key, track_reads=True)
        target_data_page_num = self._find_target_data_page(search_key, target_leaf_page_num, track_reads=True)

        current_page_num = target_data_page_num

        with open(self.filename, "rb") as file:
            while current_page_num is not None:
                page = self._read_page(file, current_page_num)

                for index_record in page.records:
                    if hasattr(index_record, 'index_value'):
                        if self._value_in_range(index_record.index_value, start_value, end_value):
                            primary_keys.append(index_record.primary_key)

                current_page_num = page.next_page if page.next_page != -1 else None

        return self.performance.end_operation(primary_keys)

    def delete(self, record: Record):
        self.performance.start_operation()
        secondary_value = getattr(record, self.field_name)
        primary_key = record.get_key()

        result = self._delete_record(secondary_value, primary_key)
        return self.performance.end_operation(result)

    # Operaciones intermedias

    def _delete_record(self, secondary_value, primary_key):
        if not os.path.exists(self.filename):
            return False

        search_key = self._get_search_key(secondary_value)
        target_leaf_page_num = self._find_target_leaf_page(search_key, track_reads=True)
        target_data_page_num = self._find_target_data_page(search_key, target_leaf_page_num, track_reads=True)

        current_page_num = target_data_page_num

        with open(self.filename, "r+b") as file:
            while current_page_num is not None:
                page = self._read_page(file, current_page_num)

                original_count = len(page.records)
                page.records = [r for r in page.records
                              if not (self._values_equal(r.index_value, secondary_value) and r.primary_key == primary_key)]

                if len(page.records) < original_count:
                    if page.is_empty() and current_page_num != 0:
                        self.free_list_stack.push_free_page(current_page_num)
                    else:
                        self._write_page(file, current_page_num, page)
                    return True

                current_page_num = page.next_page if page.next_page != -1 else None

        return False

    # Escritura y lectura de páginas e índices

    def _read_page(self, file, page_num):
        page_size = SecondaryPage.HEADER_SIZE + self.block_factor * self.index_record_template.RECORD_SIZE
        file.seek(self.DATA_START_OFFSET + page_num * page_size)
        self.performance.track_read()
        page_data = file.read(page_size)
        return SecondaryPage.unpack(page_data, self.block_factor, self.index_record_template.RECORD_SIZE, self.field_type, self.field_size)

    def _write_page(self, file, page_num, page):
        page_size = SecondaryPage.HEADER_SIZE + self.block_factor * self.index_record_template.RECORD_SIZE
        file.seek(self.DATA_START_OFFSET + page_num * page_size)
        self.performance.track_write()
        file.write(page.pack())

    def _handle_page_overflow(self, file, target_data_page_num, page, new_record, target_leaf_page_num):
        if page.next_page == -1:
            # Create new overflow page
            free_page_num = self.free_list_stack.pop_free_page()
            if free_page_num is not None:
                overflow_page_num = free_page_num
            else:
                file.seek(0, 2)
                file_size = file.tell()
                page_size = SecondaryPage.HEADER_SIZE + self.block_factor * self.index_record_template.RECORD_SIZE
                overflow_page_num = (file_size - self.DATA_START_OFFSET) // page_size

            # Create and write overflow page
            overflow_page = SecondaryPage([], -1, self.block_factor, self.index_record_template.RECORD_SIZE)
            overflow_page.insert_sorted(new_record)
            self._write_page(file, overflow_page_num, overflow_page)

            # Update main page to point to overflow page
            page.next_page = overflow_page_num
            self._write_page(file, target_data_page_num, page)
        else:
            # Find the last page in overflow chain
            current_overflow_page_num = page.next_page
            current_overflow_page = self._read_page(file, current_overflow_page_num)

            # Traverse to find last non-full page or end of chain
            while current_overflow_page.next_page != -1:
                if not current_overflow_page.is_full():
                    break
                current_overflow_page_num = current_overflow_page.next_page
                current_overflow_page = self._read_page(file, current_overflow_page_num)

            if not current_overflow_page.is_full():
                # Add to existing overflow page with space
                current_overflow_page.insert_sorted(new_record)
                self._write_page(file, current_overflow_page_num, current_overflow_page)
            else:
                # Create new overflow page at end of chain
                free_page_num = self.free_list_stack.pop_free_page()
                if free_page_num is not None:
                    new_overflow_page_num = free_page_num
                else:
                    file.seek(0, 2)
                    file_size = file.tell()
                    page_size = SecondaryPage.HEADER_SIZE + self.block_factor * self.index_record_template.RECORD_SIZE
                    new_overflow_page_num = (file_size - self.DATA_START_OFFSET) // page_size

                # Create new overflow page
                new_overflow_page = SecondaryPage([], -1, self.block_factor, self.index_record_template.RECORD_SIZE)
                new_overflow_page.insert_sorted(new_record)
                self._write_page(file, new_overflow_page_num, new_overflow_page)

                # Link previous page to new page
                current_overflow_page.next_page = new_overflow_page_num
                self._write_page(file, current_overflow_page_num, current_overflow_page)

    # Funciones extras

    def scanAll(self):
        results = []

        if not os.path.exists(self.filename):
            return results

        try:
            with open(self.filename, "rb") as file:
                file_size = os.path.getsize(self.filename)
                if file_size < self.DATA_START_OFFSET:
                    return results

                page_size = SecondaryPage.HEADER_SIZE + self.block_factor * self.index_record_template.RECORD_SIZE
                num_pages = (file_size - self.DATA_START_OFFSET) // page_size

            
                file.seek(self.DATA_START_OFFSET)
                page_data = file.read(page_size)
                self.performance.track_read()

                if len(page_data) >= SecondaryPage.HEADER_SIZE:
                    try:
                        page = SecondaryPage.unpack(page_data, self.block_factor, self.index_record_template.RECORD_SIZE, self.field_type, self.field_size)
                        results.extend(page.records)

                        # Follow the overflow chain
                        next_page_num = page.next_page
                        while next_page_num > 0 and next_page_num < num_pages:
                            file.seek(self.DATA_START_OFFSET + next_page_num * page_size)
                            page_data = file.read(page_size)
                            self.performance.track_read()
                            if len(page_data) < SecondaryPage.HEADER_SIZE:
                                break
                            try:
                                overflow_page = SecondaryPage.unpack(page_data, self.block_factor, self.index_record_template.RECORD_SIZE, self.field_type, self.field_size)
                                results.extend(overflow_page.records)
                                next_page_num = overflow_page.next_page
                            except:
                                break
                    except:
                        pass

        except Exception as e:
            pass

        return results

    def rebuild(self):
        files_to_clean = [self.filename, self.root_index_filename, self.leaf_index_filename, self.free_list_filename]
        for file_path in files_to_clean:
            if os.path.exists(file_path):
                os.remove(file_path)

        all_primary_records = self.primary_isam.scanAll()
        if all_primary_records:
            for record in all_primary_records:
                self.add_to_secondary(record)


    def show_structure(self):
        print(f"=== ESTRUCTURA DEL ISAM SECUNDARIO ({self.field_name.upper()}) ===")

        print("\n--- FREE LIST ---")
        print(f"  Páginas libres: {self.free_list_stack.get_free_count()}")

        if os.path.exists(self.root_index_filename):
            print("\n--- ROOT INDEX ---")
            self._show_root_structure()

        if os.path.exists(self.leaf_index_filename):
            print("\n--- LEAF INDEX ---")
            self._show_leaf_structure()

        print("\n--- DATA PAGES (primera muestra) ---")
        if os.path.exists(self.filename):
            self._show_data_structure()

    def _create_index_record(self, secondary_value, primary_key):
        raise NotImplementedError("Must be implemented by subclasses")

    def _get_search_key(self, secondary_value):
        raise NotImplementedError("Must be implemented by subclasses")

    def _values_equal(self, val1, val2):
        raise NotImplementedError("Must be implemented by subclasses")

    def _value_greater(self, val1, val2):
        raise NotImplementedError("Must be implemented by subclasses")

    def _value_in_range(self, value, start, end):
        raise NotImplementedError("Must be implemented by subclasses")

    def _create_initial_files(self, first_record):
        raise NotImplementedError("Must be implemented by subclasses")

    def _find_target_leaf_page(self, key_value):
        raise NotImplementedError("Must be implemented by subclasses")

    def _find_target_data_page(self, key_value, leaf_page_num):
        raise NotImplementedError("Must be implemented by subclasses")

    def _show_root_structure(self):
        raise NotImplementedError("Must be implemented by subclasses")

    def _show_leaf_structure(self):
        raise NotImplementedError("Must be implemented by subclasses")

    def _show_data_structure(self):
        try:
            with open(self.filename, "rb") as file:
                file.seek(self.DATA_START_OFFSET)
                page_data = file.read(SecondaryPage.HEADER_SIZE + self.block_factor * self.index_record_template.RECORD_SIZE)
                if page_data:
                    page = SecondaryPage.unpack(page_data, self.block_factor, self.index_record_template.RECORD_SIZE, self.field_type, self.field_size)
                    record_count = len(page.records)
                    print(f"  Página 0: {record_count} registros IndexRecord, next_page: {page.next_page}")
                    for i, record in enumerate(page.records[:5]):
                        if hasattr(record, 'index_value') and hasattr(record, 'primary_key'):
                            print(f"    [{i}] {record.index_value} -> PK:{record.primary_key}")
                    if len(page.records) > 5:
                        print(f"    ... y {len(page.records) - 5} más")
        except:
            print("  Error leyendo páginas de datos")

    def drop_index(self):
        files_to_remove = [
            self.filename,
            self.root_index_filename,
            self.leaf_index_filename,
            self.free_list_filename
        ]

        removed_files = []
        for file_path in files_to_remove:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    removed_files.append(file_path)
                except OSError:
                    pass

        return removed_files


class IntLeafIndexEntry:
    FORMAT = "ii"
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, key: int, data_page_number: int):
        self.key = key
        self.data_page_number = data_page_number

    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.data_page_number)

    @staticmethod
    def unpack(data: bytes):
        key, data_page_number = struct.unpack(IntLeafIndexEntry.FORMAT, data)
        return IntLeafIndexEntry(key, data_page_number)


class IntLeafIndex:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, entries=None, block_factor=LEAF_INDEX_BLOCK_FACTOR):
        self.entries = entries if entries else []
        self.block_factor = block_factor
        self.SIZE_OF_LEAF_INDEX = self.HEADER_SIZE + self.block_factor * IntLeafIndexEntry.SIZE

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.entries))
        entries_data = b''.join(entry.pack() for entry in self.entries)
        padding = b'\x00' * (IntLeafIndexEntry.SIZE * (self.block_factor - len(self.entries)))
        return header + entries_data + padding

    @staticmethod
    def unpack(data: bytes, block_factor=LEAF_INDEX_BLOCK_FACTOR):
        count = struct.unpack(IntLeafIndex.HEADER_FORMAT, data[:IntLeafIndex.HEADER_SIZE])[0]
        entries = []
        offset = IntLeafIndex.HEADER_SIZE

        for _ in range(count):
            entry_data = data[offset:offset + IntLeafIndexEntry.SIZE]
            entries.append(IntLeafIndexEntry.unpack(entry_data))
            offset += IntLeafIndexEntry.SIZE

        return IntLeafIndex(entries, block_factor)

    def insert_sorted(self, entry):
        left, right = 0, len(self.entries)
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < entry.key:
                left = mid + 1
            else:
                right = mid
        self.entries.insert(left, entry)

    def is_full(self):
        return len(self.entries) >= self.block_factor


class IntRootIndexEntry:
    FORMAT = "ii"
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, key: int, leaf_page_number: int):
        self.key = key
        self.leaf_page_number = leaf_page_number

    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.leaf_page_number)

    @staticmethod
    def unpack(data: bytes):
        key, leaf_page_number = struct.unpack(IntRootIndexEntry.FORMAT, data)
        return IntRootIndexEntry(key, leaf_page_number)


class IntRootIndex:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, entries=None, block_factor=ROOT_INDEX_BLOCK_FACTOR):
        self.entries = entries if entries else []
        self.block_factor = block_factor
        self.SIZE_OF_ROOT_INDEX = self.HEADER_SIZE + self.block_factor * IntRootIndexEntry.SIZE

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.entries))
        entries_data = b''.join(entry.pack() for entry in self.entries)
        padding = b'\x00' * (IntRootIndexEntry.SIZE * (self.block_factor - len(self.entries)))
        return header + entries_data + padding

    @staticmethod
    def unpack(data: bytes, block_factor=ROOT_INDEX_BLOCK_FACTOR):
        count = struct.unpack(IntRootIndex.HEADER_FORMAT, data[:IntRootIndex.HEADER_SIZE])[0]
        entries = []
        offset = IntRootIndex.HEADER_SIZE

        for _ in range(count):
            entry_data = data[offset:offset + IntRootIndexEntry.SIZE]
            entries.append(IntRootIndexEntry.unpack(entry_data))
            offset += IntRootIndexEntry.SIZE

        return IntRootIndex(entries, block_factor)

    def insert_sorted(self, entry):
        left, right = 0, len(self.entries)
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < entry.key:
                left = mid + 1
            else:
                right = mid
        self.entries.insert(left, entry)

    def is_full(self):
        return len(self.entries) >= self.block_factor


class ISAMSecondaryIndexINT(ISAMSecondaryBase):
    def __init__(self, field_name: str, primary_isam, filename=None):
        super().__init__(field_name, primary_isam, filename)
        self.field_type = "INT"
        self.field_size = 4
        self.index_record_template = IndexRecord("INT", 4)

    def _create_index_record(self, secondary_value, primary_key):
        index_record = IndexRecord(self.field_type, self.field_size)
        index_record.set_index_data(secondary_value, primary_key)
        return index_record

    def _get_search_key(self, secondary_value):
        return secondary_value

    def _values_equal(self, val1, val2):
        return val1 == val2

    def _value_greater(self, val1, val2):
        return val1 > val2

    def _value_in_range(self, value, start, end):
        return start <= value <= end

    def _create_initial_files(self, first_record):
        with open(self.filename, "wb") as file:
            file.write(struct.pack(self.HEADER_FORMAT, 1))

            page = SecondaryPage([first_record], -1, self.block_factor, self.index_record_template.RECORD_SIZE)
            file.write(page.pack())

        with open(self.root_index_filename, "wb") as root_file:
            root_entry = IntRootIndexEntry(first_record.get_key(), 0)
            root_index = IntRootIndex([root_entry], self.root_index_block_factor)
            root_file.write(root_index.pack())

        with open(self.leaf_index_filename, "wb") as leaf_file:
            leaf_entry = IntLeafIndexEntry(first_record.get_key(), 0)
            leaf_index = IntLeafIndex([leaf_entry], self.leaf_index_block_factor)
            leaf_file.write(leaf_index.pack())

    def _find_target_leaf_page(self, key_value, track_reads=False):
        if not os.path.exists(self.root_index_filename):
            return 0

        with open(self.root_index_filename, "rb") as file:
            root_index = IntRootIndex.unpack(file.read(), self.root_index_block_factor)
            if track_reads:
                self.performance.track_read()

        for i in range(len(root_index.entries) - 1, -1, -1):
            if key_value >= root_index.entries[i].key:
                return root_index.entries[i].leaf_page_number

        return 0

    def _find_target_data_page(self, key_value, leaf_page_num, track_reads=False):
        if not os.path.exists(self.leaf_index_filename):
            return 0

        with open(self.leaf_index_filename, "rb") as file:
            file.seek(leaf_page_num * IntLeafIndex().SIZE_OF_LEAF_INDEX)
            leaf_data = file.read(IntLeafIndex().SIZE_OF_LEAF_INDEX)
            if track_reads:
                self.performance.track_read()
            if len(leaf_data) < IntLeafIndex().SIZE_OF_LEAF_INDEX:
                return 0
            leaf_index = IntLeafIndex.unpack(leaf_data, self.leaf_index_block_factor)

        for i in range(len(leaf_index.entries) - 1, -1, -1):
            if key_value >= leaf_index.entries[i].key:
                return leaf_index.entries[i].data_page_number

        return 0

    def _show_root_structure(self):
        with open(self.root_index_filename, "rb") as file:
            root_index = IntRootIndex.unpack(file.read(), self.root_index_block_factor)
            for entry in root_index.entries:
                print(f"  RootKey: {entry.key} -> LeafPage: {entry.leaf_page_number}")

    def _show_leaf_structure(self):
        with open(self.leaf_index_filename, "rb") as file:
            file.seek(0, 2)
            file_size = file.tell()
            num_leaf_pages = file_size // IntLeafIndex().SIZE_OF_LEAF_INDEX

            for i in range(min(3, num_leaf_pages)):
                file.seek(i * IntLeafIndex().SIZE_OF_LEAF_INDEX)
                leaf_data = file.read(IntLeafIndex().SIZE_OF_LEAF_INDEX)
                leaf_index = IntLeafIndex.unpack(leaf_data, self.leaf_index_block_factor)
                print(f"  Leaf Page {i}:")
                for entry in leaf_index.entries:
                    print(f"    LeafKey: {entry.key} -> DataPage: {entry.data_page_number}")


class CharLeafIndexEntry:
    def __init__(self, key: str, data_page_number: int, max_key_size: int = 50):
        self.key = key
        self.data_page_number = data_page_number
        self.max_key_size = max_key_size
        self.FORMAT = f"{max_key_size}si"
        self.SIZE = struct.calcsize(self.FORMAT)

    def pack(self) -> bytes:
        key_bytes = str(self.key).encode('utf-8')[:self.max_key_size].ljust(self.max_key_size, b'\x00')
        return struct.pack(self.FORMAT, key_bytes, self.data_page_number)

    @staticmethod
    def unpack(data: bytes, max_key_size: int = 50):
        format_str = f"{max_key_size}si"
        key_bytes, data_page_number = struct.unpack(format_str, data)
        key = key_bytes.decode('utf-8').rstrip('\x00')
        entry = CharLeafIndexEntry(key, data_page_number, max_key_size)
        return entry


class CharLeafIndex:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, entries=None, block_factor=LEAF_INDEX_BLOCK_FACTOR, max_key_size=50):
        self.entries = entries if entries else []
        self.block_factor = block_factor
        self.max_key_size = max_key_size
        self.entry_size = struct.calcsize(f"{max_key_size}si")
        self.SIZE_OF_LEAF_INDEX = self.HEADER_SIZE + self.block_factor * self.entry_size

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.entries))
        entries_data = b''.join(entry.pack() for entry in self.entries)
        padding = b'\x00' * (self.entry_size * (self.block_factor - len(self.entries)))
        return header + entries_data + padding

    @staticmethod
    def unpack(data: bytes, block_factor=LEAF_INDEX_BLOCK_FACTOR, max_key_size=50):
        count = struct.unpack(CharLeafIndex.HEADER_FORMAT, data[:CharLeafIndex.HEADER_SIZE])[0]
        entries = []
        offset = CharLeafIndex.HEADER_SIZE
        entry_size = struct.calcsize(f"{max_key_size}si")

        for _ in range(count):
            entry_data = data[offset:offset + entry_size]
            entries.append(CharLeafIndexEntry.unpack(entry_data, max_key_size))
            offset += entry_size

        return CharLeafIndex(entries, block_factor, max_key_size)

    def insert_sorted(self, entry):
        left, right = 0, len(self.entries)
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < entry.key:
                left = mid + 1
            else:
                right = mid
        self.entries.insert(left, entry)

    def is_full(self):
        return len(self.entries) >= self.block_factor


class CharRootIndexEntry:
    def __init__(self, key: str, leaf_page_number: int, max_key_size: int = 50):
        self.key = key
        self.leaf_page_number = leaf_page_number
        self.max_key_size = max_key_size
        self.FORMAT = f"{max_key_size}si"
        self.SIZE = struct.calcsize(self.FORMAT)

    def pack(self) -> bytes:
        key_bytes = str(self.key).encode('utf-8')[:self.max_key_size].ljust(self.max_key_size, b'\x00')
        return struct.pack(self.FORMAT, key_bytes, self.leaf_page_number)

    @staticmethod
    def unpack(data: bytes, max_key_size: int = 50):
        format_str = f"{max_key_size}si"
        key_bytes, leaf_page_number = struct.unpack(format_str, data)
        key = key_bytes.decode('utf-8').rstrip('\x00')
        entry = CharRootIndexEntry(key, leaf_page_number, max_key_size)
        return entry


class CharRootIndex:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, entries=None, block_factor=ROOT_INDEX_BLOCK_FACTOR, max_key_size=50):
        self.entries = entries if entries else []
        self.block_factor = block_factor
        self.max_key_size = max_key_size
        self.entry_size = struct.calcsize(f"{max_key_size}si")
        self.SIZE_OF_ROOT_INDEX = self.HEADER_SIZE + self.block_factor * self.entry_size

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.entries))
        entries_data = b''.join(entry.pack() for entry in self.entries)
        padding = b'\x00' * (self.entry_size * (self.block_factor - len(self.entries)))
        return header + entries_data + padding

    @staticmethod
    def unpack(data: bytes, block_factor=ROOT_INDEX_BLOCK_FACTOR, max_key_size=50):
        count = struct.unpack(CharRootIndex.HEADER_FORMAT, data[:CharRootIndex.HEADER_SIZE])[0]
        entries = []
        offset = CharRootIndex.HEADER_SIZE
        entry_size = struct.calcsize(f"{max_key_size}si")

        for _ in range(count):
            entry_data = data[offset:offset + entry_size]
            entries.append(CharRootIndexEntry.unpack(entry_data, max_key_size))
            offset += entry_size

        return CharRootIndex(entries, block_factor, max_key_size)

    def insert_sorted(self, entry):
        left, right = 0, len(self.entries)
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < entry.key:
                left = mid + 1
            else:
                right = mid
        self.entries.insert(left, entry)

    def is_full(self):
        return len(self.entries) >= self.block_factor


class ISAMSecondaryIndexCHAR(ISAMSecondaryBase):
    def __init__(self, field_name: str, field_size: int, primary_isam, filename=None):
        super().__init__(field_name, primary_isam, filename)
        self.field_type = "CHAR"
        self.field_size = field_size
        self.max_key_size = field_size
        self.index_record_template = IndexRecord("CHAR", field_size)

    def _create_index_record(self, secondary_value, primary_key):
        index_record = IndexRecord(self.field_type, self.field_size)
        index_record.set_index_data(secondary_value, primary_key)
        return index_record

    def _get_search_key(self, secondary_value):
        return str(secondary_value)

    def _values_equal(self, val1, val2):
        try:
            str_val1 = val1.decode('utf-8', errors='ignore').rstrip('\x00').rstrip() if isinstance(val1, bytes) else str(val1).rstrip()
            str_val2 = val2.decode('utf-8', errors='ignore').rstrip('\x00').rstrip() if isinstance(val2, bytes) else str(val2).rstrip()
            return str_val1 == str_val2
        except (UnicodeDecodeError, AttributeError):
            return False

    def _value_greater(self, val1, val2):
        try:
            str_val1 = val1.decode('utf-8', errors='ignore').rstrip('\x00').rstrip() if isinstance(val1, bytes) else str(val1).rstrip()
            str_val2 = val2.decode('utf-8', errors='ignore').rstrip('\x00').rstrip() if isinstance(val2, bytes) else str(val2).rstrip()
            return str_val1 > str_val2
        except (UnicodeDecodeError, AttributeError):
            return False

    def _value_in_range(self, value, start, end):
        try:
            str_val = value.decode('utf-8', errors='ignore').rstrip('\x00').rstrip() if isinstance(value, bytes) else str(value).rstrip()
            str_start = start.decode('utf-8', errors='ignore').rstrip('\x00').rstrip() if isinstance(start, bytes) else str(start).rstrip()
            str_end = end.decode('utf-8', errors='ignore').rstrip('\x00').rstrip() if isinstance(end, bytes) else str(end).rstrip()
            return str_start <= str_val <= str_end
        except (UnicodeDecodeError, AttributeError):
            # Fallback para casos problemáticos
            return False

    def _create_initial_files(self, first_record):
        with open(self.filename, "wb") as file:
            file.write(struct.pack(self.HEADER_FORMAT, 1))

            page = SecondaryPage([first_record], -1, self.block_factor, self.index_record_template.RECORD_SIZE)
            file.write(page.pack())

        with open(self.root_index_filename, "wb") as root_file:
            key_str = self._get_search_key(first_record.get_key())
            root_entry = CharRootIndexEntry(key_str, 0, self.max_key_size)
            root_index = CharRootIndex([root_entry], self.root_index_block_factor, self.max_key_size)
            root_file.write(root_index.pack())

        with open(self.leaf_index_filename, "wb") as leaf_file:
            key_str = self._get_search_key(first_record.get_key())
            leaf_entry = CharLeafIndexEntry(key_str, 0, self.max_key_size)
            leaf_index = CharLeafIndex([leaf_entry], self.leaf_index_block_factor, self.max_key_size)
            leaf_file.write(leaf_index.pack())

    def _find_target_leaf_page(self, key_value, track_reads=False):
        if not os.path.exists(self.root_index_filename):
            return 0

        with open(self.root_index_filename, "rb") as file:
            root_index = CharRootIndex.unpack(file.read(), self.root_index_block_factor, self.max_key_size)
            if track_reads:
                self.performance.track_read()

        for i in range(len(root_index.entries) - 1, -1, -1):
            if key_value >= root_index.entries[i].key:
                return root_index.entries[i].leaf_page_number

        return 0

    def _find_target_data_page(self, key_value, leaf_page_num, track_reads=False):
        if not os.path.exists(self.leaf_index_filename):
            return 0

        with open(self.leaf_index_filename, "rb") as file:
            leaf_index_size = CharLeafIndex(max_key_size=self.max_key_size).SIZE_OF_LEAF_INDEX
            file.seek(leaf_page_num * leaf_index_size)
            leaf_data = file.read(leaf_index_size)
            if track_reads:
                self.performance.track_read()
            if len(leaf_data) < leaf_index_size:
                return 0
            leaf_index = CharLeafIndex.unpack(leaf_data, self.leaf_index_block_factor, self.max_key_size)

        for i in range(len(leaf_index.entries) - 1, -1, -1):
            if key_value >= leaf_index.entries[i].key:
                return leaf_index.entries[i].data_page_number

        return 0

    def _show_root_structure(self):
        with open(self.root_index_filename, "rb") as file:
            root_index = CharRootIndex.unpack(file.read(), self.root_index_block_factor, self.max_key_size)
            for entry in root_index.entries:
                print(f"  RootKey: '{entry.key}' -> LeafPage: {entry.leaf_page_number}")

    def _show_leaf_structure(self):
        with open(self.leaf_index_filename, "rb") as file:
            file.seek(0, 2)
            file_size = file.tell()
            leaf_index_size = CharLeafIndex(max_key_size=self.max_key_size).SIZE_OF_LEAF_INDEX
            num_leaf_pages = file_size // leaf_index_size

            for i in range(min(3, num_leaf_pages)):
                file.seek(i * leaf_index_size)
                leaf_data = file.read(leaf_index_size)
                leaf_index = CharLeafIndex.unpack(leaf_data, self.leaf_index_block_factor, self.max_key_size)
                print(f"  Leaf Page {i}:")
                for entry in leaf_index.entries:
                    print(f"    LeafKey: '{entry.key}' -> DataPage: {entry.data_page_number}")


class FloatLeafIndexEntry:
    FORMAT = "fi"
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, key: float, data_page_number: int):
        self.key = key
        self.data_page_number = data_page_number

    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.data_page_number)

    @staticmethod
    def unpack(data: bytes):
        key, data_page_number = struct.unpack(FloatLeafIndexEntry.FORMAT, data)
        return FloatLeafIndexEntry(key, data_page_number)


class FloatLeafIndex:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, entries=None, block_factor=LEAF_INDEX_BLOCK_FACTOR):
        self.entries = entries if entries else []
        self.block_factor = block_factor
        self.SIZE_OF_LEAF_INDEX = self.HEADER_SIZE + self.block_factor * FloatLeafIndexEntry.SIZE

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.entries))
        entries_data = b''.join(entry.pack() for entry in self.entries)
        padding = b'\x00' * (FloatLeafIndexEntry.SIZE * (self.block_factor - len(self.entries)))
        return header + entries_data + padding

    @staticmethod
    def unpack(data: bytes, block_factor=LEAF_INDEX_BLOCK_FACTOR):
        count = struct.unpack(FloatLeafIndex.HEADER_FORMAT, data[:FloatLeafIndex.HEADER_SIZE])[0]
        entries = []
        offset = FloatLeafIndex.HEADER_SIZE

        for _ in range(count):
            entry_data = data[offset:offset + FloatLeafIndexEntry.SIZE]
            entries.append(FloatLeafIndexEntry.unpack(entry_data))
            offset += FloatLeafIndexEntry.SIZE

        return FloatLeafIndex(entries, block_factor)

    def insert_sorted(self, entry):
        left, right = 0, len(self.entries)
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < entry.key:
                left = mid + 1
            else:
                right = mid
        self.entries.insert(left, entry)

    def is_full(self):
        return len(self.entries) >= self.block_factor


class FloatRootIndexEntry:
    FORMAT = "fi"
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, key: float, leaf_page_number: int):
        self.key = key
        self.leaf_page_number = leaf_page_number

    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.leaf_page_number)

    @staticmethod
    def unpack(data: bytes):
        key, leaf_page_number = struct.unpack(FloatRootIndexEntry.FORMAT, data)
        return FloatRootIndexEntry(key, leaf_page_number)


class FloatRootIndex:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, entries=None, block_factor=ROOT_INDEX_BLOCK_FACTOR):
        self.entries = entries if entries else []
        self.block_factor = block_factor
        self.SIZE_OF_ROOT_INDEX = self.HEADER_SIZE + self.block_factor * FloatRootIndexEntry.SIZE

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.entries))
        entries_data = b''.join(entry.pack() for entry in self.entries)
        padding = b'\x00' * (FloatRootIndexEntry.SIZE * (self.block_factor - len(self.entries)))
        return header + entries_data + padding

    @staticmethod
    def unpack(data: bytes, block_factor=ROOT_INDEX_BLOCK_FACTOR):
        count = struct.unpack(FloatRootIndex.HEADER_FORMAT, data[:FloatRootIndex.HEADER_SIZE])[0]
        entries = []
        offset = FloatRootIndex.HEADER_SIZE

        for _ in range(count):
            entry_data = data[offset:offset + FloatRootIndexEntry.SIZE]
            entries.append(FloatRootIndexEntry.unpack(entry_data))
            offset += FloatRootIndexEntry.SIZE

        return FloatRootIndex(entries, block_factor)

    def insert_sorted(self, entry):
        left, right = 0, len(self.entries)
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < entry.key:
                left = mid + 1
            else:
                right = mid
        self.entries.insert(left, entry)

    def is_full(self):
        return len(self.entries) >= self.block_factor


class ISAMSecondaryIndexFLOAT(ISAMSecondaryBase):
    def __init__(self, field_name: str, primary_isam, filename=None):
        super().__init__(field_name, primary_isam, filename)
        self.field_type = "FLOAT"
        self.field_size = 4
        self.index_record_template = IndexRecord("FLOAT", 4)

    def _create_index_record(self, secondary_value, primary_key):
        index_record = IndexRecord(self.field_type, self.field_size)
        index_record.set_index_data(secondary_value, primary_key)
        return index_record

    def _get_search_key(self, secondary_value):
        return float(secondary_value)

    def _values_equal(self, val1, val2):
        # Comparación de floats con tolerancia para manejar errores de precisión
        return abs(float(val1) - float(val2)) < 1e-9

    def _value_greater(self, val1, val2):
        return float(val1) > float(val2)

    def _value_in_range(self, value, start, end):
        float_value = float(value)
        float_start = float(start)
        float_end = float(end)
        return float_start <= float_value <= float_end

    def _create_initial_files(self, first_record):
        with open(self.filename, "wb") as file:
            file.write(struct.pack(self.HEADER_FORMAT, 1))

            page = SecondaryPage([first_record], -1, self.block_factor, self.index_record_template.RECORD_SIZE)
            file.write(page.pack())

        with open(self.root_index_filename, "wb") as root_file:
            root_entry = FloatRootIndexEntry(first_record.get_key(), 0)
            root_index = FloatRootIndex([root_entry], self.root_index_block_factor)
            root_file.write(root_index.pack())

        with open(self.leaf_index_filename, "wb") as leaf_file:
            leaf_entry = FloatLeafIndexEntry(first_record.get_key(), 0)
            leaf_index = FloatLeafIndex([leaf_entry], self.leaf_index_block_factor)
            leaf_file.write(leaf_index.pack())

    def _find_target_leaf_page(self, key_value, track_reads=False):
        if not os.path.exists(self.root_index_filename):
            return 0

        with open(self.root_index_filename, "rb") as file:
            root_index = FloatRootIndex.unpack(file.read(), self.root_index_block_factor)
            if track_reads:
                self.performance.track_read()

        for i in range(len(root_index.entries) - 1, -1, -1):
            if key_value >= root_index.entries[i].key:
                return root_index.entries[i].leaf_page_number

        return 0

    def _find_target_data_page(self, key_value, leaf_page_num, track_reads=False):
        if not os.path.exists(self.leaf_index_filename):
            return 0

        with open(self.leaf_index_filename, "rb") as file:
            file.seek(leaf_page_num * FloatLeafIndex().SIZE_OF_LEAF_INDEX)
            leaf_data = file.read(FloatLeafIndex().SIZE_OF_LEAF_INDEX)
            if track_reads:
                self.performance.track_read()
            if len(leaf_data) < FloatLeafIndex().SIZE_OF_LEAF_INDEX:
                return 0
            leaf_index = FloatLeafIndex.unpack(leaf_data, self.leaf_index_block_factor)

        for i in range(len(leaf_index.entries) - 1, -1, -1):
            if key_value >= leaf_index.entries[i].key:
                return leaf_index.entries[i].data_page_number

        return 0

    def _show_root_structure(self):
        with open(self.root_index_filename, "rb") as file:
            root_index = FloatRootIndex.unpack(file.read(), self.root_index_block_factor)
            for entry in root_index.entries:
                print(f"  RootKey: {entry.key:.2f} -> LeafPage: {entry.leaf_page_number}")

    def _show_leaf_structure(self):
        with open(self.leaf_index_filename, "rb") as file:
            file.seek(0, 2)
            file_size = file.tell()
            num_leaf_pages = file_size // FloatLeafIndex().SIZE_OF_LEAF_INDEX

            for i in range(min(3, num_leaf_pages)):
                file.seek(i * FloatLeafIndex().SIZE_OF_LEAF_INDEX)
                leaf_data = file.read(FloatLeafIndex().SIZE_OF_LEAF_INDEX)
                leaf_index = FloatLeafIndex.unpack(leaf_data, self.leaf_index_block_factor)
                print(f"  Leaf Page {i}:")
                for entry in leaf_index.entries:
                    print(f"    LeafKey: {entry.key:.2f} -> DataPage: {entry.data_page_number}")


def create_secondary_index(field_name: str, field_type: str, field_size: int, primary_isam, filename=None):
    if field_type == "INT":
        return ISAMSecondaryIndexINT(field_name, primary_isam, filename)
    elif field_type == "CHAR":
        return ISAMSecondaryIndexCHAR(field_name, field_size, primary_isam, filename)
    elif field_type == "FLOAT":
        return ISAMSecondaryIndexFLOAT(field_name, primary_isam, filename)
    else:
        raise ValueError(f"Unsupported secondary index type: {field_type}. Supported: INT, CHAR")


ISAMSecondaryIndex = create_secondary_index
import os, struct

BLOCK_FACTOR = 10
ROOT_INDEX_BLOCK_FACTOR = 5
LEAF_INDEX_BLOCK_FACTOR = 8
CONSOLIDATION_THRESHOLD = BLOCK_FACTOR // 3

class Record:
    FORMAT = '50sifi10s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id_venta: int, nombre_producto: str, cantidad_vendida: int, precio_unitario: float, fecha_venta: str):
        self.id_venta = id_venta
        self.nombre_producto = nombre_producto
        self.cantidad_vendida = cantidad_vendida
        self.precio_unitario = precio_unitario
        self.fecha_venta = fecha_venta
    
    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, 
            self.nombre_producto[:50].ljust(50).encode(),
            self.id_venta,
            self.precio_unitario,
            self.cantidad_vendida,
            self.fecha_venta[:10].ljust(10).encode()
        )

    @staticmethod
    def unpack(data: bytes):
        nombre_producto, id_venta, precio_unitario, cantidad_vendida, fecha_venta = struct.unpack(Record.FORMAT, data)
        return Record(id_venta, nombre_producto.decode().rstrip(), cantidad_vendida, precio_unitario, fecha_venta.decode().rstrip())

    def __str__(self):
        return f"{self.id_venta} - {self.nombre_producto} - {self.cantidad_vendida} - {self.precio_unitario} - {self.fecha_venta}"


class Page:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_PAGE = HEADER_SIZE + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records=None, next_page=-1):
        self.records = records if records else []
        self.next_page = next_page

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_page)
        record_data = b''.join(r.pack() for r in self.records)
        record_data += b'\x00' * (Record.SIZE_OF_RECORD * (BLOCK_FACTOR - len(self.records)))
        return header_data + record_data

    @staticmethod
    def unpack(data: bytes): 
        size, next_page = struct.unpack(Page.HEADER_FORMAT, data[:Page.HEADER_SIZE])
        offset = Page.HEADER_SIZE
        records = []
        for _ in range(size):
            record_data = data[offset: offset + Record.SIZE_OF_RECORD]
            records.append(Record.unpack(record_data))
            offset += Record.SIZE_OF_RECORD
        return Page(records, next_page)
    
    def insert_sorted(self, record):
        left, right = 0, len(self.records)
        while left < right:
            mid = (left + right) // 2
            if self.records[mid].id_venta < record.id_venta:
                left = mid + 1
            else:
                right = mid
        self.records.insert(left, record)

    def is_full(self):
        return len(self.records) >= BLOCK_FACTOR
    
    def remove_record(self, id_venta):
        original_count = len(self.records)
        left, right = 0, len(self.records) - 1
        found_indices = []
        
        while left <= right:
            mid = (left + right) // 2
            if self.records[mid].id_venta == id_venta:
                found_indices.append(mid)
                i = mid - 1
                while i >= 0 and self.records[i].id_venta == id_venta:
                    found_indices.append(i)
                    i -= 1
                i = mid + 1
                while i < len(self.records) and self.records[i].id_venta == id_venta:
                    found_indices.append(i)
                    i += 1
                break
            elif self.records[mid].id_venta < id_venta:
                left = mid + 1
            else:
                right = mid - 1
        
        for idx in sorted(found_indices, reverse=True):
            del self.records[idx]
            
        return len(self.records) < original_count
    
    def is_empty(self):
        return len(self.records) == 0
    
    def can_merge_with(self, other_page):
        return len(self.records) + len(other_page.records) <= BLOCK_FACTOR
    
    def merge_with(self, other_page):
        all_records = self.records + other_page.records
        all_records.sort(key=lambda r: r.id_venta)
        self.records = all_records


class RootIndexEntry:
    FORMAT = "ii"
    SIZE = struct.calcsize(FORMAT)
    
    def __init__(self, key: int, leaf_page_number: int):
        self.key = key
        self.leaf_page_number = leaf_page_number
    
    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.leaf_page_number)
    
    @staticmethod
    def unpack(data: bytes):
        key, leaf_page_number = struct.unpack(RootIndexEntry.FORMAT, data)
        return RootIndexEntry(key, leaf_page_number)
    
    def __str__(self):
        return f"RootKey: {self.key} -> LeafPage: {self.leaf_page_number}"


class RootIndex:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_ROOT_INDEX = HEADER_SIZE + ROOT_INDEX_BLOCK_FACTOR * RootIndexEntry.SIZE

    def __init__(self, entries=None, next_page=-1):
        self.entries = entries if entries else []
        self.next_page = next_page

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.entries), self.next_page)
        entries_data = b''.join(entry.pack() for entry in self.entries)
        entries_data += b'\x00' * (RootIndexEntry.SIZE * (ROOT_INDEX_BLOCK_FACTOR - len(self.entries)))
        return header_data + entries_data

    @staticmethod
    def unpack(data: bytes):
        size, next_page = struct.unpack(RootIndex.HEADER_FORMAT, data[:RootIndex.HEADER_SIZE])
        offset = RootIndex.HEADER_SIZE
        entries = []
        for _ in range(size):
            entry_data = data[offset: offset + RootIndexEntry.SIZE]
            entries.append(RootIndexEntry.unpack(entry_data))
            offset += RootIndexEntry.SIZE
        return RootIndex(entries, next_page)

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
        return len(self.entries) >= ROOT_INDEX_BLOCK_FACTOR

    def find_leaf_page_for_key(self, key):
        if not self.entries:
            return 0
        
        left = 0
        right = len(self.entries) - 1
        result_page = 0 
        
        while left <= right:
            mid = (left + right) // 2
            mid_key = self.entries[mid].key
            
            if key < mid_key:
                right = mid - 1
            elif key >= mid_key:
                result_page = self.entries[mid].leaf_page_number
                left = mid + 1
            
        return result_page


class LeafIndexEntry:
    FORMAT = "ii"
    SIZE = struct.calcsize(FORMAT)
    
    def __init__(self, key: int, data_page_number: int):
        self.key = key
        self.data_page_number = data_page_number
    
    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.data_page_number)
    
    @staticmethod
    def unpack(data: bytes):
        key, data_page_number = struct.unpack(LeafIndexEntry.FORMAT, data)
        return LeafIndexEntry(key, data_page_number)
    
    def __str__(self):
        return f"LeafKey: {self.key} -> DataPage: {self.data_page_number}"


class LeafIndex:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_LEAF_INDEX = HEADER_SIZE + LEAF_INDEX_BLOCK_FACTOR * LeafIndexEntry.SIZE

    def __init__(self, entries=None, next_page=-1):
        self.entries = entries if entries else []
        self.next_page = next_page

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.entries), self.next_page)
        entries_data = b''.join(entry.pack() for entry in self.entries)
        entries_data += b'\x00' * (LeafIndexEntry.SIZE * (LEAF_INDEX_BLOCK_FACTOR - len(self.entries)))
        return header_data + entries_data

    @staticmethod
    def unpack(data: bytes):
        size, next_page = struct.unpack(LeafIndex.HEADER_FORMAT, data[:LeafIndex.HEADER_SIZE])
        offset = LeafIndex.HEADER_SIZE
        entries = []
        for _ in range(size):
            entry_data = data[offset: offset + LeafIndexEntry.SIZE]
            entries.append(LeafIndexEntry.unpack(entry_data))
            offset += LeafIndexEntry.SIZE
        return LeafIndex(entries, next_page)

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
        return len(self.entries) >= LEAF_INDEX_BLOCK_FACTOR

    def find_data_page_for_key(self, key):
        if not self.entries:
            return 0
        
        left = 0
        right = len(self.entries) - 1
        result_page = 0 
        
        while left <= right:
            mid = (left + right) // 2
            mid_key = self.entries[mid].key
            
            if key < mid_key:
                right = mid - 1
            elif key >= mid_key:
                result_page = self.entries[mid].data_page_number
                left = mid + 1
            
        return result_page


class FreeListStack:
    def __init__(self, free_list_file="free_list.dat"):
        self.free_list_file = free_list_file

    def push_free_page(self, page_num):
        try:
            if not os.path.exists(self.free_list_file):
                count = 0
            else:
                with open(self.free_list_file, "rb") as file:
                    count_data = file.read(4)
                    count = struct.unpack('i', count_data)[0] if count_data else 0

            with open(self.free_list_file, "r+b" if count > 0 else "wb") as file:
                file.seek(0)
                file.write(struct.pack('i', count + 1))
                file.seek(0, 2)
                file.write(struct.pack('i', page_num))
            return True
        except:
            return False
    
    def pop_free_page(self):
        if not os.path.exists(self.free_list_file):
            return None

        try:
            with open(self.free_list_file, "r+b") as file:
                count_data = file.read(4)
                if len(count_data) < 4:
                    return None

                count = struct.unpack('i', count_data)[0]
                if count <= 0:
                    return None

                file.seek(4 + (count - 1) * 4)
                page_num = struct.unpack('i', file.read(4))[0]

                file.seek(0)
                file.write(struct.pack('i', count - 1))

                return page_num
        except:
            return None
    
    def get_free_count(self):
        if not os.path.exists(self.free_list_file):
            return 0

        try:
            with open(self.free_list_file, "rb") as file:
                count_data = file.read(4)
                return struct.unpack('i', count_data)[0] if count_data else 0
        except:
            return 0
    
    def clear(self):
        if os.path.exists(self.free_list_file):
            os.remove(self.free_list_file)
    
    def is_empty(self):
        return self.get_free_count() == 0


class ISAMFile:
    HEADER_FORMAT = 'i'  
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DATA_START_OFFSET = HEADER_SIZE  
    
    # Inicialización

    def __init__(self, filename="datos.dat", root_index_file="root_index.dat", leaf_index_file="leaf_index.dat", free_list_file="free_list.dat",
                 block_factor=None, root_index_block_factor=None, leaf_index_block_factor=None, consolidation_threshold=None):
        self.filename = filename
        self.root_index_file = root_index_file
        self.leaf_index_file = leaf_index_file
        self.free_list_stack = FreeListStack(free_list_file)

        self.block_factor = block_factor if block_factor is not None else BLOCK_FACTOR
        self.root_index_block_factor = root_index_block_factor if root_index_block_factor is not None else ROOT_INDEX_BLOCK_FACTOR
        self.leaf_index_block_factor = leaf_index_block_factor if leaf_index_block_factor is not None else LEAF_INDEX_BLOCK_FACTOR
        self.consolidation_threshold = consolidation_threshold if consolidation_threshold is not None else CONSOLIDATION_THRESHOLD

        self.next_page_number = 0
        self.next_root_index_page_number = 0
        self.next_leaf_index_page_number = 0

    def _create_initial_files(self, record):
        with open(self.filename, "wb") as file:
            file.write(struct.pack(self.HEADER_FORMAT, 0))
            page = Page([record])
            file.write(page.pack())

        with open(self.leaf_index_file, "wb") as file:
            initial_entry = LeafIndexEntry(record.id_venta, 0)
            leaf_index = LeafIndex([initial_entry])
            file.write(leaf_index.pack())

        with open(self.root_index_file, "wb") as file:
            root_index = RootIndex([])
            file.write(root_index.pack())
        
        self.free_list_stack.clear()
        
        self.next_page_number = 1
        self.next_leaf_index_page_number = 1
        self.next_root_index_page_number = 1

    # Manejo de la free list
    
    def _push_free_page(self, page_num):
        return self.free_list_stack.push_free_page(page_num)

    def _pop_free_page(self):
        return self.free_list_stack.pop_free_page()

    def _get_free_count(self):
        return self.free_list_stack.get_free_count()

    # Escritura y lectura de páginas e índices
    
    def _read_page(self, file, page_num):
        offset = self.DATA_START_OFFSET + (page_num * Page.SIZE_OF_PAGE)
        file.seek(offset)
        return Page.unpack(file.read(Page.SIZE_OF_PAGE))

    def _write_page(self, file, page_num, page):
        offset = self.DATA_START_OFFSET + (page_num * Page.SIZE_OF_PAGE)
        file.seek(offset)
        file.write(page.pack())

    def _read_root_index(self, file, page_num):
        file.seek(page_num * RootIndex.SIZE_OF_ROOT_INDEX)
        return RootIndex.unpack(file.read(RootIndex.SIZE_OF_ROOT_INDEX))

    def _write_root_index(self, file, page_num, root_index):
        file.seek(page_num * RootIndex.SIZE_OF_ROOT_INDEX)
        file.write(root_index.pack())

    def _read_leaf_index(self, file, page_num):
        file.seek(page_num * LeafIndex.SIZE_OF_LEAF_INDEX)
        return LeafIndex.unpack(file.read(LeafIndex.SIZE_OF_LEAF_INDEX))

    def _write_leaf_index(self, file, page_num, leaf_index):
        file.seek(page_num * LeafIndex.SIZE_OF_LEAF_INDEX)
        file.write(leaf_index.pack())

    # Operaciones comunes
    
    def _find_target_leaf_page(self, id_venta):
        if not os.path.exists(self.root_index_file):
            return 0
        
        with open(self.root_index_file, "rb") as file:
            root_index = self._read_root_index(file, 0)
            return root_index.find_leaf_page_for_key(id_venta)

    def _find_target_data_page(self, id_venta, leaf_page_num):
        if not os.path.exists(self.leaf_index_file):
            return 0
        
        with open(self.leaf_index_file, "rb") as file:
            leaf_index = self._read_leaf_index(file, leaf_page_num)
            return leaf_index.find_data_page_for_key(id_venta)

    def _handle_page_overflow(self, file, page_num, page, new_record, current_leaf_page_num):
        with open(self.leaf_index_file, "rb") as leaf_index_file:
            leaf_index_obj = self._read_leaf_index(leaf_index_file, current_leaf_page_num)
            
            if not leaf_index_obj.is_full():
                # Estrategia 1: Split página de datos
                self._split_page_strategy(file, page_num, page, new_record, current_leaf_page_num)
            else:
                with open(self.root_index_file, "rb") as root_file:
                    root_index_obj = self._read_root_index(root_file, 0)

                    if not root_index_obj.is_full():
                        # Estrategia 2: Split leaf index
                        self._split_leaf_index_strategy(file, page_num, page, new_record, current_leaf_page_num)
                    else:
                        # Estrategia 3: Overflow chain
                        self._overflow_page_strategy(file, page_num, page, new_record)

    def _split_page_strategy(self, file, page_num, page, new_record, leaf_page_num):
        all_records = page.records + [new_record]
        all_records.sort(key=lambda r: r.id_venta)

        mid_point = len(all_records) // 2
        left_records = all_records[:mid_point]
        right_records = all_records[mid_point:]

        left_page = Page(left_records)
        self._write_page(file, page_num, left_page)

        file.seek(0, 2)
        new_page_num = (file.tell() - self.DATA_START_OFFSET) // Page.SIZE_OF_PAGE
        right_page = Page(right_records)
        file.write(right_page.pack())

        separator_key = right_records[0].id_venta

        # Actualizar índice: 1) nueva entrada para página derecha, 2) actualizar entrada de página izquierda
        self._update_leaf_index_after_split(separator_key, new_page_num, page_num, left_records[0].id_venta, leaf_page_num)
        self.next_page_number = new_page_num + 1

    def _split_leaf_index_strategy(self, file, page_num, page, new_record, leaf_page_num):

        all_records = page.records + [new_record]
        all_records.sort(key=lambda r: r.id_venta)
        
        mid_point = len(all_records) // 2
        left_records = all_records[:mid_point]
        right_records = all_records[mid_point:]
        
        left_page = Page(left_records)
        self._write_page(file, page_num, left_page)
        
        file.seek(0, 2)
        new_data_page_num = (file.tell() - self.DATA_START_OFFSET) // Page.SIZE_OF_PAGE
        right_page = Page(right_records)
        file.write(right_page.pack())
        self.next_page_number = new_data_page_num + 1
        
        separator_key = right_records[0].id_venta
        
        with open(self.leaf_index_file, "r+b") as leaf_index_file:
            current_leaf_index = self._read_leaf_index(leaf_index_file, leaf_page_num)
            
            new_entry = LeafIndexEntry(separator_key, new_data_page_num)
            current_leaf_index.insert_sorted(new_entry)
            
            if len(current_leaf_index.entries) > LEAF_INDEX_BLOCK_FACTOR:
                self._split_leaf_index_page(leaf_index_file, leaf_page_num, current_leaf_index)
            else:
                self._write_leaf_index(leaf_index_file, leaf_page_num, current_leaf_index)

    def _split_leaf_index_page(self, leaf_index_file, leaf_page_num, overloaded_leaf_index):
        mid_point = len(overloaded_leaf_index.entries) // 2
        left_entries = overloaded_leaf_index.entries[:mid_point]
        right_entries = overloaded_leaf_index.entries[mid_point:]

        # Calcular nueva página antes de escribir
        leaf_index_file.seek(0, 2)
        new_leaf_page_num = leaf_index_file.tell() // LeafIndex.SIZE_OF_LEAF_INDEX

        # El lado izquierdo apunta al nuevo lado derecho
        left_leaf_index = LeafIndex(left_entries, new_leaf_page_num)
        self._write_leaf_index(leaf_index_file, leaf_page_num, left_leaf_index)

        # El lado derecho mantiene el next_page original
        right_leaf_index = LeafIndex(right_entries, overloaded_leaf_index.next_page)
        self._write_leaf_index(leaf_index_file, new_leaf_page_num, right_leaf_index)

        self.next_leaf_index_page_number = new_leaf_page_num + 1
        
        separator_key = right_entries[0].key
        self._update_root_index_with_new_page(separator_key, new_leaf_page_num)

    def _overflow_page_strategy(self, file, page_num, original_page, new_record):
        page_num_found, page_found, need_new_page = self._find_available_or_last_page_in_chain(file, page_num)
        
        if not need_new_page:
            page_found.insert_sorted(new_record)
            self._write_page(file, page_num_found, page_found)
        else:
            free_page_num = self.free_list_stack.pop_free_page()
            if free_page_num is not None:
                new_overflow_page_num = free_page_num
            else:
                file.seek(0, 2)
                new_overflow_page_num = (file.tell() - self.DATA_START_OFFSET) // Page.SIZE_OF_PAGE
                self.next_page_number = new_overflow_page_num + 1
            
            new_overflow_page = Page([new_record])
            self._write_page(file, new_overflow_page_num, new_overflow_page)
            
            page_found.next_page = new_overflow_page_num
            self._write_page(file, page_num_found, page_found)

    def _find_available_or_last_page_in_chain(self, file, start_page_num):
        current_page_num = start_page_num
        
        while current_page_num != -1:
            page = self._read_page(file, current_page_num)
            
            if not page.is_full():
                return current_page_num, page, False
            
            if page.next_page == -1:
                return current_page_num, page, True 
            
            current_page_num = page.next_page
        
        return start_page_num, None, True

    def _update_leaf_index_after_split(self, right_key, right_page_num, left_page_num, left_key, leaf_page_num):
        with open(self.leaf_index_file, "r+b") as file:
            leaf_index = self._read_leaf_index(file, leaf_page_num)

            # 1. Encontrar y actualizar la entrada que apunta a la página izquierda
            for entry in leaf_index.entries:
                if entry.data_page_number == left_page_num:
                    entry.key = left_key
                    break

            # 2. Agregar nueva entrada para la página derecha
            new_entry = LeafIndexEntry(right_key, right_page_num)
            leaf_index.insert_sorted(new_entry)

            if len(leaf_index.entries) > LEAF_INDEX_BLOCK_FACTOR:
                self._split_leaf_index_page(file, leaf_page_num, leaf_index)
            else:
                self._write_leaf_index(file, leaf_page_num, leaf_index)

    def _update_leaf_index_with_new_page(self, key, page_num, leaf_page_num):
        with open(self.leaf_index_file, "r+b") as file:
            leaf_index = self._read_leaf_index(file, leaf_page_num)
            new_entry = LeafIndexEntry(key, page_num)
            leaf_index.insert_sorted(new_entry)

            if len(leaf_index.entries) > LEAF_INDEX_BLOCK_FACTOR:
                self._split_leaf_index_page(file, leaf_page_num, leaf_index)
            else:
                self._write_leaf_index(file, leaf_page_num, leaf_index)

    def _update_root_index_with_new_page(self, key, leaf_page_num):
        with open(self.root_index_file, "r+b") as file:
            root_index = self._read_root_index(file, 0)
            new_entry = RootIndexEntry(key, leaf_page_num)
            root_index.insert_sorted(new_entry)
            
            if root_index.is_full():
                print("WARNING: Root index is full.")
            
            self._write_root_index(file, 0, root_index)

    def _delete_from_overflow_chain(self, file, start_page_num, id_venta):
        current_page_num = start_page_num
        
        while current_page_num != -1:
            page = self._read_page(file, current_page_num)
            
            if page.remove_record(id_venta):
                self._write_page(file, current_page_num, page)
                
                if len(page.records) == 0 and self._is_overflow_page(current_page_num):
                    self._remove_page_from_chain(file, start_page_num, current_page_num)
                    self.free_list_stack.push_free_page(current_page_num)
                elif len(page.records) <= CONSOLIDATION_THRESHOLD:
                    self._try_consolidate_page(file, current_page_num)
                
                return True
            
            current_page_num = page.next_page
        
        return False

    def _try_consolidate_page(self, file, page_num):
        page = self._read_page(file, page_num)
        
        if page.next_page != -1:
            next_page_num = page.next_page
            next_page = self._read_page(file, next_page_num)
            
            if page.can_merge_with(next_page):
                page.merge_with(next_page)
                page.next_page = next_page.next_page
                
                self._write_page(file, page_num, page)
                
                if self._is_overflow_page(next_page_num):
                    self.free_list_stack.push_free_page(next_page_num)
                else:
                    empty_page = Page()
                    self._write_page(file, next_page_num, empty_page)

    def _remove_page_from_chain(self, file, start_page_num, page_to_remove):
        if start_page_num == page_to_remove:
            return
        
        current_page_num = start_page_num
        while current_page_num != -1:
            page = self._read_page(file, current_page_num)
            
            if page.next_page == page_to_remove:
                page_to_remove_obj = self._read_page(file, page_to_remove)
                page.next_page = page_to_remove_obj.next_page
                self._write_page(file, current_page_num, page)
                return
            
            current_page_num = page.next_page

    # Operaciones principales
    
    def add(self, record: Record):
        if not os.path.exists(self.filename):
            self._create_initial_files(record)
            return

        target_leaf_page_num = self._find_target_leaf_page(record.id_venta)
        target_data_page_num = self._find_target_data_page(record.id_venta, target_leaf_page_num)

        with open(self.filename, "r+b") as file:
            page = self._read_page(file, target_data_page_num)

            if not page.is_full():
                page.insert_sorted(record)
                self._write_page(file, target_data_page_num, page)
            else:
                self._handle_page_overflow(file, target_data_page_num, page, record, target_leaf_page_num)

    def search(self, id_venta):
        if not os.path.exists(self.filename):
            return None

        target_leaf_page_num = self._find_target_leaf_page(id_venta)
        target_data_page_num = self._find_target_data_page(id_venta, target_leaf_page_num)

        with open(self.filename, "rb") as file:
            current_page_num = target_data_page_num

            while current_page_num != -1:
                page = self._read_page(file, current_page_num)

                for record in page.records:
                    if record.id_venta == id_venta:
                        return record
                    elif record.id_venta > id_venta:
                        return None

                current_page_num = page.next_page if page.next_page != -1 else -1

            return None

    def delete(self, id_venta):
        if not os.path.exists(self.filename):
            return False
        
        target_leaf_page_num = self._find_target_leaf_page(id_venta)
        target_data_page_num = self._find_target_data_page(id_venta, target_leaf_page_num)
        
        with open(self.filename, "r+b") as file:
            page = self._read_page(file, target_data_page_num)
            
            if page.remove_record(id_venta):
                self._write_page(file, target_data_page_num, page)
                
                if len(page.records) <= CONSOLIDATION_THRESHOLD:
                    self._try_consolidate_page(file, target_data_page_num)
                
                return True
            
            return self._delete_from_overflow_chain(file, target_data_page_num, id_venta)

    def range_search(self, begin_key, end_key):
        results = []

        if not os.path.exists(self.filename):
            return results

        # Usar índices para encontrar punto de inicio
        start_leaf_page = self._find_target_leaf_page(begin_key)
        start_data_page = self._find_target_data_page(begin_key, start_leaf_page)

        with open(self.filename, "rb") as file:
            file_size = os.path.getsize(self.filename)
            if file_size < self.DATA_START_OFFSET:
                return results

            visited_pages = set()

            # Comenzar desde la página encontrada por los índices
            current_page_num = start_data_page

            while current_page_num is not None and current_page_num not in visited_pages:
                visited_pages.add(current_page_num)
                page = self._read_page(file, current_page_num)

                for record in page.records:
                    if record.id_venta > end_key:
                        # Si encontramos un registro mayor al final del rango, terminamos
                        return sorted(results, key=lambda r: r.id_venta)
                    if begin_key <= record.id_venta <= end_key:
                        results.append(record)

                # Continuar con overflow pages
                current_page_num = page.next_page if page.next_page != -1 else None

            # Si no encontramos registros en el rango de inicio, buscar en páginas siguientes
            if not results and os.path.exists(self.leaf_index_file):
                self._continue_range_search_in_next_pages(begin_key, end_key, start_leaf_page, visited_pages, results)

        return sorted(results, key=lambda r: r.id_venta)

    def _continue_range_search_in_next_pages(self, begin_key, end_key, start_leaf_page, visited_pages, results):
        with open(self.leaf_index_file, "rb") as leaf_file:
            file_size = os.path.getsize(self.leaf_index_file)
            num_leaf_pages = file_size // LeafIndex.SIZE_OF_LEAF_INDEX

            with open(self.filename, "rb") as data_file:
                # Buscar en las siguientes páginas de datos desde el leaf index actual
                for leaf_page_num in range(start_leaf_page, num_leaf_pages):
                    leaf_index = self._read_leaf_index(leaf_file, leaf_page_num)

                    for entry in leaf_index.entries:
                        if entry.key > end_key:
                            return  # Ya pasamos el rango

                        if entry.data_page_number not in visited_pages:
                            current_page_num = entry.data_page_number

                            while current_page_num is not None and current_page_num not in visited_pages:
                                visited_pages.add(current_page_num)
                                page = self._read_page(data_file, current_page_num)

                                for record in page.records:
                                    if record.id_venta > end_key:
                                        return
                                    if begin_key <= record.id_venta <= end_key:
                                        results.append(record)

                                current_page_num = page.next_page if page.next_page != -1 else None

    # Funciones extras

    def _is_overflow_page(self, page_num):
        if not os.path.exists(self.leaf_index_file):
            return page_num > 0
        
        try:
            with open(self.leaf_index_file, "rb") as file:
                file_size = os.path.getsize(self.leaf_index_file)
                num_leaf_pages = file_size // LeafIndex.SIZE_OF_LEAF_INDEX
                
                for i in range(num_leaf_pages):
                    leaf_index = self._read_leaf_index(file, i)
                    for entry in leaf_index.entries:
                        if entry.data_page_number == page_num:
                            return False
                return True
        except:
            return page_num > 0

    def validate_index_consistency(self):
        errors = []
        files_to_check = [
            (self.filename, "Archivo de datos"),
            (self.leaf_index_file, "Archivo de índice Leaf"),
            (self.root_index_file, "Archivo de índice Root")
        ]
        
        for file_path, file_desc in files_to_check:
            if not os.path.exists(file_path):
                errors.append(f"{file_desc} no existe")
        
        if errors:
            return errors
        
        try:

            with open(self.root_index_file, "rb") as root_file:
                root_index = self._read_root_index(root_file, 0)
                
                with open(self.leaf_index_file, "rb") as leaf_file:
                    leaf_file_size = os.path.getsize(self.leaf_index_file)
                    num_leaf_pages = leaf_file_size // LeafIndex.SIZE_OF_LEAF_INDEX
                    

                    for root_entry in root_index.entries:
                        if root_entry.leaf_page_number >= num_leaf_pages:
                            errors.append(f"Root apunta a página Leaf inexistente: {root_entry.leaf_page_number}")
                    

                    with open(self.filename, "rb") as data_file:
                        data_file_size = os.path.getsize(self.filename)
                        num_data_pages = (data_file_size - self.DATA_START_OFFSET) // Page.SIZE_OF_PAGE
                        
                        for i in range(num_leaf_pages):
                            leaf_index = self._read_leaf_index(leaf_file, i)
                            for leaf_entry in leaf_index.entries:
                                if leaf_entry.data_page_number >= num_data_pages:
                                    errors.append(f"Leaf apunta a página de datos inexistente: {leaf_entry.data_page_number}")
            

                    
        except Exception as e:
            errors.append(f"Error durante validación: {str(e)}")
        
        return errors

    def show_structure(self):
        print("=== ESTRUCTURA DEL ISAM DE DOS NIVELES ===")
        
        free_count = self.free_list_stack.get_free_count()
        print(f"\n--- FREE LIST ---")
        print(f"  Páginas libres: {free_count}")
        
        print("\n--- ROOT INDEX ---")
        if os.path.exists(self.root_index_file):
            with open(self.root_index_file, "rb") as file:
                root_index = self._read_root_index(file, 0)
                if root_index.entries:
                    for entry in root_index.entries:
                        print(f"  {entry}")
                else:
                    print("  (vacío - apunta por defecto a Leaf Page 0)")
        else:
            print("  (no existe)")
        
        print("\n--- LEAF INDEX ---")
        if os.path.exists(self.leaf_index_file):
            with open(self.leaf_index_file, "rb") as file:
                file_size = os.path.getsize(self.leaf_index_file)
                num_leaf_pages = file_size // LeafIndex.SIZE_OF_LEAF_INDEX
                
                for i in range(num_leaf_pages):
                    leaf_index = self._read_leaf_index(file, i)
                    print(f"  Leaf Page {i}:")
                    if leaf_index.entries:
                        for entry in leaf_index.entries:
                            print(f"    {entry}")
                    else:
                        print("    (empty)")
        else:
            print("  (no existe)")

    def scanAll(self):
        print("\n--- DATA PAGES ---")
        if not os.path.exists(self.filename):
            print("  (no existe)")
            return
            
        with open(self.filename, "rb") as file:
            file_size = os.path.getsize(self.filename)
            if file_size < self.DATA_START_OFFSET:
                print("  (archivo vacío)")
                return
                
            num_pages = (file_size - self.DATA_START_OFFSET) // Page.SIZE_OF_PAGE
            visited = set()
            
            for i in range(num_pages):
                if i in visited:
                    continue
                    
                current_page_num = i
                while current_page_num is not None and current_page_num not in visited:
                    visited.add(current_page_num)
                    page = self._read_page(file, current_page_num)
                    
                    ids = [record.id_venta for record in page.records]
                    next_page_info = f"next_page: {page.next_page}" if page.next_page != -1 else "next_page: None"
                    overflow_info = " (overflow)" if self._is_overflow_page(current_page_num) else " (main)"
                    print(f"  Página {current_page_num}: IDs {ids}, {next_page_info}{overflow_info}")
                    
                    current_page_num = page.next_page if page.next_page != -1 else None



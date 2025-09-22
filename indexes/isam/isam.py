import os, struct

BLOCK_FACTOR = 4
ROOT_INDEX_BLOCK_FACTOR = 3
LEAF_INDEX_BLOCK_FACTOR = 5

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


class IndexPage:
    FORMAT = "ii"
    SIZE = struct.calcsize(FORMAT)
    
    def __init__(self, key: int, page_number: int):
        self.key = key
        self.page_number = page_number
    
    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.page_number)
    
    @staticmethod
    def unpack(data: bytes):
        key, page_number = struct.unpack(IndexPage.FORMAT, data)
        return IndexPage(key, page_number)
    
    def __str__(self):
        return f"Key: {self.key} -> Page: {self.page_number}"


class IndexFile:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_INDEX_FILE = HEADER_SIZE + INDEX_BLOCK_FACTOR * IndexPage.SIZE

    def __init__(self, entries=None, next_page=-1):
        self.entries = entries if entries else []
        self.next_page = next_page

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.entries), self.next_page)
        entries_data = b''.join(entry.pack() for entry in self.entries)
        entries_data += b'\x00' * (IndexPage.SIZE * (INDEX_BLOCK_FACTOR - len(self.entries)))
        return header_data + entries_data

    @staticmethod
    def unpack(data: bytes):
        size, next_page = struct.unpack(IndexFile.HEADER_FORMAT, data[:IndexFile.HEADER_SIZE])
        offset = IndexFile.HEADER_SIZE
        entries = []
        for _ in range(size):
            entry_data = data[offset: offset + IndexPage.SIZE]
            entries.append(IndexPage.unpack(entry_data))
            offset += IndexPage.SIZE
        return IndexFile(entries, next_page)

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
        return len(self.entries) >= INDEX_BLOCK_FACTOR

    # Encuentra la target page en donde debería estar cierta key
    def find_page_for_key(self, key):
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
                result_page = self.entries[mid].page_number
                left = mid + 1
            
        return result_page


class ISAMFile:
    HEADER_FORMAT = 'i'  # contador de páginas libres
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    MAX_FREE_PAGES = 5  # máximo número de páginas libres a guardar
    FREE_LIST_SIZE = MAX_FREE_PAGES * 4
    # vamos a tener que tener encuenta siempre el offset que genera la free list y su contador
    DATA_START_OFFSET = HEADER_SIZE + FREE_LIST_SIZE
    
    def __init__(self, filename="datos.dat", indexfile="index.dat"):
        self.filename = filename
        self.indexfile = indexfile
        self.next_page_number = 0

    def _push_free_page(self, file, page_num):
        free_count = self._read_free_count(file)
        
        if free_count >= self.MAX_FREE_PAGES:
            return False  
        
        free_pos = self.HEADER_SIZE + free_count * 4
        file.seek(free_pos)
        file.write(struct.pack('i', page_num))
        
        file.seek(0)
        file.write(struct.pack(self.HEADER_FORMAT, free_count + 1))
        
        return True
    
    def _pop_free_page(self, file):
        free_count = self._read_free_count(file)
        if free_count == 0:
            return None
        
        last_pos = self.HEADER_SIZE + (free_count - 1) * 4
        file.seek(last_pos)
        page_num = struct.unpack('i', file.read(4))[0]
        
        file.seek(last_pos)
        file.write(b'\x00\x00\x00\x00')
        file.seek(0)
        file.write(struct.pack(self.HEADER_FORMAT, free_count - 1))
        
        return page_num
    
    def _read_free_count(self, file):
        file.seek(0)
        return struct.unpack(self.HEADER_FORMAT, file.read(self.HEADER_SIZE))[0]

    def _is_overflow_page(self, page_num):
        if not os.path.exists(self.indexfile):
            return page_num > 0
        
        with open(self.indexfile, "rb") as file:
            index_file = self._read_index_file(file, 0)
            for entry in index_file.entries:
                if entry.page_number == page_num:
                    return False
            return True

    # inserta un nuevo registro en la página correcta del archivo
    def add(self, record: Record):
        if not os.path.exists(self.filename):
            self._create_initial_files(record)
            return

        target_page_num = self._find_target_page(record.id_venta)
        
        with open(self.filename, "r+b") as file:
            page = self._read_page(file, target_page_num)
            
            if not page.is_full():
                page.insert_sorted(record)
                self._write_page(file, target_page_num, page)
            else:
                # maneja el overflow al momento de querer insertar un nuevo registro cuando la page donde tiene que estar está llena
                self._handle_page_overflow(file, target_page_num, page, record)

    # creamos los archivos de datos e índice cuando no existen todavía
    def _create_initial_files(self, record):
        with open(self.filename, "wb") as file:
            file.write(struct.pack(self.HEADER_FORMAT, 0))
            file.write(b'\x00' * self.FREE_LIST_SIZE)
            page = Page([record])
            file.write(page.pack())
        
        with open(self.indexfile, "wb") as file:
            index_file = IndexFile([])
            file.write(index_file.pack())
        
        self.next_page_number = 1

    # decide cómo manejar cuando una página está llena y llega un nuevo registro
    def _handle_page_overflow(self, file, page_num, page, new_record):
        with open(self.indexfile, "rb") as index_file:
            index_file_obj = self._read_index_file(index_file, 0)
            
            if not index_file_obj.is_full():
                # Opción 1 : divide la página e inserta nueva key al indexfile porque todavía hay espacio
                self._split_page_strategy(file, page_num, page, new_record)
            else:
                # Opción 2: una vez se llene el indexfile, se empieza a utilizar la técnica de encadenamiento de pages usando el atributo next_page 
                self._overflow_page_strategy(file, page_num, page, new_record)

    # divide una página llena en dos y actualiza el índice
    def _split_page_strategy(self, file, page_num, page, new_record):
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
        self._update_index_with_new_page(separator_key, new_page_num)
        self.next_page_number = new_page_num + 1

    # crea páginas adicionales encadenadas cuando el índice está lleno
    def _overflow_page_strategy(self, file, page_num, original_page, new_record):
        page_num_found, page_found, need_new_page = self._find_available_or_last_page_in_chain(file, page_num)
        
        if not need_new_page:
            page_found.insert_sorted(new_record)
            self._write_page(file, page_num_found, page_found)
        else:
            # intentamos usar una página libre primero
            free_page_num = self._pop_free_page(file)
            if free_page_num is not None:
                new_overflow_page_num = free_page_num
            else:
                # si no hay páginas libres, creamos una nueva al final del archivo
                file.seek(0, 2)
                new_overflow_page_num = (file.tell() - self.DATA_START_OFFSET) // Page.SIZE_OF_PAGE
                self.next_page_number = new_overflow_page_num + 1
            
            new_overflow_page = Page([new_record])
            self._write_page(file, new_overflow_page_num, new_overflow_page)
            
            page_found.next_page = new_overflow_page_num
            self._write_page(file, page_num_found, page_found)

    # encuentra la primera página con espacio o la última si todas están llenas
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

    # agrega una nueva entrada al indexfile manteniéndolo ordenado
    def _update_index_with_new_page(self, key, page_num):
        with open(self.indexfile, "r+b") as file:
            index_file = self._read_index_file(file, 0)
            new_entry = IndexPage(key, page_num)
            
            index_file.insert_sorted(new_entry)
            self._write_index_file(file, 0, index_file)

    # busca en qué página debería estar un registro según su id
    def _find_target_page(self, id_venta):
        if not os.path.exists(self.indexfile):
            return 0
        
        with open(self.indexfile, "rb") as file:
            index_file = self._read_index_file(file, 0)
            return index_file.find_page_for_key(id_venta)

    def _read_page(self, file, page_num):
        offset = self.DATA_START_OFFSET + (page_num * Page.SIZE_OF_PAGE)
        file.seek(offset)
        return Page.unpack(file.read(Page.SIZE_OF_PAGE))

    def _write_page(self, file, page_num, page):
        offset = self.DATA_START_OFFSET + (page_num * Page.SIZE_OF_PAGE)
        file.seek(offset)
        file.write(page.pack())

    def _read_index_file(self, file, page_num):
        file.seek(page_num * IndexFile.SIZE_OF_INDEX_FILE)
        return IndexFile.unpack(file.read(IndexFile.SIZE_OF_INDEX_FILE))

    def _write_index_file(self, file, page_num, index_file):
        file.seek(page_num * IndexFile.SIZE_OF_INDEX_FILE)
        file.write(index_file.pack())

    def _add_free_page(self, page_num):
        with open(self.freelist_file, "ab") as file:
            file.write(struct.pack('i', page_num))
    
    def _get_free_page(self):
        if not os.path.exists(self.freelist_file):
            return None
        
        with open(self.freelist_file, "rb") as file:
            file.seek(0, 2)  # ir al final
            file_size = file.tell()
            if file_size < 4:
                return None
            
            file.seek(file_size - 4)
            page_num = struct.unpack('i', file.read(4))[0]
        
        with open(self.freelist_file, "r+b") as file:
            file.truncate(file_size - 4)
        
        return page_num

    def _is_overflow_page(self, page_num):
        if not os.path.exists(self.indexfile):
            return page_num > 0
        
        with open(self.indexfile, "rb") as file:
            index_file = self._read_index_file(file, 0)
            for entry in index_file.entries:
                if entry.page_number == page_num:
                    return False
            return True

    # búsqueda binaria para encontrar la target page en el indexfile y también para encontrar el lugar exacto del registro dentro de la target page
    def search(self, id_venta):
        if not os.path.exists(self.filename):
            return None
        
        target_page_num = self._find_target_page(id_venta)
        
        with open(self.filename, "rb") as file:
            current_page_num = target_page_num
            
            while current_page_num != -1:
                page = self._read_page(file, current_page_num)
                
                left, right = 0, len(page.records) - 1
                while left <= right:
                    mid = (left + right) // 2
                    if page.records[mid].id_venta == id_venta:
                        return page.records[mid]  
                    elif page.records[mid].id_venta < id_venta:
                        left = mid + 1
                    else:
                        right = mid - 1
                
                current_page_num = page.next_page if page.next_page != -1 else -1
            
            return None  

    # muestra la estructura del indexfile
    def show_index_structure(self):
        if os.path.exists(self.indexfile):
            with open(self.indexfile, "rb") as file:
                index_file = self._read_index_file(file, 0)
                
                if index_file.entries:
                    
                    header_physical = "|P0|"
                    for i, entry in enumerate(index_file.entries):
                        header_physical += f"K{entry.key}|P{entry.page_number}|"
                    print(f"{header_physical}")
                    
                else:
                    print("|P0| (índice vacío - solo página 0)")

    # muestra todos los registros y pages
    def scanAll(self):
        if not os.path.exists(self.filename):
            return
            
        with open(self.filename, "rb") as file:
            num_pages = os.path.getsize(self.filename) // Page.SIZE_OF_PAGE
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
                    print(f"Página {current_page_num}: IDs {ids}, {next_page_info}")
                    
                    current_page_num = page.next_page if page.next_page != -1 else None

    # elimina un registro reorganizando toda la cadena de páginas
    def delete(self, id_venta):
        if not os.path.exists(self.filename):
            return False
        
        target_page_num = self._find_target_page(id_venta)
        
        with open(self.filename, "r+b") as file:
            main_page = self._read_page(file, target_page_num)
            
            found_in_main = any(r.id_venta == id_venta for r in main_page.records)
            
            if found_in_main:
                return self._delete_from_page_chain(file, target_page_num, id_venta)
            
            return self._delete_from_overflow_chain(file, target_page_num, id_venta)

    # elimina registros de toda una cadena de páginas enlazadas
    def _delete_from_page_chain(self, file, start_page_num, id_venta):
        pages_to_process = []
        current_page_num = start_page_num
        
        while current_page_num != -1:
            page = self._read_page(file, current_page_num)
            pages_to_process.append((current_page_num, page))
            current_page_num = page.next_page
        
        deleted_count = 0
        all_records = []
        
        for page_num, page in pages_to_process:
            for record in page.records:
                if record.id_venta != id_venta:
                    all_records.append(record)
                else:
                    deleted_count += 1
        
        if deleted_count == 0:
            return False
        
        self._reorganize_pages(file, pages_to_process, all_records)
        return True
    
    # redistribuye los registros restantes en las páginas disponibles
    def _reorganize_pages(self, file, original_pages, remaining_records):
        if not remaining_records:
            for page_num, _ in original_pages:
                empty_page = Page()
                self._write_page(file, page_num, empty_page)
            return
        
        remaining_records.sort(key=lambda r: r.id_venta)
        
        page_index = 0
        record_index = 0
        
        while record_index < len(remaining_records) and page_index < len(original_pages):
            page_num, _ = original_pages[page_index]
            
            page_records = []
            while (record_index < len(remaining_records) and 
                   len(page_records) < BLOCK_FACTOR):
                page_records.append(remaining_records[record_index])
                record_index += 1
            
            next_page = -1
            if (record_index < len(remaining_records) and 
                page_index + 1 < len(original_pages)):
                next_page = original_pages[page_index + 1][0]
            
            new_page = Page(page_records, next_page)
            self._write_page(file, page_num, new_page)
            page_index += 1
        
        while page_index < len(original_pages):
            page_num, _ = original_pages[page_index]
            
            # si es una página de overflow (no está en el índice), la mandamos a la free list
            if self._is_overflow_page(page_num):
                self._push_free_page(file, page_num)
            else:
                # si es una página principal, la dejamos vacía porque luego puede usarse para nuevos registros
                empty_page = Page()
                self._write_page(file, page_num, empty_page)
            
            page_index += 1

    # busca y elimina un registro en páginas de overflow enlazadas
    def _delete_from_overflow_chain(self, file, start_page_num, id_venta):
        current_page_num = start_page_num
        
        while current_page_num != -1:
            page = self._read_page(file, current_page_num)
            
            if page.remove_record(id_venta):
                self._write_page(file, current_page_num, page)
                
                # si una pagina overflow queda vacia entonces la mandamos a la free list
                if len(page.records) == 0 and self._is_overflow_page(current_page_num):
                    self._remove_page_from_chain(file, start_page_num, current_page_num)
                    self._push_free_page(file, current_page_num)
                # si aun no esta vacia pero tiene pocos registros entonces intentamos consolidarla con la siguiente
                elif len(page.records) <= BLOCK_FACTOR // 3:
                    self._try_consolidate_page(file, current_page_num)
                
                return True
            
            current_page_num = page.next_page
        
        return False
    
    # desconectamos una página de overflow vacía su cadena
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

    # elimina un registro y reorganiza las páginas si quedan muy vacías
    def delete_optimized(self, id_venta):
        if not os.path.exists(self.filename):
            return False
        
        target_page_num = self._find_target_page(id_venta)
        
        with open(self.filename, "r+b") as file:
            page = self._read_page(file, target_page_num)
            
            if page.remove_record(id_venta):
                self._write_page(file, target_page_num, page)
                
                if len(page.records) <= BLOCK_FACTOR // 3:
                    self._try_consolidate_page(file, target_page_num)
                
                return True
            
            return self._delete_from_overflow_chain(file, target_page_num, id_venta)
    
    # junta páginas que tienen pocos registros para aprovechar mejor el espacio
    def _try_consolidate_page(self, file, page_num):
        page = self._read_page(file, page_num)
        
        if page.next_page != -1:
            next_page_num = page.next_page
            next_page = self._read_page(file, next_page_num)
            
            if page.can_merge_with(next_page):
                page.merge_with(next_page)
                page.next_page = next_page.next_page
                
                self._write_page(file, page_num, page)
                
                # si la page que se eliminó era de overflow, la mandamos a la free list
                if self._is_overflow_page(next_page_num):
                    self._push_free_page(file, next_page_num)
                # si era una page principal, la dejamos vacía
                else:
                    empty_page = Page()
                    self._write_page(file, next_page_num, empty_page)

def load_csv_data(filename):
    records = []
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines[1:]:
            parts = line.strip().split(';')
            if len(parts) == 5:
                id_venta = int(parts[0])
                nombre_producto = parts[1]
                cantidad_vendida = int(parts[2])
                precio_unitario = float(parts[3])
                fecha_venta = parts[4]
                records.append(Record(id_venta, nombre_producto, cantidad_vendida, precio_unitario, fecha_venta))
    return records


if __name__ == "__main__":
    for file in ["datos.dat", "index.dat"]:
        if os.path.exists(file):
            os.remove(file)

    isam = ISAMFile()
    records = load_csv_data("sales_dataset_unsorted.csv")[:30]

    for record in records:
        isam.add(record)

    print("INDEX FILE:")
    isam.show_index_structure()
    
    print("\nDATOS")
    isam.scanAll()
    
    test_id = records[5].id_venta
    print("\nOPERACIONES")

    overflow_ids = [747, 801, 854, 871]
    for id_del in overflow_ids:
        isam.delete_optimized(id_del)
        print(f"Deleted {id_del}")
    
    # insertar nuevos registros 
    new_records = [
        Record(101, "ProductoA", 5, 25.50, "2023-11-01"),
        Record(102, "ProductoB", 3, 15.75, "2023-11-02"),
        Record(103, "ProductoC", 8, 45.25, "2023-11-03"),
    ]
    
    for record in new_records:
        isam.add(record)
        print(f"Inserted {record.id_venta}")
    
    print("\nse reuso la page 8 que fue liberada al eliminar esos 4 registros y luego insertar esos otros 3")
    print("\nDATOS")
    isam.scanAll()
    
    print("\nBUSQUEDAS")
    test_keys = [101, 102, 103, 747, 801]
    for key in test_keys:
        found = isam.search(key) is not None
        print(f"Search {key}: {'Found' if found else 'Not found'}")

    print("\nELIMINACIONES EXTRAS")
    more_deletions = [403,449]
    for id_del in more_deletions:
        isam.delete_optimized(id_del)
        print(f"Deleted {id_del}")
    
    print("\nla page 2 queda vacia pero no se manda a free list porque es una page principal, \nademás se mantiene el index")
    print("\nDATOS")
    isam.scanAll()
    print("\nINDEX FILE:")
    isam.show_index_structure()
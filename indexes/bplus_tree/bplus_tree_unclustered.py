from typing import Any, List, Optional
import bisect
import pickle
import os
from ..core.performance_tracker import PerformanceTracker, OperationResult
from ..core.record import IndexRecord 


class Node:
    def __init__(self, is_leaf: bool=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.parent_id = None
        self.page_id = None
    
    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) > max_keys
    
    def is_underflow(self, min_keys: int) -> bool:
        return len(self.keys) < min_keys


class LeafNode(Node):
    def __init__(self):
        super().__init__(is_leaf=True)
        self.values = [] 
        self.previous_id = None
        self.next_id = None


class InternalNode(Node):
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children_ids = []


class PrimaryKeyPointer:
    def __init__(self, primary_key: Any):
        self.primary_key = primary_key

    def __str__(self):
        return f"PK({self.primary_key})"

    def __repr__(self):
        return self.__str__()
    
    def __eq__(self, other):
        return (isinstance(other, PrimaryKeyPointer) and 
                self.primary_key == other.primary_key)
    
    def __hash__(self):
        return hash(self.primary_key)


class BPlusTreeUnclusteredIndex:

    def __init__(self, order: int, index_column: str, file_path: str):
        self.index_column = index_column
        self.order = order
        self.max_keys = order - 1
        self.min_keys = (order + 1) // 2 - 1
        self.key_field = index_column
        self.file_path = file_path + ".dat"
        self.page_size = 8192

        # --- NORMALIZACIÓN DE CLAVES: evita bytes vs str ---
        self._norm = lambda k: (k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else k)

        self.performance = PerformanceTracker()

        if not os.path.exists(self.file_path):
            # Crear archivo nuevo
            self.next_page_id = 2  # Página 1 será la raíz
            self.root_page_id = 1
            self._initialize_file()
        else:
            # Leer metadata existente
            self._read_metadata()

    def _initialize_file(self):
        """Inicializa archivo nuevo con metadata y raíz"""
        # Crear archivo con página de metadata (primeros 16 bytes)
        with open(self.file_path, 'wb') as f:
            # Escribir metadata inicial
            f.write(self.root_page_id.to_bytes(8, 'little'))
            f.write(self.next_page_id.to_bytes(8, 'little'))

        # Crear raíz inicial (hoja) en página 1
        root = LeafNode()
        root.page_id = 1
        self._write_page(1, root)

    def _write_metadata(self):
        try:
            if not os.path.exists(self.file_path):
                with open(self.file_path, 'wb') as f:
                    f.write(b'\x00' * 16)

            with open(self.file_path, 'r+b') as f:
                f.seek(0)
                f.write(self.root_page_id.to_bytes(8, 'little'))
                f.write(self.next_page_id.to_bytes(8, 'little'))
                f.flush()
        except Exception as e:
            print(f"Error writing metadata: {e}")

    def _read_metadata(self):
        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, 'rb') as f:
                    f.seek(0)
                    root_bytes = f.read(8)
                    next_bytes = f.read(8)
                    if len(root_bytes) == 8 and len(next_bytes) == 8:
                        self.root_page_id = int.from_bytes(root_bytes, 'little')
                        self.next_page_id = int.from_bytes(next_bytes, 'little')
                    else:
                        self.root_page_id = 1
                        self.next_page_id = 2
            else:
                self.root_page_id = 1
                self.next_page_id = 2
        except Exception as e:
            print(f"Error reading metadata: {e}")
            self.root_page_id = 1
            self.next_page_id = 2
    
    def _get_page_offset(self, page_id: int) -> int:
        return 16 + (page_id * self.page_size) 
    
    def _read_page(self, page_id: int) -> Optional[Node]:
        if page_id is None or page_id < 1:  # Página 0 es metadata
            return None

        self.performance.track_read()

        if not os.path.exists(self.file_path):
            return None

        try:
            with open(self.file_path, 'rb') as f:
                offset = self._get_page_offset(page_id)
                f.seek(offset)
                page_data = f.read(self.page_size)

                if len(page_data) < self.page_size:
                    return None

                # Verificar si la página está vacía
                if page_data == b'\x00' * self.page_size:
                    return None

                node = pickle.loads(page_data.rstrip(b'\x00'))
                node.page_id = page_id

                # --- normaliza claves al cargar ---
                if isinstance(node.keys, list):
                    node.keys = [self._norm(k) for k in node.keys]

                return node
        except (EOFError, pickle.UnpicklingError, OSError):
            return None

    def _write_page(self, page_id: int, node: Node):
        if page_id < 1:  # No permitir escribir en página 0 (metadata)
            raise Exception("Cannot write to page 0 (metadata page)")

        self.performance.track_write()

        # Asegurar que el directorio existe
        dir_path = os.path.dirname(self.file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        node.page_id = page_id

        # --- normaliza claves antes de serializar ---
        if isinstance(node.keys, list):
            node.keys = [self._norm(k) for k in node.keys]

        serialized = pickle.dumps(node)

        if len(serialized) > self.page_size:
            raise Exception(f"Page {page_id} too large: {len(serialized)} > {self.page_size}")

        padded_data = serialized + b'\x00' * (self.page_size - len(serialized))

        # Asegurar que el archivo existe con metadata
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'wb') as f:
                f.write(b'\x00' * 16)  # Metadata vacía

        # Calcular tamaño necesario del archivo
        offset = self._get_page_offset(page_id)
        required_size = offset + self.page_size

        # Extender archivo si es necesario
        current_size = os.path.getsize(self.file_path)
        if current_size < required_size:
            with open(self.file_path, 'ab') as f:
                f.write(b'\x00' * (required_size - current_size))

        # Escribir página
        with open(self.file_path, 'r+b') as f:
            f.seek(offset)
            f.write(padded_data)
            f.flush()
    
    def _allocate_page_id(self) -> int:
        page_id = self.next_page_id
        self.next_page_id += 1
        self._write_metadata()
        return page_id

    def search(self, key: Any) -> OperationResult:
        self.performance.start_operation()
        
        key = self._norm(key)
        self._read_metadata()
        leaf_node = self._find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        
        primary_keys = []
        while pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            primary_keys.append(leaf_node.values[pos].primary_key)
            pos += 1

        return self.performance.end_operation(primary_keys)

    def insert(self, index_record : IndexRecord) -> OperationResult:
        self.performance.start_operation()

        secondary_key = self._norm(index_record.index_value)
        primary_key = index_record.primary_key
        primary_key_pointer = PrimaryKeyPointer(primary_key)

        self._read_metadata()
        root = self._read_page(self.root_page_id)
        self._insert_recursive(root, secondary_key, primary_key_pointer)

        return self.performance.end_operation(True)

    def _insert_recursive(self, node: Node, key: Any, primary_key_pointer: PrimaryKeyPointer):
        key = self._norm(key)
        if isinstance(node, LeafNode):
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.values.insert(pos, primary_key_pointer)
            
            self._write_page(node.page_id, node)
            
            if node.is_full(self.max_keys):
                self._split_leaf(node)
        else:
            pos = bisect.bisect_right(node.keys, key)
            child_id = node.children_ids[pos]
            child = self._read_page(child_id)
            self._insert_recursive(child, key, primary_key_pointer)

    def delete(self, secondary_key: Any, primary_key: Any = None) -> OperationResult:
        self.performance.start_operation()

        secondary_key = self._norm(secondary_key)
        if primary_key is not None:
            result = self._delete_by_keys(secondary_key, primary_key)
            return self.performance.end_operation(result)
        else:
            result = self._delete_all_by_secondary_key(secondary_key)
            return self.performance.end_operation(result)

    def _delete_by_keys(self, secondary_key: Any, primary_key: Any) -> bool:
        self._read_metadata()
        leaf = self._find_leaf_node(secondary_key)

        for i, (key, value) in enumerate(zip(leaf.keys, leaf.values)):
            if key == secondary_key and value.primary_key == primary_key:
                leaf.keys.pop(i)
                leaf.values.pop(i)
                self._write_page(leaf.page_id, leaf)

                if leaf.page_id != self.root_page_id and leaf.is_underflow(self.min_keys):
                    self._handle_leaf_underflow(leaf)

                # Verificar si la raíz necesita reducirse
                root = self._read_page(self.root_page_id)
                if isinstance(root, InternalNode) and len(root.keys) == 0:
                    if len(root.children_ids) > 0:
                        self.root_page_id = root.children_ids[0]
                        new_root = self._read_page(self.root_page_id)
                        new_root.parent_id = None
                        self._write_page(self.root_page_id, new_root)
                        # Persistir cambio de raíz
                        self._write_metadata()

                return True

        return False

    def _delete_all_by_secondary_key(self, secondary_key: Any) -> list:
        self._read_metadata()
        deleted_pks = []
        leaf = self._find_leaf_node(secondary_key)

        indices_to_delete = []
        for i, (key, value) in enumerate(zip(leaf.keys, leaf.values)):
            if key == secondary_key:
                indices_to_delete.append(i)
                deleted_pks.append(value.primary_key)

        for i in reversed(indices_to_delete):
            leaf.keys.pop(i)
            leaf.values.pop(i)

        if indices_to_delete:
            self._write_page(leaf.page_id, leaf)

            if leaf.page_id != self.root_page_id and leaf.is_underflow(self.min_keys):
                self._handle_leaf_underflow(leaf)

            root = self._read_page(self.root_page_id)
            if isinstance(root, InternalNode) and len(root.keys) == 0:
                if len(root.children_ids) > 0:
                    self.root_page_id = root.children_ids[0]
                    new_root = self._read_page(self.root_page_id)
                    new_root.parent_id = None
                    self._write_page(self.root_page_id, new_root)
                    self._write_metadata()

        return deleted_pks

    def range_search(self, start_key: Any, end_key: Any) -> OperationResult:
        self.performance.start_operation()
        
        start_key = self._norm(start_key)
        end_key = self._norm(end_key)

        results = []
        self._read_metadata()
        leaf = self._find_leaf_node(start_key)
        
        pos = bisect.bisect_left(leaf.keys, start_key)
        
        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_key:
                    return self.performance.end_operation(results)
                if leaf.keys[i] >= start_key:
                    results.append(leaf.values[i].primary_key)
            
            if leaf.next_id is not None:
                leaf = self._read_page(leaf.next_id)
            else:
                leaf = None
            pos = 0
        
        return self.performance.end_operation(results)

    def _find_leaf_node(self, key: Any) -> LeafNode:
        key = self._norm(key)
        current = self._read_page(self.root_page_id)
        while isinstance(current, InternalNode):
            pos = bisect.bisect_right(current.keys, key)
            child_id = current.children_ids[pos]
            current = self._read_page(child_id)
        return current

    def _split_leaf(self, leaf: LeafNode):
        mid = len(leaf.keys) // 2
        new_leaf = LeafNode()
        
        new_page_id = self._allocate_page_id()
        new_leaf.page_id = new_page_id
        
        new_leaf.keys = [self._norm(k) for k in leaf.keys[mid:]]
        new_leaf.values = leaf.values[mid:]
        new_leaf.parent_id = leaf.parent_id
        
        new_leaf.next_id = leaf.next_id
        new_leaf.previous_id = leaf.page_id
        
        if leaf.next_id is not None:
            next_leaf = self._read_page(leaf.next_id)
            next_leaf.previous_id = new_leaf.page_id
            self._write_page(leaf.next_id, next_leaf)
        
        leaf.next_id = new_leaf.page_id
        
        leaf.keys = [self._norm(k) for k in leaf.keys[:mid]]
        leaf.values = leaf.values[:mid]
        
        self._write_page(new_leaf.page_id, new_leaf)
        self._write_page(leaf.page_id, leaf)
        
        promote_key = new_leaf.keys[0]
        self._promote_key(leaf, promote_key, new_leaf)

    def _split_internal(self, internal: InternalNode):
        mid = len(internal.keys) // 2
        promote_key = self._norm(internal.keys[mid])
        
        new_internal = InternalNode()
        new_page_id = self._allocate_page_id()
        new_internal.page_id = new_page_id
        
        new_internal.keys = [self._norm(k) for k in internal.keys[mid + 1:]]
        new_internal.children_ids = internal.children_ids[mid + 1:]
        new_internal.parent_id = internal.parent_id
        
        for child_id in new_internal.children_ids:
            child = self._read_page(child_id)
            child.parent_id = new_internal.page_id
            self._write_page(child_id, child)
        
        internal.keys = [self._norm(k) for k in internal.keys[:mid]]
        internal.children_ids = internal.children_ids[:mid + 1]
        
        self._write_page(new_internal.page_id, new_internal)
        self._write_page(internal.page_id, internal)
        
        self._promote_key(internal, promote_key, new_internal)

    def _promote_key(self, left_child: Node, key: Any, right_child: Node):
        key = self._norm(key)
        if left_child.parent_id is None:
            new_root = InternalNode()
            new_root_id = self._allocate_page_id()
            new_root.page_id = new_root_id
            new_root.keys = [key]
            new_root.children_ids = [left_child.page_id, right_child.page_id]
            new_root.parent_id = None
            
            left_child.parent_id = new_root_id
            right_child.parent_id = new_root_id
            
            self._write_page(new_root_id, new_root)
            self._write_page(left_child.page_id, left_child)
            self._write_page(right_child.page_id, right_child)
            
            self.root_page_id = new_root_id
            self._write_metadata()
        else:
            parent = self._read_page(left_child.parent_id)
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children_ids.insert(pos + 1, right_child.page_id)
            right_child.parent_id = parent.page_id
            
            self._write_page(parent.page_id, parent)
            self._write_page(right_child.page_id, right_child)

            if parent.is_full(self.max_keys):
                self._split_internal(parent)

    def _handle_leaf_underflow(self, leaf: LeafNode):
        if leaf.parent_id is None:
            return
        
        parent = self._read_page(leaf.parent_id)
        if not parent:
            return
        
        leaf_index = parent.children_ids.index(leaf.page_id)
        
        if leaf_index > 0:
            left_sibling_id = parent.children_ids[leaf_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, LeafNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return
        
        if leaf_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[leaf_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, LeafNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_leaf(leaf, right_sibling, parent, leaf_index)
                return
        
        if leaf_index > 0:
            left_sibling_id = parent.children_ids[leaf_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, LeafNode):
                self._merge_leaf_with_left(leaf, left_sibling, parent, leaf_index)
        else:
            right_sibling_id = parent.children_ids[leaf_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, LeafNode):
                self._merge_leaf_with_right(leaf, right_sibling, parent, leaf_index)

    def _borrow_from_left_leaf(self, leaf: LeafNode, left_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        borrowed_key = left_sibling.keys.pop()
        borrowed_value = left_sibling.values.pop()
        
        leaf.keys.insert(0, borrowed_key)
        leaf.values.insert(0, borrowed_value)
        
        parent.keys[leaf_index - 1] = leaf.keys[0]
        
        self._write_page(leaf.page_id, leaf)
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(parent.page_id, parent)

    def _borrow_from_right_leaf(self, leaf: LeafNode, right_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_value = right_sibling.values.pop(0)

        leaf.keys.append(borrowed_key)
        leaf.values.append(borrowed_value)

        # Actualizar separador: debe ser la primera clave del hijo derecho
        if len(right_sibling.keys) > 0:
            parent.keys[leaf_index] = right_sibling.keys[0]
        else:
            # No debería ocurrir si min_keys > 0
            raise Exception("Right sibling became empty after borrowing")

        self._write_page(leaf.page_id, leaf)
        self._write_page(right_sibling.page_id, right_sibling)
        self._write_page(parent.page_id, parent)

    def _merge_leaf_with_left(self, leaf: LeafNode, left_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        left_sibling.keys.extend(leaf.keys)
        left_sibling.values.extend(leaf.values)
        
        left_sibling.next_id = leaf.next_id
        if leaf.next_id is not None:
            next_leaf = self._read_page(leaf.next_id)
            next_leaf.previous_id = left_sibling.page_id
            self._write_page(leaf.next_id, next_leaf)
        
        parent.children_ids.pop(leaf_index)
        parent.keys.pop(leaf_index - 1)
        
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(parent.page_id, parent)
        
        if parent.page_id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _merge_leaf_with_right(self, leaf: LeafNode, right_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        leaf.keys.extend(right_sibling.keys)
        leaf.values.extend(right_sibling.values)
        
        leaf.next_id = right_sibling.next_id
        if right_sibling.next_id is not None:
            next_leaf = self._read_page(right_sibling.next_id)
            next_leaf.previous_id = leaf.page_id
            self._write_page(right_sibling.next_id, next_leaf)
        
        parent.children_ids.pop(leaf_index + 1)
        parent.keys.pop(leaf_index)
        
        self._write_page(leaf.page_id, leaf)
        self._write_page(parent.page_id, parent)
        
        if parent.page_id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _handle_internal_underflow(self, internal: InternalNode):
        if internal.parent_id is None:
            return
        
        parent = self._read_page(internal.parent_id)
        if not parent:
            return
        
        internal_index = parent.children_ids.index(internal.page_id)
        
        if internal_index > 0:
            left_sibling_id = parent.children_ids[internal_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, InternalNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_internal(internal, left_sibling, parent, internal_index)
                return
        
        if internal_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[internal_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, InternalNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_internal(internal, right_sibling, parent, internal_index)
                return
        
        if internal_index > 0:
            left_sibling_id = parent.children_ids[internal_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, InternalNode):
                self._merge_internal_with_left(internal, left_sibling, parent, internal_index)
        else:
            right_sibling_id = parent.children_ids[internal_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, InternalNode):
                self._merge_internal_with_right(internal, right_sibling, parent, internal_index)

    def _borrow_from_left_internal(self, internal: InternalNode, left_sibling: InternalNode, parent: InternalNode, internal_index: int):
        separator_key = parent.keys[internal_index - 1]
        
        internal.keys.insert(0, separator_key)
        borrowed_child_id = left_sibling.children_ids.pop()
        internal.children_ids.insert(0, borrowed_child_id)
        
        borrowed_child = self._read_page(borrowed_child_id)
        borrowed_child.parent_id = internal.page_id
        self._write_page(borrowed_child_id, borrowed_child)
        
        parent.keys[internal_index - 1] = left_sibling.keys.pop()
        
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(internal.page_id, internal)
        self._write_page(parent.page_id, parent)

    def _borrow_from_right_internal(self, internal: InternalNode, right_sibling: InternalNode, parent: InternalNode, internal_index: int):
        separator_key = parent.keys[internal_index]
        
        internal.keys.append(separator_key)
        borrowed_child_id = right_sibling.children_ids.pop(0)
        internal.children_ids.append(borrowed_child_id)
        
        borrowed_child = self._read_page(borrowed_child_id)
        borrowed_child.parent_id = internal.page_id
        self._write_page(borrowed_child_id, borrowed_child)
        
        parent.keys[internal_index] = right_sibling.keys.pop(0)
        
        self._write_page(right_sibling.page_id, right_sibling)
        self._write_page(internal.page_id, internal)
        self._write_page(parent.page_id, parent)

    def _merge_internal_with_left(self, internal: InternalNode, left_sibling: InternalNode, parent: InternalNode, internal_index: int):
        separator_key = parent.keys[internal_index - 1]
        
        left_sibling.keys.append(separator_key)
        left_sibling.keys.extend(internal.keys)
        left_sibling.children_ids.extend(internal.children_ids)
        
        for child_id in internal.children_ids:
            child = self._read_page(child_id)
            child.parent_id = left_sibling.page_id
            self._write_page(child_id, child)
        
        parent.children_ids.pop(internal_index)
        parent.keys.pop(internal_index - 1)
        
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(parent.page_id, parent)
        
        if parent.page_id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _merge_internal_with_right(self, internal: InternalNode, right_sibling: InternalNode, parent: InternalNode, internal_index: int):
        separator_key = parent.keys[internal_index]
        
        internal.keys.append(separator_key)
        internal.keys.extend(right_sibling.keys)
        internal.children_ids.extend(right_sibling.children_ids)
        
        for child_id in right_sibling.children_ids:
            child = self._read_page(child_id)
            child.parent_id = internal.page_id
            self._write_page(child_id, child)
        
        parent.children_ids.pop(internal_index + 1)
        parent.keys.pop(internal_index)
        
        self._write_page(internal.page_id, internal)
        self._write_page(parent.page_id, parent)
        
        if parent.page_id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def drop_index(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
            return [self.file_path]
        return []

    def clear(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

        # Reiniciar metadata
        self.next_page_id = 2
        self.root_page_id = 1

        # Reinicializar archivo
        self._initialize_file()

from typing import Any, List, Optional, Tuple, Union
import bisect
import pickle
import os
import time
from ..core.performance_tracker import PerformanceTracker


PAGE_SIZE = 4096  # 4KB pages


class Node:
    def __init__(self, is_leaf: bool=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.parent_id = None  # Parent page ID
        self.page_id = None  # Page ID for this node
    
    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) > max_keys
    
    def is_underflow(self, min_keys:int)-> bool:
        return len(self.keys) < min_keys


class LeafNode(Node):
    def __init__(self):
        super().__init__(is_leaf=True)
        self.values = []  # List of PrimaryKeyPointer objects (claves primarias)
        self.previous_id = None  # Previous leaf page ID
        self.next_id = None  # Next leaf page ID


class InternalNode(Node):
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children_ids = []  # Child page IDs




class PrimaryKeyPointer:
    """
    En un índice unclustered, almacenamos la clave primaria del registro
    en lugar de un puntero físico. Luego se usa esta clave para buscar
    en el índice primario (clustered).
    """
    def __init__(self, primary_key: Any):
        self.primary_key = primary_key

    def __str__(self):
        return f"PrimaryKeyPointer(pk={self.primary_key})"

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
        self.max_keys = order - 1  # Maximum keys for node
        self.min_keys = (order + 1) // 2 - 1  # Minimum keys for non-root nodes
        self.key_field = index_column
        self.file_path = file_path
        
        # Performance tracking
        self.performance = PerformanceTracker()
        
        # Disk-based storage
        self.next_page_id = 1
        self.root_page_id = 0
        
        # Initialize file and create root page if needed
        self._initialize_file()
        
    def _initialize_file(self):
        """Initialize the B+ tree file with a root leaf page if it doesn't exist"""
        if not os.path.exists(self.file_path):
            # Create root leaf page
            root = LeafNode()
            root.page_id = 0
            self._write_page(0, root)
            
            # Write metadata (root page ID and next page ID)
            self._write_metadata()
    
    def _write_metadata(self):
        """Write metadata to the beginning of the file"""
        with open(self.file_path, 'r+b') as f:
            # Write at the beginning: root_page_id (8 bytes) + next_page_id (8 bytes)
            f.seek(0)
            f.write(self.root_page_id.to_bytes(8, 'little'))
            f.write(self.next_page_id.to_bytes(8, 'little'))
    
    def _read_metadata(self):
        """Read metadata from the beginning of the file"""
        if os.path.exists(self.file_path):
            with open(self.file_path, 'rb') as f:
                f.seek(0)
                root_bytes = f.read(8)
                next_bytes = f.read(8)
                if len(root_bytes) == 8 and len(next_bytes) == 8:
                    self.root_page_id = int.from_bytes(root_bytes, 'little')
                    self.next_page_id = int.from_bytes(next_bytes, 'little')
    
    def _get_page_offset(self, page_id: int) -> int:
        """Calculate file offset for a page"""
        return 16 + (page_id * PAGE_SIZE)  # 16 bytes for metadata
    
    def _read_page(self, page_id: int) -> Optional[Node]:
        """Read a page from disk"""
        self.performance.track_read()
        
        if not os.path.exists(self.file_path):
            return None
        
        try:
            with open(self.file_path, 'rb') as f:
                offset = self._get_page_offset(page_id)
                f.seek(offset)
                page_data = f.read(PAGE_SIZE)
                
                if len(page_data) < PAGE_SIZE:
                    return None
                
                # Deserialize the page
                node = pickle.loads(page_data)
                node.page_id = page_id
                return node
        except (EOFError, pickle.UnpicklingError, OSError):
            return None
    
    def _write_page(self, page_id: int, node: Node):
        """Write a page to disk"""
        self.performance.track_write()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        
        # Serialize the node
        node.page_id = page_id
        serialized = pickle.dumps(node)
        
        # Pad to page size
        if len(serialized) > PAGE_SIZE:
            # If the page is too large, we need to handle this gracefully
            print(f"Warning: Page {page_id} data too large: {len(serialized)} > {PAGE_SIZE}")
            # For now, just truncate or use a larger temporary size
            # This is a simplification - in a real system we'd need better handling
            try:
                # Try to compress the node by limiting data
                if isinstance(node, LeafNode) and len(node.keys) > 20:
                    # Split if too many keys
                    print(f"Warning: Too many keys in leaf {page_id}: {len(node.keys)}")
                elif isinstance(node, InternalNode) and len(node.keys) > 20:
                    # Split if too many keys
                    print(f"Warning: Too many keys in internal {page_id}: {len(node.keys)}")
                
                # Use original size anyway for demo
                padded_data = serialized + b'\x00' * max(0, (PAGE_SIZE - len(serialized)))
                if len(padded_data) > PAGE_SIZE:
                    padded_data = padded_data[:PAGE_SIZE]
                    
            except Exception as e:
                print(f"Error writing page {page_id}: {e}")
                return
        else:
            padded_data = serialized + b'\x00' * (PAGE_SIZE - len(serialized))
        
        # Write to file
        with open(self.file_path, 'r+b' if os.path.exists(self.file_path) else 'w+b') as f:
            offset = self._get_page_offset(page_id)
            f.seek(offset)
            f.write(padded_data)
    
    def _allocate_page_id(self) -> int:
        """Allocate a new page ID"""
        page_id = self.next_page_id
        self.next_page_id += 1
        self._write_metadata()
        return page_id

    def search(self, key: Any) -> List[PrimaryKeyPointer]:
        """
        Busca por clave secundaria y retorna lista de claves primarias.
        En índices unclustered, múltiples registros pueden tener la misma clave secundaria.
        """
        self._read_metadata()  # Ensure metadata is current
        leaf_node = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        
        primary_keys = []
        # Buscar todas las ocurrencias de la clave (pueden ser múltiples)
        while pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            primary_keys.append(leaf_node.values[pos])
            pos += 1
        
        return primary_keys

    def insert(self, record) -> bool:
        """
        Inserta un registro en el índice unclustered.
        Almacena la clave secundaria -> clave primaria del registro.
        """
        secondary_key = self.get_key_value(record)
        primary_key = record.get_key()  # Obtener la clave primaria del registro
        
        # Crear puntero a la clave primaria
        primary_key_pointer = PrimaryKeyPointer(primary_key)
        
        self._read_metadata()  # Ensure metadata is current
        root = self._read_page(self.root_page_id)
        self.insert_recursive(root, secondary_key, primary_key_pointer)
        return True

    def insert_recursive(self, node: Node, key: Any, primary_key_pointer: PrimaryKeyPointer):
        if isinstance(node, LeafNode):
            # insert in leaf node
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.values.insert(pos, primary_key_pointer)
            
            self._write_page(node.page_id, node)
            
            # split if is full
            if node.is_full(self.max_keys):
                self.split_leaf(node)
        else:
            # find correct child
            pos = bisect.bisect_right(node.keys, key)
            child_id = node.children_ids[pos]
            child = self._read_page(child_id)
            self.insert_recursive(child, key, primary_key_pointer)
            
            # reload node in case it was modified during recursion
            node = self._read_page(node.page_id)
            
            # check if child was split (would have updated parent)
            if len(node.children_ids) > pos and node.children_ids[pos] != child_id:
                return  # Child was split and this node was already updated

    def update(self, old_record, new_record) -> bool:
        """
        Actualiza un registro en el índice unclustered.
        Elimina la entrada antigua e inserta la nueva.
        """
        old_secondary_key = self.get_key_value(old_record)
        old_primary_key = old_record.get_key()
        
        new_secondary_key = self.get_key_value(new_record)
        new_primary_key = new_record.get_key()
        
        # Si las claves son iguales, no hay nada que actualizar en el índice secundario
        if old_secondary_key == new_secondary_key and old_primary_key == new_primary_key:
            return True
        
        # Eliminar la entrada antigua y agregar la nueva
        if self.delete_by_primary_key(old_secondary_key, old_primary_key):
            return self.insert(new_record)
        return False

    def delete(self, key: Any) -> bool:
        self._read_metadata()  # Ensure metadata is current
        leaf = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf.keys, key)
        
        if pos >= len(leaf.keys) or leaf.keys[pos] != key:
            return False  # key does not exist
        
        # remove from leaf
        leaf.keys.pop(pos)
        leaf.values.pop(pos)
        self._write_page(leaf.page_id, leaf)
        
        # check for underflow
        if leaf.page_id != self.root_page_id and leaf.is_underflow(self.min_keys):
            self.handle_leaf_underflow(leaf)
        
        # check if root is empty (only for internal nodes)
        root = self._read_page(self.root_page_id)
        if isinstance(root, InternalNode) and len(root.keys) == 0:
            if len(root.children_ids) > 0:
                # Make the only child the new root
                self.root_page_id = root.children_ids[0]
                new_root = self._read_page(self.root_page_id)
                new_root.parent_id = None
                self._write_page(self.root_page_id, new_root)
                self._write_metadata()
        
        return True

    def delete_by_primary_key(self, secondary_key: Any, primary_key: Any) -> bool:
        """
        Elimina una entrada específica del índice unclustered usando tanto
        la clave secundaria como la clave primaria (para casos de duplicados).
        """
        self._read_metadata()
        leaf = self.find_leaf_node(secondary_key)
        
        # Buscar la entrada específica con ambas claves
        for i, (key, value) in enumerate(zip(leaf.keys, leaf.values)):
            if key == secondary_key and value.primary_key == primary_key:
                # Eliminar esta entrada específica
                leaf.keys.pop(i)
                leaf.values.pop(i)
                self._write_page(leaf.page_id, leaf)
                
                # Manejar underflow si es necesario
                if leaf.page_id != self.root_page_id and leaf.is_underflow(self.min_keys):
                    self.handle_leaf_underflow(leaf)
                
                return True
        
        return False  # No se encontró la entrada específica

    def handle_leaf_underflow(self, leaf: LeafNode):
        if leaf.parent_id is None:
            return
        
        parent = self._read_page(leaf.parent_id)
        if not parent:
            return
        
        # find leaf position in parent
        leaf_index = -1
        for i, child_id in enumerate(parent.children_ids):
            if child_id == leaf.page_id:
                leaf_index = i
                break
        
        if leaf_index == -1:
            return
        
        # try to borrow from left sibling
        if leaf_index > 0:
            left_sibling_id = parent.children_ids[leaf_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, LeafNode) and len(left_sibling.keys) > self.min_keys:
                self.borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return
        
        # try to borrow from right sibling
        if leaf_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[leaf_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, LeafNode) and len(right_sibling.keys) > self.min_keys:
                self.borrow_from_right_leaf(leaf, right_sibling, parent, leaf_index)
                return
        
        # merge if cannot borrow
        if leaf_index > 0:
            # merge with left sibling
            left_sibling_id = parent.children_ids[leaf_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, LeafNode):
                self.merge_leaf_with_left(leaf, left_sibling, parent, leaf_index)
        else:
            # merge with right sibling
            right_sibling_id = parent.children_ids[leaf_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, LeafNode):
                self.merge_leaf_with_right(leaf, right_sibling, parent, leaf_index)

    def borrow_from_left_leaf(self, leaf: LeafNode, left_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        # move from left sibling to leaf
        borrowed_key = left_sibling.keys.pop()
        borrowed_value = left_sibling.values.pop()
        
        leaf.keys.insert(0, borrowed_key)
        leaf.values.insert(0, borrowed_value)
        
        # update parent key
        parent.keys[leaf_index - 1] = leaf.keys[0]
        
        # write updated pages
        self._write_page(leaf.page_id, leaf)
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(parent.page_id, parent)

    def borrow_from_right_leaf(self, leaf: LeafNode, right_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        # move from right sibling to leaf
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_value = right_sibling.values.pop(0)
        
        leaf.keys.append(borrowed_key)
        leaf.values.append(borrowed_value)
        
        # update parent key
        parent.keys[leaf_index] = right_sibling.keys[0] if right_sibling.keys else borrowed_key
        
        # write updated pages
        self._write_page(leaf.page_id, leaf)
        self._write_page(right_sibling.page_id, right_sibling)
        self._write_page(parent.page_id, parent)

    def merge_leaf_with_left(self, leaf: LeafNode, left_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        # move from leaf to left sibling
        left_sibling.keys.extend(leaf.keys)
        left_sibling.values.extend(leaf.values)
        
        # update pointers
        left_sibling.next_id = leaf.next_id
        if leaf.next_id is not None:
            next_leaf = self._read_page(leaf.next_id)
            next_leaf.previous_id = left_sibling.page_id
            self._write_page(leaf.next_id, next_leaf)
        
        # remove leaf from parent
        parent.children_ids.pop(leaf_index)
        parent.keys.pop(leaf_index - 1)
        
        # write updated pages
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(parent.page_id, parent)
        
        # check for parent underflow
        if parent.page_id != self.root_page_id and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def merge_leaf_with_right(self, leaf: LeafNode, right_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        # move from right sibling to leaf
        leaf.keys.extend(right_sibling.keys)
        leaf.values.extend(right_sibling.values)
        
        # update pointers
        leaf.next_id = right_sibling.next_id
        if right_sibling.next_id is not None:
            next_leaf = self._read_page(right_sibling.next_id)
            next_leaf.previous_id = leaf.page_id
            self._write_page(right_sibling.next_id, next_leaf)
        
        # remove right sibling from parent
        parent.children_ids.pop(leaf_index + 1)
        parent.keys.pop(leaf_index)
        
        # write updated pages
        self._write_page(leaf.page_id, leaf)
        self._write_page(parent.page_id, parent)
        
        # check for parent underflow
        if parent.page_id != self.root_page_id and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def handle_internal_underflow(self, internal: InternalNode):
        if internal.parent_id is None:
            return
        
        parent = self._read_page(internal.parent_id)
        if not parent:
            return
        
        # find internal position in parent
        internal_index = -1
        for i, child_id in enumerate(parent.children_ids):
            if child_id == internal.page_id:
                internal_index = i
                break
        
        if internal_index == -1:
            return
        
        # try to borrow from left sibling
        if internal_index > 0:
            left_sibling_id = parent.children_ids[internal_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, InternalNode) and len(left_sibling.keys) > self.min_keys:
                self.borrow_from_left_internal(internal, left_sibling, parent, internal_index)
                return
        
        # try to borrow from right sibling
        if internal_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[internal_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, InternalNode) and len(right_sibling.keys) > self.min_keys:
                self.borrow_from_right_internal(internal, right_sibling, parent, internal_index)
                return
        
        # merge if cannot borrow
        if internal_index > 0:
            # merge with left sibling
            left_sibling_id = parent.children_ids[internal_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, InternalNode):
                self._merge_internal_with_left(internal, left_sibling, parent, internal_index)
        else:
            # merge with right sibling
            right_sibling_id = parent.children_ids[internal_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, InternalNode):
                self._merge_internal_with_right(internal, right_sibling, parent, internal_index)

    def borrow_from_left_internal(self, internal: InternalNode, left_sibling: InternalNode, parent: InternalNode, internal_index: int):
        # separator key from parent
        separator_key = parent.keys[internal_index - 1]
        
        # move last key from left sibling to parent
        parent.keys[internal_index - 1] = left_sibling.keys.pop()
        
        # move last child from left sibling to internal
        borrowed_child_id = left_sibling.children_ids.pop()
        borrowed_child = self._read_page(borrowed_child_id)
        borrowed_child.parent_id = internal.page_id
        self._write_page(borrowed_child_id, borrowed_child)
        
        # insert separator key and borrowed child to internal
        internal.keys.insert(0, separator_key)
        internal.children_ids.insert(0, borrowed_child_id)
        
        # write updated pages
        self._write_page(internal.page_id, internal)
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(parent.page_id, parent)

    def borrow_from_right_internal(self, internal: InternalNode, right_sibling: InternalNode, parent: InternalNode, internal_index: int):
        # separator key from parent
        separator_key = parent.keys[internal_index]
        
        # move the first key from right sibling to parent
        parent.keys[internal_index] = right_sibling.keys.pop(0)
        
        # move the first child from right sibling to internal
        borrowed_child_id = right_sibling.children_ids.pop(0)
        borrowed_child = self._read_page(borrowed_child_id)
        borrowed_child.parent_id = internal.page_id
        self._write_page(borrowed_child_id, borrowed_child)
        
        # add separator key and borrowed child to internal
        internal.keys.append(separator_key)
        internal.children_ids.append(borrowed_child_id)
        
        # write updated pages
        self._write_page(internal.page_id, internal)
        self._write_page(right_sibling.page_id, right_sibling)
        self._write_page(parent.page_id, parent)

    def _merge_internal_with_left(self, internal: InternalNode, left_sibling: InternalNode, parent: InternalNode, internal_index: int):
        # separator key from parent
        separator_key = parent.keys[internal_index - 1]
        
        # merge left_sibling + separator + internal
        left_sibling.keys.append(separator_key)
        left_sibling.keys.extend(internal.keys)
        left_sibling.children_ids.extend(internal.children_ids)
        
        # update parent pointers for moved children
        for child_id in internal.children_ids:
            child = self._read_page(child_id)
            child.parent_id = left_sibling.page_id
            self._write_page(child_id, child)
        
        # remove internal from parent
        parent.children_ids.pop(internal_index)
        parent.keys.pop(internal_index - 1)
        
        # write updated pages
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(parent.page_id, parent)
        
        if parent.page_id != self.root_page_id and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def _merge_internal_with_right(self, internal: InternalNode, right_sibling: InternalNode, parent: InternalNode, internal_index: int):
        # separator key from parent
        separator_key = parent.keys[internal_index]
        
        # merge internal + separator + right_sibling
        internal.keys.append(separator_key)
        internal.keys.extend(right_sibling.keys)
        internal.children_ids.extend(right_sibling.children_ids)
        
        # update parent pointers for moved children
        for child_id in right_sibling.children_ids:
            child = self._read_page(child_id)
            child.parent_id = internal.page_id
            self._write_page(child_id, child)
        
        # remove right sibling from parent
        parent.children_ids.pop(internal_index + 1)
        parent.keys.pop(internal_index)
        
        # write updated pages
        self._write_page(internal.page_id, internal)
        self._write_page(parent.page_id, parent)
        
        if parent.page_id != self.root_page_id and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def split_leaf(self, leaf: LeafNode):
        mid = len(leaf.keys) // 2
        new_leaf = LeafNode()
        
        # allocate new page ID
        new_page_id = self._allocate_page_id()
        new_leaf.page_id = new_page_id
        
        # move half the keys and values to new leaf
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        new_leaf.parent_id = leaf.parent_id
        
        # update pointers
        new_leaf.next_id = leaf.next_id
        new_leaf.previous_id = leaf.page_id
        
        if leaf.next_id is not None:
            next_leaf = self._read_page(leaf.next_id)
            next_leaf.previous_id = new_leaf.page_id
            self._write_page(leaf.next_id, next_leaf)
        
        leaf.next_id = new_leaf.page_id
        
        # keep first half in original leaf
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        
        # write both pages
        self._write_page(new_leaf.page_id, new_leaf)
        self._write_page(leaf.page_id, leaf)
        
        promote_key = new_leaf.keys[0]
        self.promote_key(leaf, promote_key, new_leaf)

    def split_internal(self, internal: InternalNode):
        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]
        
        new_internal = InternalNode()
        
        # allocate new page ID
        new_page_id = self._allocate_page_id()
        new_internal.page_id = new_page_id
        
        # move keys and children to new internal node
        new_internal.keys = internal.keys[mid + 1:]
        new_internal.children_ids = internal.children_ids[mid + 1:]
        new_internal.parent_id = internal.parent_id
        
        # update parent pointers for moved children
        for child_id in new_internal.children_ids:
            child = self._read_page(child_id)
            child.parent_id = new_internal.page_id
            self._write_page(child_id, child)
        
        # keep first half in original internal node
        internal.keys = internal.keys[:mid]
        internal.children_ids = internal.children_ids[:mid + 1]
        
        # write both pages
        self._write_page(new_internal.page_id, new_internal)
        self._write_page(internal.page_id, internal)
        
        self.promote_key(internal, promote_key, new_internal)

    def promote_key(self, left_child: Node, key: Any, right_child: Node):
        if left_child.parent_id is None:
            # create new root
            new_root = InternalNode()
            new_root_id = self._allocate_page_id()
            new_root.page_id = new_root_id
            new_root.keys = [key]
            new_root.children_ids = [left_child.page_id, right_child.page_id]
            new_root.parent_id = None
            
            # update children's parent pointers
            left_child.parent_id = new_root_id
            right_child.parent_id = new_root_id
            
            # update root reference
            self.root_page_id = new_root_id
            
            # write all updated pages
            self._write_page(new_root_id, new_root)
            self._write_page(left_child.page_id, left_child)
            self._write_page(right_child.page_id, right_child)
            self._write_metadata()
            
        else:
            # insert in parent
            parent = self._read_page(left_child.parent_id)
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children_ids.insert(pos + 1, right_child.page_id)
            right_child.parent_id = parent.page_id
            
            # write updated pages
            self._write_page(parent.page_id, parent)
            self._write_page(right_child.page_id, right_child)

            if parent.is_full(self.max_keys):
                self.split_internal(parent)

    def find_leaf_node(self, key: Any) -> LeafNode:
        current = self._read_page(self.root_page_id)
        while isinstance(current, InternalNode):
            pos = bisect.bisect_right(current.keys, key)
            child_id = current.children_ids[pos]
            current = self._read_page(child_id)
        return current

    def get_key_value(self, record) -> Any:
        return record.get_field_value(self.index_column)

    def range_search(self, start_key: Any, end_key: Any) -> List[PrimaryKeyPointer]:
        """
        Búsqueda por rango en el índice unclustered.
        Retorna lista de claves primarias para todos los registros
        cuyas claves secundarias estén en el rango [start_key, end_key].
        """
        results = []
        self._read_metadata()  # Ensure metadata is current
        leaf = self.find_leaf_node(start_key)
        
        pos = bisect.bisect_left(leaf.keys, start_key)
        
        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_key:
                    return results
                if leaf.keys[i] >= start_key:
                    results.append(leaf.values[i])
            
            # move to next leaf
            if leaf.next_id is not None:
                leaf = self._read_page(leaf.next_id)
            else:
                leaf = None
            pos = 0 
        
        return results

    def print_tree(self):
        def print_node(node, level=0):
            indent = "  " * level
            if isinstance(node, LeafNode):
                values_str = [str(v) for v in node.values]
                print(f"{indent}Leaf {node.page_id}: {node.keys} -> {values_str}")
            else:
                print(f"{indent}Internal {node.page_id}: {node.keys}")
                for child_id in node.children_ids:
                    child = self._read_page(child_id)
                    print_node(child, level + 1)
        
        self._read_metadata()
        root = self._read_page(self.root_page_id)
        print_node(root)
        
    def info_btree_unclustered(self) -> dict:
        self._read_metadata()
        stats = {
            'total_nodes': 0,
            'leaf_nodes': 0,
            'internal_nodes': 0,
            'total_keys': 0,
            'height': 0,
            'root_id': self.root_page_id
        }
        
        def traverse(node, depth=0):
            stats['total_nodes'] += 1
            stats['height'] = max(stats['height'], depth)
            stats['total_keys'] += len(node.keys)
            
            if isinstance(node, LeafNode):
                stats['leaf_nodes'] += 1
            else:
                stats['internal_nodes'] += 1
                for child_id in node.children_ids:
                    child = self._read_page(child_id)
                    traverse(child, depth + 1)
        
        root = self._read_page(self.root_page_id)
        if root:
            traverse(root)
            stats['height'] += 1  
        
        return stats

    def close(self):
        """Close the index file"""
        pass

    def clear(self):
        """Clear the index and recreate empty file"""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        
        self.next_page_id = 1
        self.root_page_id = 0
        
        # Create new root leaf page
        root = LeafNode()
        root.page_id = 0
        self._write_page(0, root)
        self._write_metadata()

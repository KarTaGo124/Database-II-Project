from typing import Any, List, Optional
import bisect
import pickle
import os
from ..core.performance_tracker import PerformanceTracker, OperationResult
from ..core.record import Record

PAGE_SIZE = 4096  # 4KB pages


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
        self.values = []  # List of PrimaryKeyPointer objects
        self.previous_id = None
        self.next_id = None


class InternalNode(Node):
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children_ids = []


class PrimaryKeyPointer:
    """
    Pointer to primary key - used in unclustered indexes.
    Secondary index stores: secondary_key -> primary_key
    Then use primary_key to search in clustered index.
    """
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
    """
    B+ Tree Unclustered (Secondary) Index - Disk-based only.
    Stores: secondary_key -> primary_key
    All operations involve disk I/O (read/write tracking).
    """
    def __init__(self, order: int, index_column: str, file_path: str):
        self.index_column = index_column
        self.order = order
        self.max_keys = order - 1
        self.min_keys = (order + 1) // 2 - 1
        self.key_field = index_column
        self.file_path = file_path
        
        # Performance tracking
        self.performance = PerformanceTracker()
        
        # Disk-based storage
        self.next_page_id = 1
        self.root_page_id = 0
        
        # Initialize file
        self._initialize_file()
        
    def _initialize_file(self):
        """Initialize B+ tree file with root leaf page"""
        if not os.path.exists(self.file_path):
            # Create root leaf page
            root = LeafNode()
            root.page_id = 0
            self._write_page(0, root)
            self._write_metadata()
    
    def _write_metadata(self):
        """Write metadata (root_page_id, next_page_id)"""
        with open(self.file_path, 'r+b') as f:
            f.seek(0)
            f.write(self.root_page_id.to_bytes(8, 'little'))
            f.write(self.next_page_id.to_bytes(8, 'little'))
    
    def _read_metadata(self):
        """Read metadata from file"""
        if os.path.exists(self.file_path):
            with open(self.file_path, 'rb') as f:
                f.seek(0)
                root_bytes = f.read(8)
                next_bytes = f.read(8)
                if len(root_bytes) == 8 and len(next_bytes) == 8:
                    self.root_page_id = int.from_bytes(root_bytes, 'little')
                    self.next_page_id = int.from_bytes(next_bytes, 'little')
    
    def _get_page_offset(self, page_id: int) -> int:
        """Calculate file offset for page"""
        return 16 + (page_id * PAGE_SIZE)  # 16 bytes for metadata
    
    def _read_page(self, page_id: int) -> Optional[Node]:
        """Read page from disk - DISK I/O"""
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
                
                # Deserialize
                node = pickle.loads(page_data.rstrip(b'\x00'))
                node.page_id = page_id
                return node
        except (EOFError, pickle.UnpicklingError, OSError):
            return None
    
    def _write_page(self, page_id: int, node: Node):
        """Write page to disk - DISK I/O"""
        self.performance.track_write()
        
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        
        # Serialize
        node.page_id = page_id
        serialized = pickle.dumps(node)
        
        # Pad to page size
        if len(serialized) > PAGE_SIZE:
            raise Exception(f"Page {page_id} too large: {len(serialized)} > {PAGE_SIZE}")
        
        padded_data = serialized + b'\x00' * (PAGE_SIZE - len(serialized))
        
        # Write to file
        with open(self.file_path, 'r+b' if os.path.exists(self.file_path) else 'w+b') as f:
            offset = self._get_page_offset(page_id)
            f.seek(offset)
            f.write(padded_data)
    
    def _allocate_page_id(self) -> int:
        """Allocate new page ID"""
        page_id = self.next_page_id
        self.next_page_id += 1
        self._write_metadata()
        return page_id

    def search(self, key: Any) -> OperationResult:
        """
        Search by secondary key, return list of PrimaryKeyPointers.
        Multiple records can have same secondary key.
        """
        self.performance.start_operation()
        
        self._read_metadata()
        leaf_node = self._find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        
        primary_keys = []
        while pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            primary_keys.append(leaf_node.values[pos])
            pos += 1
        
        return self.performance.end_operation(primary_keys)

    def insert(self, record: Record) -> OperationResult:
        """
        Insert record into unclustered index.
        Stores: secondary_key -> primary_key
        """
        self.performance.start_operation()
        
        secondary_key = self.get_key_value(record)
        primary_key = record.get_key()
        
        primary_key_pointer = PrimaryKeyPointer(primary_key)
        
        self._read_metadata()
        root = self._read_page(self.root_page_id)
        self._insert_recursive(root, secondary_key, primary_key_pointer)
        
        return self.performance.end_operation(True)

    def _insert_recursive(self, node: Node, key: Any, primary_key_pointer: PrimaryKeyPointer):
        """Recursively insert into tree"""
        if isinstance(node, LeafNode):
            # Insert in leaf
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.values.insert(pos, primary_key_pointer)
            
            self._write_page(node.page_id, node)
            
            # Split if full
            if node.is_full(self.max_keys):
                self._split_leaf(node)
        else:
            # Navigate to child
            pos = bisect.bisect_right(node.keys, key)
            child_id = node.children_ids[pos]
            child = self._read_page(child_id)
            self._insert_recursive(child, key, primary_key_pointer)

    def delete(self, record: Record) -> OperationResult:
        """Delete record from unclustered index"""
        self.performance.start_operation()
        
        secondary_key = self.get_key_value(record)
        primary_key = record.get_key()
        
        result = self._delete_by_keys(secondary_key, primary_key)
        return self.performance.end_operation(result)

    def _delete_by_keys(self, secondary_key: Any, primary_key: Any) -> bool:
        """Delete specific entry by secondary and primary keys"""
        self._read_metadata()
        leaf = self._find_leaf_node(secondary_key)
        
        # Find specific entry
        for i, (key, value) in enumerate(zip(leaf.keys, leaf.values)):
            if key == secondary_key and value.primary_key == primary_key:
                leaf.keys.pop(i)
                leaf.values.pop(i)
                self._write_page(leaf.page_id, leaf)
                
                # Handle underflow
                if leaf.page_id != self.root_page_id and leaf.is_underflow(self.min_keys):
                    self._handle_leaf_underflow(leaf)
                
                # Check if root is empty
                root = self._read_page(self.root_page_id)
                if isinstance(root, InternalNode) and len(root.keys) == 0:
                    if len(root.children_ids) > 0:
                        self.root_page_id = root.children_ids[0]
                        new_root = self._read_page(self.root_page_id)
                        new_root.parent_id = None
                        self._write_page(self.root_page_id, new_root)
                        self._write_metadata()
                
                return True
        
        return False

    def range_search(self, start_key: Any, end_key: Any) -> OperationResult:
        """
        Range search on secondary index.
        Returns list of PrimaryKeyPointers in range [start_key, end_key].
        """
        self.performance.start_operation()
        
        results = []
        self._read_metadata()
        leaf = self._find_leaf_node(start_key)
        
        pos = bisect.bisect_left(leaf.keys, start_key)
        
        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_key:
                    return self.performance.end_operation(results)
                if leaf.keys[i] >= start_key:
                    results.append(leaf.values[i])
            
            # Move to next leaf
            if leaf.next_id is not None:
                leaf = self._read_page(leaf.next_id)
            else:
                leaf = None
            pos = 0
        
        return self.performance.end_operation(results)

    def _find_leaf_node(self, key: Any) -> LeafNode:
        """Find leaf node for key"""
        current = self._read_page(self.root_page_id)
        while isinstance(current, InternalNode):
            pos = bisect.bisect_right(current.keys, key)
            child_id = current.children_ids[pos]
            current = self._read_page(child_id)
        return current

    def get_key_value(self, record) -> Any:
        """Extract secondary key from record"""
        return record.get_field_value(self.index_column)

    def _split_leaf(self, leaf: LeafNode):
        """Split full leaf node"""
        mid = len(leaf.keys) // 2
        new_leaf = LeafNode()
        
        new_page_id = self._allocate_page_id()
        new_leaf.page_id = new_page_id
        
        # Move half to new leaf
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        new_leaf.parent_id = leaf.parent_id
        
        # Update pointers
        new_leaf.next_id = leaf.next_id
        new_leaf.previous_id = leaf.page_id
        
        if leaf.next_id is not None:
            next_leaf = self._read_page(leaf.next_id)
            next_leaf.previous_id = new_leaf.page_id
            self._write_page(leaf.next_id, next_leaf)
        
        leaf.next_id = new_leaf.page_id
        
        # Keep first half
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        
        # Write pages
        self._write_page(new_leaf.page_id, new_leaf)
        self._write_page(leaf.page_id, leaf)
        
        # Promote key
        promote_key = new_leaf.keys[0]
        self._promote_key(leaf, promote_key, new_leaf)

    def _split_internal(self, internal: InternalNode):
        """Split full internal node"""
        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]
        
        new_internal = InternalNode()
        new_page_id = self._allocate_page_id()
        new_internal.page_id = new_page_id
        
        # Move to new internal
        new_internal.keys = internal.keys[mid + 1:]
        new_internal.children_ids = internal.children_ids[mid + 1:]
        new_internal.parent_id = internal.parent_id
        
        # Update children's parent pointers
        for child_id in new_internal.children_ids:
            child = self._read_page(child_id)
            child.parent_id = new_internal.page_id
            self._write_page(child_id, child)
        
        # Keep first half
        internal.keys = internal.keys[:mid]
        internal.children_ids = internal.children_ids[:mid + 1]
        
        # Write pages
        self._write_page(new_internal.page_id, new_internal)
        self._write_page(internal.page_id, internal)
        
        # Promote key
        self._promote_key(internal, promote_key, new_internal)

    def _promote_key(self, left_child: Node, key: Any, right_child: Node):
        """Promote key to parent"""
        if left_child.parent_id is None:
            # Create new root
            new_root = InternalNode()
            new_root_id = self._allocate_page_id()
            new_root.page_id = new_root_id
            new_root.keys = [key]
            new_root.children_ids = [left_child.page_id, right_child.page_id]
            new_root.parent_id = None
            
            # Update children's parents
            left_child.parent_id = new_root_id
            right_child.parent_id = new_root_id
            
            # Write pages
            self._write_page(new_root_id, new_root)
            self._write_page(left_child.page_id, left_child)
            self._write_page(right_child.page_id, right_child)
            
            # Update root
            self.root_page_id = new_root_id
            self._write_metadata()
        else:
            # Insert in parent
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
        """Handle leaf underflow"""
        if leaf.parent_id is None:
            return
        
        parent = self._read_page(leaf.parent_id)
        if not parent:
            return
        
        leaf_index = parent.children_ids.index(leaf.page_id)
        
        # Try borrow from left
        if leaf_index > 0:
            left_sibling_id = parent.children_ids[leaf_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, LeafNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return
        
        # Try borrow from right
        if leaf_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[leaf_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, LeafNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_leaf(leaf, right_sibling, parent, leaf_index)
                return
        
        # Merge
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
        """Borrow from left sibling"""
        borrowed_key = left_sibling.keys.pop()
        borrowed_value = left_sibling.values.pop()
        
        leaf.keys.insert(0, borrowed_key)
        leaf.values.insert(0, borrowed_value)
        
        parent.keys[leaf_index - 1] = leaf.keys[0]
        
        self._write_page(leaf.page_id, leaf)
        self._write_page(left_sibling.page_id, left_sibling)
        self._write_page(parent.page_id, parent)

    def _borrow_from_right_leaf(self, leaf: LeafNode, right_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        """Borrow from right sibling"""
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_value = right_sibling.values.pop(0)
        
        leaf.keys.append(borrowed_key)
        leaf.values.append(borrowed_value)
        
        parent.keys[leaf_index] = right_sibling.keys[0] if right_sibling.keys else borrowed_key
        
        self._write_page(leaf.page_id, leaf)
        self._write_page(right_sibling.page_id, right_sibling)
        self._write_page(parent.page_id, parent)

    def _merge_leaf_with_left(self, leaf: LeafNode, left_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        """Merge with left sibling"""
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
        """Merge with right sibling"""
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
        """Handle internal node underflow"""
        if internal.parent_id is None:
            return
        
        parent = self._read_page(internal.parent_id)
        if not parent:
            return
        
        internal_index = parent.children_ids.index(internal.page_id)
        
        # Try borrow from left
        if internal_index > 0:
            left_sibling_id = parent.children_ids[internal_index - 1]
            left_sibling = self._read_page(left_sibling_id)
            if isinstance(left_sibling, InternalNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_internal(internal, left_sibling, parent, internal_index)
                return
        
        # Try borrow from right
        if internal_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[internal_index + 1]
            right_sibling = self._read_page(right_sibling_id)
            if isinstance(right_sibling, InternalNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_internal(internal, right_sibling, parent, internal_index)
                return
        
        # Merge
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
        """Borrow from left internal sibling"""
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
        """Borrow from right internal sibling"""
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
        """Merge with left internal sibling"""
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
        """Merge with right internal sibling"""
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
        """Drop index by removing file"""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
            return [self.file_path]
        return []

    def clear(self):
        """Clear index and recreate empty"""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        
        self.next_page_id = 1
        self.root_page_id = 0
        
        root = LeafNode()
        root.page_id = 0
        self._write_page(0, root)
        self._write_metadata()
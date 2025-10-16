from typing import Any, List, Optional, Dict
import bisect
import pickle
import os
from ..core.record import Record
from ..core.performance_tracker import PerformanceTracker, OperationResult


class Node:
    """Base node class for B+ Tree"""
    def __init__(self, is_leaf: bool = False):
        self.is_leaf = is_leaf
        self.keys = []
        self.parent_id = None  # ID only, not object reference
        self.id = None

    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) > max_keys
    
    def is_underflow(self, min_keys: int) -> bool:
        return len(self.keys) < min_keys


class ClusteredLeafNode(Node):
    """Leaf node that stores actual records (clustered index)"""
    def __init__(self):
        super().__init__(is_leaf=True)
        self.records = []  # List of actual Record objects
        self.previous_id = None  # ID only
        self.next_id = None  # ID only


class ClusteredInternalNode(Node):
    """Internal node that stores child page IDs (not objects)"""
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children_ids = []  # List of child page IDs only, NO object references


class BPlusTreeClusteredIndex:
    """
    Disk-based B+ Tree Clustered Index - stores actual records in leaf nodes.
    All operations use disk I/O, no in-memory caching, all references by page ID.
    """
    def __init__(self, order: int, key_column: str, file_path: str, record_class):
        self.key_column = key_column
        self.record_class = record_class
        self.order = order
        self.max_keys = order - 1
        self.min_keys = (order + 1) // 2 - 1
        self.file_path = file_path
        self.data_file = file_path + ".dat"
        self.page_size = 4096
        
        # Performance tracking
        self.performance = PerformanceTracker()
        
        # Page ID management
        self.next_page_id = 1
        self.root_page_id = 0
        
        # Initialize file and root page
        if not os.path.exists(self.data_file):
            # Create initial root (empty leaf)
            root = ClusteredLeafNode()
            root.id = 0
            root.parent_id = None
            root.previous_id = None
            root.next_id = None
            
            with open(self.data_file, 'wb') as f:
                # Pre-allocate first page
                f.write(b'\x00' * self.page_size)
            
            # Write root page
            self._write_page(0, root)
        

    
    def _load_page(self, page_id: int) -> Optional[Node]:
        """Load a page from disk by page ID - DISK I/O ONLY"""
        if page_id is None:
            return None
            
        self.performance.track_read()
        
        if not os.path.exists(self.data_file):
            return None
            
        try:
            offset = page_id * self.page_size
            
            with open(self.data_file, 'rb') as f:
                f.seek(offset)
                page_data = f.read(self.page_size)
                
                # Check if page is empty
                if page_data == b'\x00' * self.page_size:
                    return None
                
                # Remove padding
                actual_end = len(page_data.rstrip(b'\x00'))
                if actual_end == 0:
                    return None
                    
                # Deserialize
                node_data = pickle.loads(page_data[:actual_end])
                
            # Reconstruct node from serialized data
            if node_data['is_leaf']:
                node = ClusteredLeafNode()
                node.keys = node_data['keys']
                node.records = node_data['records']
                node.previous_id = node_data.get('previous_id')
                node.next_id = node_data.get('next_id')
            else:
                node = ClusteredInternalNode()
                node.keys = node_data['keys']
                node.children_ids = node_data['children_ids']  # IDs only
                
            node.id = node_data['id']
            node.parent_id = node_data.get('parent_id')
            
            return node
            
        except Exception as e:
            print(f"Error loading page {page_id}: {e}")
            return None
    
    def _write_page(self, page_id: int, page: Node):
        """Write a page to disk - DISK I/O ONLY"""
        self.performance.track_write()
        
        try:
            # Serialize node data (IDs only, no object references)
            page_data = {
                'id': page.id,
                'is_leaf': page.is_leaf,
                'keys': page.keys,
                'parent_id': page.parent_id  # ID only
            }
            
            if isinstance(page, ClusteredLeafNode):
                page_data['records'] = page.records
                page_data['previous_id'] = page.previous_id
                page_data['next_id'] = page.next_id
            else:
                page_data['children_ids'] = page.children_ids  # IDs only
                
            # Serialize
            serialized_data = pickle.dumps(page_data, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Check size
            if len(serialized_data) > self.page_size:
                raise Exception(f"Page data too large: {len(serialized_data)} > {self.page_size}")
            
            # Pad to page size
            padded_data = serialized_data + b'\x00' * (self.page_size - len(serialized_data))
            
            # Calculate offset
            offset = page_id * self.page_size
            
            # Extend file if necessary
            if not os.path.exists(self.data_file):
                with open(self.data_file, 'wb') as f:
                    f.write(b'\x00' * self.page_size)
            
            current_size = os.path.getsize(self.data_file)
            required_size = (page_id + 1) * self.page_size
            
            if current_size < required_size:
                with open(self.data_file, 'ab') as f:
                    f.write(b'\x00' * (required_size - current_size))
            
            # Write page at offset
            with open(self.data_file, 'r+b') as f:
                f.seek(offset)
                f.write(padded_data)
                f.flush()
                
        except Exception as e:
            print(f"Error writing page {page_id}: {e}")

    
    def _delete_page(self, page_id: int):
        """Mark a page as deleted by writing zeros"""
        try:
            offset = page_id * self.page_size
            
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r+b') as f:
                    f.seek(offset)
                    f.write(b'\x00' * self.page_size)
                    f.flush()
        except Exception as e:
            print(f"Error deleting page {page_id}: {e}")
    
    def _allocate_page_id(self) -> int:
        """Allocate a new page ID"""
        page_id = self.next_page_id
        self.next_page_id += 1
        return page_id
    
    def get_total_pages(self) -> int:
        """Count total pages in the data file"""
        if not os.path.exists(self.data_file):
            return 0
        
        file_size = os.path.getsize(self.data_file)
        return file_size // self.page_size
    
    def get_file_info(self) -> dict:
        """Get information about the data file"""
        if not os.path.exists(self.data_file):
            return {"exists": False}
        
        file_size = os.path.getsize(self.data_file)
        total_pages = file_size // self.page_size
        
        return {
            "exists": True,
            "file_path": self.data_file,
            "file_size_bytes": file_size,
            "file_size_kb": file_size / 1024,
            "page_size": self.page_size,
            "total_pages": total_pages,
            "used_space_ratio": f"{(self.next_page_id / total_pages * 100):.1f}%" if total_pages > 0 else "0%"
        }
    
    def get_key_value(self, record: Record) -> Any:
        """Extract key value from record"""
        return record.get_field_value(self.key_column)

            
    def search(self, key: Any) -> OperationResult:
        """Search for a record by key and return OperationResult with metrics"""
        self.performance.start_operation()
        
        leaf_node = self._find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            record = leaf_node.records[pos]
            return self.performance.end_operation([record])
        
        return self.performance.end_operation([])

    def insert(self, record: Record) -> OperationResult:
        """Insert a record and return OperationResult with metrics"""
        self.performance.start_operation()
        
        key = self.get_key_value(record)
        
        # Check if key already exists
        leaf_node = self._find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            return self.performance.end_operation(False)  # Already exists, use end_operation to reset tracker
            
        self._insert_recursive(self.root_page_id, key, record)
        return self.performance.end_operation(True)

    def delete(self, key: Any) -> OperationResult:
        """Delete a record by key and return OperationResult with metrics"""
        self.performance.start_operation()
        
        leaf = self._find_leaf_node(key)
        pos = bisect.bisect_left(leaf.keys, key)
        
        if pos >= len(leaf.keys) or leaf.keys[pos] != key:
            return self.performance.end_operation(False)  # Key not found
        
        # Remove key and record from leaf
        leaf.keys.pop(pos)
        leaf.records.pop(pos)
        
        self._write_page(leaf.id, leaf)
        
        # Handle underflow if needed
        if leaf.id != self.root_page_id and leaf.is_underflow(self.min_keys):
            self._handle_leaf_underflow(leaf)
        
        # Check if root is empty internal node
        root = self._load_page(self.root_page_id)
        if isinstance(root, ClusteredInternalNode) and len(root.keys) == 0:
            if len(root.children_ids) > 0:
                old_root_id = root.id
                self.root_page_id = root.children_ids[0]
                
                # Update new root's parent
                new_root = self._load_page(self.root_page_id)
                new_root.parent_id = None
                self._write_page(self.root_page_id, new_root)
                
                self._delete_page(old_root_id)
        
        return self.performance.end_operation(True)
    
    def range_search(self, start_key: Any, end_key: Any) -> OperationResult:
        """Range search and return OperationResult with metrics"""
        self.performance.start_operation()
        
        results = []
        leaf = self._find_leaf_node(start_key)
        
        # Starting position
        pos = bisect.bisect_left(leaf.keys, start_key)
        
        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_key:
                    return self.performance.end_operation(results)
                if leaf.keys[i] >= start_key:
                    results.append(leaf.records[i])
            
            # Move to next leaf
            if leaf.next_id is not None:
                leaf = self._load_page(leaf.next_id)
                pos = 0
            else:
                break
        
        return self.performance.end_operation(results)
    
    def _find_leaf_node(self, key: Any) -> ClusteredLeafNode:
        """Find the leaf node that should contain the key - DISK I/O ONLY"""
        current = self._load_page(self.root_page_id)
        
        while isinstance(current, ClusteredInternalNode):
            pos = bisect.bisect_right(current.keys, key)
            child_id = current.children_ids[pos]  # ID only
            current = self._load_page(child_id)
        
        return current
    
    def _insert_recursive(self, node_id: int, key: Any, record: Record):
        """Recursively insert a record - DISK I/O ONLY, using page IDs"""
        node = self._load_page(node_id)
        
        if isinstance(node, ClusteredLeafNode):
            # Insert into leaf
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.records.insert(pos, record)
            
            self._write_page(node.id, node)
            
            # Check for split
            if node.is_full(self.max_keys):
                self._split_leaf(node)
        else:
            # Navigate to child
            pos = bisect.bisect_right(node.keys, key)
            child_id = node.children_ids[pos]
            child = self._load_page(child_id)
            
            self._insert_recursive(child_id, key, record)
            
            # Reload node after recursive call (may have been modified)
            node = self._load_page(node_id)
            child = self._load_page(child_id)
            
            # Check for split
            if child.is_full(self.max_keys):
                if isinstance(child, ClusteredLeafNode):
                    self._split_leaf(child)
                else:
                    self._split_internal(child)
    
    def _split_leaf(self, leaf: ClusteredLeafNode):
        """Split a full leaf node - ALL OPERATIONS USE IDs"""
        
        mid = len(leaf.keys) // 2
        new_leaf = ClusteredLeafNode()
        
        # Move half the keys and records to new leaf
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.records = leaf.records[mid:]
        new_leaf.parent_id = leaf.parent_id  # ID only
        
        # Update linked list pointers (IDs only)
        new_leaf.next_id = leaf.next_id
        new_leaf.previous_id = leaf.id
        
        if leaf.next_id is not None:
            next_leaf = self._load_page(leaf.next_id)
            next_leaf.previous_id = None  # Will be updated below
        
        leaf.next_id = None  # Will be set to new_leaf.id
        
        # Keep first half in original leaf
        leaf.keys = leaf.keys[:mid]
        leaf.records = leaf.records[:mid]
        
        
        # Allocate page ID for new leaf
        new_leaf.id = self._allocate_page_id()
        
        
        # Update pointers with actual IDs
        leaf.next_id = new_leaf.id
        if new_leaf.next_id is not None:
            next_leaf = self._load_page(new_leaf.next_id)
            next_leaf.previous_id = new_leaf.id
            self._write_page(next_leaf.id, next_leaf)
        
        # Write new leaf first
        self._write_page(new_leaf.id, new_leaf)
        
        # DON'T write the original leaf yet - _promote_key will update its parent_id
        # and write it
        
        # Promote first key of new leaf to parent
        promote_key = new_leaf.keys[0]
        self._promote_key_and_write_left(leaf, promote_key, new_leaf.id)
    
    def _split_internal(self, internal: ClusteredInternalNode):
        """Split a full internal node - ALL OPERATIONS USE IDs"""
        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]
        
        new_internal = ClusteredInternalNode()
        
        # Move keys and children to new internal node
        new_internal.keys = internal.keys[mid + 1:]
        new_internal.children_ids = internal.children_ids[mid + 1:]  # IDs only
        new_internal.parent_id = internal.parent_id  # ID only
        
        # Keep first half in original internal node
        internal.keys = internal.keys[:mid]
        internal.children_ids = internal.children_ids[:mid + 1]  # IDs only
        
        # Allocate page ID for new internal
        new_internal.id = self._allocate_page_id()
        
        # Update children's parent pointers with actual ID
        for child_id in new_internal.children_ids:
            child = self._load_page(child_id)
            child.parent_id = new_internal.id
            self._write_page(child_id, child)
        
        # Write new internal node
        self._write_page(new_internal.id, new_internal)
        
        # DON'T write the original internal yet - _promote_key will update its parent_id
        # and write it
        
        # Promote middle key to parent
        self._promote_key_and_write_left(internal, promote_key, new_internal.id)
    
    def _promote_key_and_write_left(self, left_child: Node, key: Any, right_child_id: int):
        """
        Promote a key to parent and properly write the left child.
        This is called from _split_leaf/_split_internal to avoid parent_id inconsistencies.
        """
        left_child_id = left_child.id
        
        
        if left_child.parent_id is None:
            # Create new root
            new_root = ClusteredInternalNode()
            new_root.keys = [key]
            new_root.children_ids = [left_child_id, right_child_id]
            new_root.parent_id = None
            
            # Allocate page ID for new root
            new_root.id = self._allocate_page_id()
            
            
            # Update children's parent pointers
            left_child.parent_id = new_root.id
            right_child = self._load_page(right_child_id)
            right_child.parent_id = new_root.id
            
            # Write all pages
            self._write_page(left_child_id, left_child)
            self._write_page(right_child_id, right_child)
            self._write_page(new_root.id, new_root)
            
            # Update root reference
            self.root_page_id = new_root.id
        else:
            # Insert into parent
            parent = self._load_page(left_child.parent_id)
            
            
            # Verify parent is an internal node
            if not isinstance(parent, ClusteredInternalNode):
                raise ValueError(f"Parent must be an internal node, got {type(parent)}")
            
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children_ids.insert(pos + 1, right_child_id)
            
            # Update right child's parent
            right_child = self._load_page(right_child_id)
            right_child.parent_id = parent.id
            self._write_page(right_child_id, right_child)
            
            # Write left child and parent
            self._write_page(left_child_id, left_child)
            self._write_page(parent.id, parent)
            
            # Check if parent is now full
            if parent.is_full(self.max_keys):
                self._split_internal(parent)
    
    def _promote_key(self, left_child_id: int, key: Any, right_child_id: int):
        """Promote a key to parent - ALL OPERATIONS USE IDs"""
        left_child = self._load_page(left_child_id)
        self._promote_key_and_write_left(left_child, key, right_child_id)
    
    def _handle_leaf_underflow(self, leaf: ClusteredLeafNode):
        """Handle leaf underflow using IDs only"""
        if leaf.parent_id is None:
            return
        
        parent = self._load_page(leaf.parent_id)
        
        # Find leaf position in parent
        leaf_index = parent.children_ids.index(leaf.id)
        
        # Try to borrow from left sibling
        if leaf_index > 0:
            left_sibling_id = parent.children_ids[leaf_index - 1]
            left_sibling = self._load_page(left_sibling_id)
            if isinstance(left_sibling, ClusteredLeafNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return
        
        # Try to borrow from right sibling
        if leaf_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[leaf_index + 1]
            right_sibling = self._load_page(right_sibling_id)
            if isinstance(right_sibling, ClusteredLeafNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_leaf(leaf, right_sibling, parent, leaf_index)
                return
        
        # Merge with sibling
        if leaf_index > 0:
            left_sibling_id = parent.children_ids[leaf_index - 1]
            left_sibling = self._load_page(left_sibling_id)
            if isinstance(left_sibling, ClusteredLeafNode):
                self._merge_leaf_with_left(leaf, left_sibling, parent, leaf_index)
        else:
            right_sibling_id = parent.children_ids[leaf_index + 1]
            right_sibling = self._load_page(right_sibling_id)
            if isinstance(right_sibling, ClusteredLeafNode):
                self._merge_leaf_with_right(leaf, right_sibling, parent, leaf_index)
    
    def _borrow_from_left_leaf(self, leaf: ClusteredLeafNode, left_sibling: ClusteredLeafNode, 
                               parent: ClusteredInternalNode, leaf_index: int):
        """Borrow from left sibling leaf"""
        borrowed_key = left_sibling.keys.pop()
        borrowed_record = left_sibling.records.pop()
        
        leaf.keys.insert(0, borrowed_key)
        leaf.records.insert(0, borrowed_record)
        
        parent.keys[leaf_index - 1] = leaf.keys[0]
        
        # Write updated pages
        self._write_page(left_sibling.id, left_sibling)
        self._write_page(leaf.id, leaf)
        self._write_page(parent.id, parent)

    def _borrow_from_right_leaf(self, leaf: ClusteredLeafNode, right_sibling: ClusteredLeafNode,
                                parent: ClusteredInternalNode, leaf_index: int):
        """Borrow from right sibling leaf"""
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_record = right_sibling.records.pop(0)
        
        leaf.keys.append(borrowed_key)
        leaf.records.append(borrowed_record)
        
        parent.keys[leaf_index] = right_sibling.keys[0] if right_sibling.keys else borrowed_key
        
        # Write updated pages
        self._write_page(right_sibling.id, right_sibling)
        self._write_page(leaf.id, leaf)
        self._write_page(parent.id, parent)

    def _merge_leaf_with_left(self, leaf: ClusteredLeafNode, left_sibling: ClusteredLeafNode,
                              parent: ClusteredInternalNode, leaf_index: int):
        """Merge leaf with left sibling"""
        left_sibling.keys.extend(leaf.keys)
        left_sibling.records.extend(leaf.records)
        
        # Update linked list pointers
        left_sibling.next_id = leaf.next_id
        if leaf.next_id is not None:
            next_leaf = self._load_page(leaf.next_id)
            next_leaf.previous_id = left_sibling.id
            self._write_page(next_leaf.id, next_leaf)
        
        # Remove leaf from parent
        parent.children_ids.pop(leaf_index)
        parent.keys.pop(leaf_index - 1)
        
        # Write updated pages and delete old leaf
        self._write_page(left_sibling.id, left_sibling)
        self._write_page(parent.id, parent)
        self._delete_page(leaf.id)
        
        # Handle parent underflow
        if parent.id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _merge_leaf_with_right(self, leaf: ClusteredLeafNode, right_sibling: ClusteredLeafNode,
                               parent: ClusteredInternalNode, leaf_index: int):
        """Merge leaf with right sibling"""
        leaf.keys.extend(right_sibling.keys)
        leaf.records.extend(right_sibling.records)
        
        # Update linked list pointers
        leaf.next_id = right_sibling.next_id
        if right_sibling.next_id is not None:
            next_leaf = self._load_page(right_sibling.next_id)
            next_leaf.previous_id = leaf.id
            self._write_page(next_leaf.id, next_leaf)
        
        # Remove right sibling from parent
        parent.children_ids.pop(leaf_index + 1)
        parent.keys.pop(leaf_index)
        
        # Write updated pages and delete right sibling
        self._write_page(leaf.id, leaf)
        self._write_page(parent.id, parent)
        self._delete_page(right_sibling.id)
        
        # Handle parent underflow
        if parent.id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _handle_internal_underflow(self, internal: ClusteredInternalNode):
        """Handle internal node underflow using IDs only"""
        if internal.parent_id is None:
            return
        
        parent = self._load_page(internal.parent_id)
        internal_index = parent.children_ids.index(internal.id)
        
        # Try borrow from left
        if internal_index > 0:
            left_sibling_id = parent.children_ids[internal_index - 1]
            left_sibling = self._load_page(left_sibling_id)
            if isinstance(left_sibling, ClusteredInternalNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_internal(internal, left_sibling, parent, internal_index)
                return
        
        # Try borrow from right
        if internal_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[internal_index + 1]
            right_sibling = self._load_page(right_sibling_id)
            if isinstance(right_sibling, ClusteredInternalNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_internal(internal, right_sibling, parent, internal_index)
                return
        
        # Merge
        if internal_index > 0:
            left_sibling_id = parent.children_ids[internal_index - 1]
            left_sibling = self._load_page(left_sibling_id)
            if isinstance(left_sibling, ClusteredInternalNode):
                self._merge_internal_with_left(internal, left_sibling, parent, internal_index)
        else:
            right_sibling_id = parent.children_ids[internal_index + 1]
            right_sibling = self._load_page(right_sibling_id)
            if isinstance(right_sibling, ClusteredInternalNode):
                self._merge_internal_with_right(internal, right_sibling, parent, internal_index)

    def _borrow_from_left_internal(self, internal: ClusteredInternalNode, left_sibling: ClusteredInternalNode,
                                   parent: ClusteredInternalNode, internal_index: int):
        """Borrow from left sibling internal"""
        separator_key = parent.keys[internal_index - 1]
        
        internal.keys.insert(0, separator_key)
        borrowed_child_id = left_sibling.children_ids.pop()
        internal.children_ids.insert(0, borrowed_child_id)
        
        # Update borrowed child's parent
        borrowed_child = self._load_page(borrowed_child_id)
        borrowed_child.parent_id = internal.id
        self._write_page(borrowed_child_id, borrowed_child)
        
        parent.keys[internal_index - 1] = left_sibling.keys.pop()
        
        # Write updated pages
        self._write_page(left_sibling.id, left_sibling)
        self._write_page(internal.id, internal)
        self._write_page(parent.id, parent)

    def _borrow_from_right_internal(self, internal: ClusteredInternalNode, right_sibling: ClusteredInternalNode,
                                    parent: ClusteredInternalNode, internal_index: int):
        """Borrow from right sibling internal"""
        separator_key = parent.keys[internal_index]
        
        internal.keys.append(separator_key)
        borrowed_child_id = right_sibling.children_ids.pop(0)
        internal.children_ids.append(borrowed_child_id)
        
        # Update borrowed child's parent
        borrowed_child = self._load_page(borrowed_child_id)
        borrowed_child.parent_id = internal.id
        self._write_page(borrowed_child_id, borrowed_child)
        
        parent.keys[internal_index] = right_sibling.keys.pop(0)
        
        # Write updated pages
        self._write_page(right_sibling.id, right_sibling)
        self._write_page(internal.id, internal)
        self._write_page(parent.id, parent)

    def _merge_internal_with_left(self, internal: ClusteredInternalNode, left_sibling: ClusteredInternalNode,
                                  parent: ClusteredInternalNode, internal_index: int):
        """Merge internal with left sibling"""
        separator_key = parent.keys[internal_index - 1]
        
        left_sibling.keys.append(separator_key)
        left_sibling.keys.extend(internal.keys)
        left_sibling.children_ids.extend(internal.children_ids)
        
        # Update children's parent pointers
        for child_id in internal.children_ids:
            child = self._load_page(child_id)
            child.parent_id = left_sibling.id
            self._write_page(child_id, child)
        
        # Remove from parent
        parent.children_ids.pop(internal_index)
        parent.keys.pop(internal_index - 1)
        
        # Write updated pages and delete old node
        self._write_page(left_sibling.id, left_sibling)
        self._write_page(parent.id, parent)
        self._delete_page(internal.id)
        
        # Handle parent underflow
        if parent.id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _merge_internal_with_right(self, internal: ClusteredInternalNode, right_sibling: ClusteredInternalNode,
                                   parent: ClusteredInternalNode, internal_index: int):
        """Merge internal with right sibling"""
        separator_key = parent.keys[internal_index]
        
        internal.keys.append(separator_key)
        internal.keys.extend(right_sibling.keys)
        internal.children_ids.extend(right_sibling.children_ids)
        
        # Update children's parent pointers
        for child_id in right_sibling.children_ids:
            child = self._load_page(child_id)
            child.parent_id = internal.id
            self._write_page(child_id, child)
        
        # Remove from parent
        parent.children_ids.pop(internal_index + 1)
        parent.keys.pop(internal_index)
        
        # Write updated pages and delete right sibling
        self._write_page(internal.id, internal)
        self._write_page(parent.id, parent)
        self._delete_page(right_sibling.id)
        
        # Handle parent underflow
        if parent.id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)
    
    def scan_all(self) -> OperationResult:
        """
        Scan all records in the B+ Tree by traversing leaf nodes.
        Returns OperationResult with all records and metrics.
        """
        self.performance.start_operation()
        results = []
        
        # Find the leftmost leaf
        current = self._load_page(self.root_page_id)
        while isinstance(current, ClusteredInternalNode):
            if len(current.children_ids) > 0:
                current = self._load_page(current.children_ids[0])
            else:
                break
        
        # Traverse all leaf nodes
        while current is not None and isinstance(current, ClusteredLeafNode):
            results.extend(current.records)
            
            # Move to next leaf
            if current.next_id is not None:
                current = self._load_page(current.next_id)
            else:
                current = None
        
        return self.performance.end_operation(results)
    
    def drop_table(self):
        """Drop the entire table by deleting the data file"""
        if os.path.exists(self.data_file):
            os.remove(self.data_file)
            
        # Reset internal state
        self.next_page_id = 1
        self.root_page_id = 0
        
        # Create new empty root
        root = ClusteredLeafNode()
        root.id = 0
        root.parent_id = None
        root.previous_id = None
        root.next_id = None
        
        with open(self.data_file, 'wb') as f:
            f.write(b'\x00' * self.page_size)
        
        self._write_page(0, root)
    
    def info_btree_clustered(self) -> dict:
        """Get B+Tree statistics"""
        stats = {
            "order": self.order,
            "max_keys": self.max_keys,
            "min_keys": self.min_keys,
            "total_pages": self.get_total_pages(),
            "root_page_id": self.root_page_id,
            "next_page_id": self.next_page_id,
        }
        return stats

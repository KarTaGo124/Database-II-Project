from typing import Any, List, Optional, Tuple, Union
import bisect
import pickle
import os
import time
from ..core.record import Record
from ..core.performance_tracker import PerformanceTracker


class Node:
    def __init__(self, is_leaf: bool = False):
        self.is_leaf = is_leaf
        self.keys = []
        self.parent = None
        self.id = None

    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) > max_keys
    
    def is_underflow(self, min_keys: int) -> bool:
        return len(self.keys) < min_keys

class ClusteredLeafNode(Node):
    def __init__(self):
        super().__init__(is_leaf=True)
        self.records = []  # list of records
        self.previous = None
        self.next = None

class ClusteredInternalNode(Node):
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children = []  # pointers to child nodes
        
class BPlusTreeClusteredIndex:
    def __init__(self, order: int, key_column: str, file_path: str, record_class):
        self.key_column = key_column
        self.record_class = record_class
        self.root = ClusteredLeafNode()
        self.order = order
        self.max_keys = order - 1
        self.min_keys = (order + 1) // 2 - 1
        self.file_path = file_path
        self.first_leaf = self.root
        
        # Performance tracking
        self.performance = PerformanceTracker()
        
        # Disk-based storage - SINGLE FILE for all pages (like real DBMS)
        self.data_file = file_path + ".dat"  # Single binary file for all pages
        self.page_size = 4096  # Standard 4KB page size (like PostgreSQL, MySQL)
        
        self.next_page_id = 1
        self.root_page_id = 0
        self.root.id = 0
        
        # Initialize empty file if it doesn't exist
        if not os.path.exists(self.data_file):
            with open(self.data_file, 'wb') as f:
                f.write(b'\x00' * self.page_size)  # Write empty root page
        
        # Save initial root page to disk immediately
        self.write_page(0, self.root)
        
    
    def load_page(self, page_id: int) -> Node:
        """Load a page from disk - SINGLE FILE with page offset (like real DBMS)"""
        self.performance.track_read()
        
        # Simulate realistic disk I/O delay
        time.sleep(0.001)
        
        if not os.path.exists(self.data_file):
            return None
            
        try:
            # Calculate offset: page_id * page_size
            offset = page_id * self.page_size
            
            with open(self.data_file, 'rb') as f:
                f.seek(offset)  # Seek to page position
                page_data = f.read(self.page_size)  # Read exactly one page
                
                # Check if page is empty (all zeros)
                if page_data == b'\x00' * self.page_size:
                    return None
                
                # Find actual data end (remove padding zeros)
                actual_end = len(page_data.rstrip(b'\x00'))
                if actual_end == 0:
                    return None
                    
                # Deserialize page data
                node_data = pickle.loads(page_data[:actual_end])
                
            # Reconstruct the node from binary data
            if node_data['is_leaf']:
                node = ClusteredLeafNode()
                node.keys = node_data['keys']
                node.records = node_data['records']  # Records preserved by pickle
                node.previous = node_data.get('previous')
                node.next = node_data.get('next')
            else:
                node = ClusteredInternalNode()
                node.keys = node_data['keys']
                node.children = node_data['children']
                
            node.id = node_data['id']
            node.parent = node_data.get('parent')
            
            return node
            
        except Exception as e:
            print(f"Error loading page {page_id}: {e}")
            return None
    
    def write_page(self, page_id: int, page: Node):
        """Write a page to disk - SINGLE FILE with page offset (like real DBMS)"""
        self.performance.track_write()
        
        # Simulate realistic disk I/O delay  
        time.sleep(0.001)
        
        try:
            page_data = {
                'id': page.id,
                'is_leaf': page.is_leaf,
                'keys': page.keys,
                'parent': page.parent.id if page.parent else None
            }
            
            if isinstance(page, ClusteredLeafNode):
                page_data['records'] = page.records
                page_data['previous'] = page.previous.id if page.previous else None
                page_data['next'] = page.next.id if page.next else None
            else:
                page_data['children'] = [child.id if hasattr(child, 'id') else child for child in page.children]
                
            # Serialize page data
            serialized_data = pickle.dumps(page_data, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Check if data fits in page size
            if len(serialized_data) > self.page_size:
                raise Exception(f"Page data too large: {len(serialized_data)} > {self.page_size}")
            
            # Pad with zeros to fill exactly one page
            padded_data = serialized_data + b'\x00' * (self.page_size - len(serialized_data))
            
            # Calculate offset and write to specific position
            offset = page_id * self.page_size
            
            # Extend file if necessary
            if not os.path.exists(self.data_file):
                with open(self.data_file, 'wb') as f:
                    f.write(b'\x00' * self.page_size)
            
            # Check current file size and extend if needed
            current_size = os.path.getsize(self.data_file)
            required_size = (page_id + 1) * self.page_size
            
            if current_size < required_size:
                with open(self.data_file, 'ab') as f:
                    f.write(b'\x00' * (required_size - current_size))
            
            # Write the page at the correct offset
            with open(self.data_file, 'r+b') as f:
                f.seek(offset)
                f.write(padded_data)
                f.flush()  # Ensure data is written to disk
                
        except Exception as e:
            print(f"Error writing page {page_id}: {e}")
    
    def delete_page(self, page_id: int):
        """Mark a page as deleted by writing zeros (like real DBMS)"""
        try:
            offset = page_id * self.page_size
            
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r+b') as f:
                    f.seek(offset)
                    f.write(b'\x00' * self.page_size)  # Zero out the page
                    f.flush()
        except Exception as e:
            print(f"Error deleting page {page_id}: {e}")
    
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
            
    def search(self, key: Any) -> Optional[Record]:
        leaf_node = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            return leaf_node.records[pos] 
        return None

    def insert(self, record: Record) -> bool:
        key = self.get_key_value(record)
        
        if self.search(key) is not None:
            return False  
            
        self.insert_recursive(self.load_page(self.root_page_id), key, record)
        return True

    def insert_recursive(self, node: Node, key: Any, record: Record):
        if isinstance(node, ClusteredLeafNode):
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.records.insert(pos, record)
            
            self.write_page(node.id, node)
            
            if node.is_full(self.max_keys):
                self.split_leaf(node)
        else:
            pos = bisect.bisect_right(node.keys, key)
            child_id = node.children[pos].id if hasattr(node.children[pos], 'id') else pos
            child = self.load_page(child_id)
            self.insert_recursive(child, key, record)
            
            if child.is_full(self.max_keys):
                if isinstance(child, ClusteredLeafNode):
                    self.split_leaf(child)
                else:
                    self.split_internal(child)
                    
    def delete(self, key: Any) -> bool:
        leaf = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf.keys, key)
        
        if pos >= len(leaf.keys) or leaf.keys[pos] != key:
            return False  # Key not found
        
        # remove key and record from leaf
        leaf.keys.pop(pos)
        leaf.records.pop(pos)
        
        self.write_page(leaf.id, leaf)
        
        if leaf != self.root and leaf.is_underflow(self.min_keys):
            self.handle_leaf_underflow(leaf)
        
        # root is empty
        if isinstance(self.root, ClusteredInternalNode) and len(self.root.keys) == 0:
            if len(self.root.children) > 0:
                old_root_id = self.root.id
                self.root = self.root.children[0]
                self.root.parent = None
                self.root_page_id = self.root.id
                
                if old_root_id is not None:
                    self.delete_page(old_root_id)
        return True

    def handle_leaf_underflow(self, leaf: ClusteredLeafNode):
        parent = leaf.parent
        if not parent:
            return
        
        # leaf position in parent
        leaf_index = parent.children.index(leaf)
        
        # borrow from left sibling
        if leaf_index > 0:
            left_sibling = parent.children[leaf_index - 1]
            if isinstance(left_sibling, ClusteredLeafNode) and len(left_sibling.keys) > self.min_keys:
                self.borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return
        
        # borrow from right sibling
        if leaf_index < len(parent.children) - 1:
            right_sibling = parent.children[leaf_index + 1]
            if isinstance(right_sibling, ClusteredLeafNode) and len(right_sibling.keys) > self.min_keys:
                self.borrow_from_right_leaf(leaf, right_sibling, parent, leaf_index)
                return
        
        # merge
        if leaf_index > 0:
            left_sibling = parent.children[leaf_index - 1]
            if isinstance(left_sibling, ClusteredLeafNode):
                self.merge_leaf_with_left(leaf, left_sibling, parent, leaf_index)
        else:
            right_sibling = parent.children[leaf_index + 1]
            if isinstance(right_sibling, ClusteredLeafNode):
                self.merge_leaf_with_right(leaf, right_sibling, parent, leaf_index)
    
    def borrow_from_left_leaf(self, leaf: ClusteredLeafNode, left_sibling: ClusteredLeafNode, 
                              parent: ClusteredInternalNode, leaf_index: int):
        # move last key and record from left sibling to beginning of leaf
        borrowed_key = left_sibling.keys.pop()
        borrowed_record = left_sibling.records.pop()
        
        leaf.keys.insert(0, borrowed_key)
        leaf.records.insert(0, borrowed_record)
        
        # update parent key
        parent.keys[leaf_index - 1] = leaf.keys[0]

    def borrow_from_right_leaf(self, leaf: ClusteredLeafNode, right_sibling: ClusteredLeafNode,
                               parent: ClusteredInternalNode, leaf_index: int):
        # move first key and record from right sibling to end of leaf
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_record = right_sibling.records.pop(0)
        
        leaf.keys.append(borrowed_key)
        leaf.records.append(borrowed_record)
        
        # update parent key
        parent.keys[leaf_index] = right_sibling.keys[0] if right_sibling.keys else borrowed_key

    def merge_leaf_with_left(self, leaf: ClusteredLeafNode, left_sibling: ClusteredLeafNode,
                             parent: ClusteredInternalNode, leaf_index: int):
        # move all keys and records from leaf to left sibling
        left_sibling.keys.extend(leaf.keys)
        left_sibling.records.extend(leaf.records)
        
        # update pointers
        left_sibling.next = leaf.next
        if leaf.next:
            leaf.next.previous = left_sibling
        
        # remove leaf from parent
        parent.children.pop(leaf_index)
        parent.keys.pop(leaf_index - 1)
        
        # remove leaf from pages
        self.delete_page(leaf.id)
        
        # handle parent underflow
        if parent != self.root and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def merge_leaf_with_right(self, leaf: ClusteredLeafNode, right_sibling: ClusteredLeafNode,
                              parent: ClusteredInternalNode, leaf_index: int):
        # move all keys and records from right sibling to leaf
        leaf.keys.extend(right_sibling.keys)
        leaf.records.extend(right_sibling.records)
        
        # update pointers
        leaf.next = right_sibling.next
        if right_sibling.next:
            right_sibling.next.previous = leaf
        
        # remove right sibling from parent
        parent.children.pop(leaf_index + 1)
        parent.keys.pop(leaf_index)
        
        # remove right sibling from disk
        self.delete_page(right_sibling.id)
        
        # handle parent underflow
        if parent != self.root and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def handle_internal_underflow(self, internal: ClusteredInternalNode):
        parent = internal.parent
        if not parent:
            return
        
        internal_index = parent.children.index(internal)
        
        # try to borrow from left sibling
        if internal_index > 0:
            left_sibling = parent.children[internal_index - 1]
            if isinstance(left_sibling, ClusteredInternalNode) and len(left_sibling.keys) > self.min_keys:
                self.borrow_from_left_internal(internal, left_sibling, parent, internal_index)
                return
        
        # try to borrow from right sibling
        if internal_index < len(parent.children) - 1:
            right_sibling = parent.children[internal_index + 1]
            if isinstance(right_sibling, ClusteredInternalNode) and len(right_sibling.keys) > self.min_keys:
                self.borrow_from_right_internal(internal, right_sibling, parent, internal_index)
                return
        
        # merge with sibling
        if internal_index > 0:
            left_sibling = parent.children[internal_index - 1]
            if isinstance(left_sibling, ClusteredInternalNode):
                self.merge_internal_with_left(internal, left_sibling, parent, internal_index)
        else:
            right_sibling = parent.children[internal_index + 1]
            if isinstance(right_sibling, ClusteredInternalNode):
                self.merge_internal_with_right(internal, right_sibling, parent, internal_index)

    def borrow_from_left_internal(self, internal: ClusteredInternalNode, left_sibling: ClusteredInternalNode,
                                  parent: ClusteredInternalNode, internal_index: int):
        separator_key = parent.keys[internal_index - 1]
        
        internal.keys.insert(0, separator_key)
        internal.children.insert(0, left_sibling.children.pop())
        internal.children[0].parent = internal
        
        parent.keys[internal_index - 1] = left_sibling.keys.pop()

    def borrow_from_right_internal(self, internal: ClusteredInternalNode, right_sibling: ClusteredInternalNode,
                                   parent: ClusteredInternalNode, internal_index: int):
        separator_key = parent.keys[internal_index]
        
        internal.keys.append(separator_key)
        internal.children.append(right_sibling.children.pop(0))
        internal.children[-1].parent = internal
        
        parent.keys[internal_index] = right_sibling.keys.pop(0)

    def merge_internal_with_left(self, internal: ClusteredInternalNode, left_sibling: ClusteredInternalNode,
                                 parent: ClusteredInternalNode, internal_index: int):
        separator_key = parent.keys[internal_index - 1]
        
        left_sibling.keys.append(separator_key)
        left_sibling.keys.extend(internal.keys)
        left_sibling.children.extend(internal.children)
        
        for child in internal.children:
            child.parent = left_sibling
        
        parent.children.pop(internal_index)
        parent.keys.pop(internal_index - 1)
        
        self.delete_page(internal.id)
        
        if parent != self.root and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)
            
    def merge_internal_with_right(self, internal: ClusteredInternalNode, right_sibling: ClusteredInternalNode,
                                  parent: ClusteredInternalNode, internal_index: int):
        separator_key = parent.keys[internal_index]
        
        internal.keys.append(separator_key)
        internal.keys.extend(right_sibling.keys)
        internal.children.extend(right_sibling.children)
        
        for child in right_sibling.children:
            child.parent = internal
        
        parent.children.pop(internal_index + 1)
        parent.keys.pop(internal_index)
        
        self.delete_page(right_sibling.id)
        
        if parent != self.root and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)
    
    def split_leaf(self, leaf: ClusteredLeafNode):
        mid = len(leaf.keys) // 2
        new_leaf = ClusteredLeafNode()
        
        # move half the keys and records to new leaf
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.records = leaf.records[mid:]
        new_leaf.parent = leaf.parent
        
        # update pointers
        new_leaf.next = leaf.next
        new_leaf.previous = leaf
        
        if leaf.next:
            leaf.next.previous = new_leaf
        leaf.next = new_leaf
        
        # keep first half in original leaf
        leaf.keys = leaf.keys[:mid]
        leaf.records = leaf.records[:mid]
        
        # assign page ID to new node and add to pages
        new_leaf.id = self.next_page_id
        self.write_page(self.next_page_id, new_leaf)
        self.next_page_id += 1
        
        # promote the first key of new leaf to parent
        promote_key = new_leaf.keys[0]
        self.promote_key(leaf, promote_key, new_leaf)
        
    def split_internal(self, internal: ClusteredInternalNode):
        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]
        
        new_internal = ClusteredInternalNode()
        
        # move keys and children to new internal node
        new_internal.keys = internal.keys[mid + 1:]
        new_internal.children = internal.children[mid + 1:]
        new_internal.parent = internal.parent
        
        # update parent pointers for moved children
        for child in new_internal.children:
            child.parent = new_internal
        
        # keep first half in original internal node
        internal.keys = internal.keys[:mid]
        internal.children = internal.children[:mid + 1]
        
        # assign page ID to new node and add to pages
        new_internal.id = self.next_page_id
        self.write_page(self.next_page_id, new_internal)
        self.next_page_id += 1
        
        # promote middle key to parent
        self.promote_key(internal, promote_key, new_internal)

    def promote_key(self, left_child: Node, key: Any, right_child: Node):
        if left_child.parent is None:
            # create new root
            new_root = ClusteredInternalNode()
            new_root.keys = [key]
            new_root.children = [left_child, right_child]
            left_child.parent = new_root
            right_child.parent = new_root
            
            # update root references
            self.root = new_root
            new_root.id = self.next_page_id
            self.write_page(self.next_page_id, new_root)
            self.next_page_id += 1
            self.root_page_id = new_root.id
            
        else:
            # insert in  parent
            parent = left_child.parent
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children.insert(pos + 1, right_child)
            right_child.parent = parent
            
            self.write_page(parent.id, parent)
            
            # Check if parent is now full
            if parent.is_full(self.max_keys):
                self.split_internal(parent)
                
    def find_leaf_node(self, key: Any) -> ClusteredLeafNode:
        current = self.load_page(self.root_page_id)
        while isinstance(current, ClusteredInternalNode):
            pos = bisect.bisect_right(current.keys, key)
            child_id = current.children[pos].id if hasattr(current.children[pos], 'id') else pos
            current = self.load_page(child_id)
        return current

    def get_key_value(self, record: Record) -> Any:
        return record.get_field_value(self.key_column)
    
    def range_search(self, start_key: Any, end_key: Any) -> List[Record]:
        results = []
        leaf = self.find_leaf_node(start_key)
        
        # starting position
        pos = bisect.bisect_left(leaf.keys, start_key)
        
        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_key:
                    return results
                if leaf.keys[i] >= start_key:
                    results.append(leaf.records[i])  
            

            next_leaf_id = leaf.next.id if leaf.next and hasattr(leaf.next, 'id') else None
            leaf = self.load_page(next_leaf_id) if next_leaf_id else None
            pos = 0
        
        return results
    
    def info_btree_clustered(self):
        stats = {
            "order": self.order,
            "max_keys": self.max_keys,
            "min_keys": self.min_keys,
            "total_pages": self.get_total_pages(),
            "root_page_id": self.root_page_id,
        }
        return stats

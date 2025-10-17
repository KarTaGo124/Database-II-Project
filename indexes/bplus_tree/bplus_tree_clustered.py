from typing import Any, List, Optional, Dict
import bisect
import pickle
import os
from ..core.record import Record
from ..core.performance_tracker import PerformanceTracker, OperationResult


class Node:
    def __init__(self, is_leaf: bool = False):
        self.is_leaf = is_leaf
        self.keys = []
        self.parent_id = None 
        self.id = None

    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) > max_keys
    
    def is_underflow(self, min_keys: int) -> bool:
        return len(self.keys) < min_keys


class ClusteredLeafNode(Node):
    def __init__(self):
        super().__init__(is_leaf=True)
        self.records = [] 
        self.previous_id = None  
        self.next_id = None  
class ClusteredInternalNode(Node):
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children_ids = []  
class BPlusTreeClusteredIndex:

    def __init__(self, order: int, key_column: str, file_path: str, record_class):
        self.key_column = key_column
        self.record_class = record_class
        self.order = order
        self.max_keys = order - 1
        self.min_keys = (order + 1) // 2 - 1
        self.file_path = file_path
        self.data_file = file_path + ".dat"
        self.page_size = 4096
        
        self.performance = PerformanceTracker()
        
        self.next_page_id = 1
        self.root_page_id = 0
        
        if not os.path.exists(self.data_file):
            root = ClusteredLeafNode()
            root.id = 0
            root.parent_id = None
            root.previous_id = None
            root.next_id = None
            
            with open(self.data_file, 'wb') as f:
                f.write(b'\x00' * self.page_size)
            
            self._write_page(0, root)
        

    
    def _load_page(self, page_id: int) -> Optional[Node]:
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
                
                if page_data == b'\x00' * self.page_size:
                    return None
                
                actual_end = len(page_data.rstrip(b'\x00'))
                if actual_end == 0:
                    return None
                    
                node_data = pickle.loads(page_data[:actual_end])
                
            if node_data['is_leaf']:
                node = ClusteredLeafNode()
                node.keys = node_data['keys']
                node.records = node_data['records']
                node.previous_id = node_data.get('previous_id')
                node.next_id = node_data.get('next_id')
            else:
                node = ClusteredInternalNode()
                node.keys = node_data['keys']
                node.children_ids = node_data['children_ids']  
                
            node.id = node_data['id']
            node.parent_id = node_data.get('parent_id')
            
            return node
            
        except Exception as e:
            print(f"Error loading page {page_id}: {e}")
            return None
    
    def _write_page(self, page_id: int, page: Node):
        self.performance.track_write()
        
        try:
            page_data = {
                'id': page.id,
                'is_leaf': page.is_leaf,
                'keys': page.keys,
                'parent_id': page.parent_id  
            }
            
            if isinstance(page, ClusteredLeafNode):
                page_data['records'] = page.records
                page_data['previous_id'] = page.previous_id
                page_data['next_id'] = page.next_id
            else:
                page_data['children_ids'] = page.children_ids 
                
            serialized_data = pickle.dumps(page_data, protocol=pickle.HIGHEST_PROTOCOL)
            
            if len(serialized_data) > self.page_size:
                raise Exception(f"Page data too large: {len(serialized_data)} > {self.page_size}")
            
            padded_data = serialized_data + b'\x00' * (self.page_size - len(serialized_data))
            
            offset = page_id * self.page_size
            
            if not os.path.exists(self.data_file):
                with open(self.data_file, 'wb') as f:
                    f.write(b'\x00' * self.page_size)
            
            current_size = os.path.getsize(self.data_file)
            required_size = (page_id + 1) * self.page_size
            
            if current_size < required_size:
                with open(self.data_file, 'ab') as f:
                    f.write(b'\x00' * (required_size - current_size))
            
            with open(self.data_file, 'r+b') as f:
                f.seek(offset)
                f.write(padded_data)
                f.flush()
                
        except Exception as e:
            print(f"Error writing page {page_id}: {e}")

    def _estimate_page_size(self, page: Node) -> int:
        try:
            page_data = {
                'id': page.id,
                'is_leaf': page.is_leaf,
                'keys': page.keys,
                'parent_id': page.parent_id
            }
            
            if isinstance(page, ClusteredLeafNode):
                page_data['records'] = page.records
                page_data['previous_id'] = page.previous_id
                page_data['next_id'] = page.next_id
            else:
                page_data['children_ids'] = page.children_ids
                
            serialized_data = pickle.dumps(page_data, protocol=pickle.HIGHEST_PROTOCOL)
            return len(serialized_data)
        except:
            return self.page_size + 1
    
    def _should_split(self, page: Node) -> bool:
   
        if page.is_full(self.max_keys):
            return True
        
        if isinstance(page, ClusteredLeafNode) and len(page.keys) > 1:
            estimated_size = self._estimate_page_size(page)
            if estimated_size > self.page_size * 0.85:
                return True
        
        return False
    
    def _delete_page(self, page_id: int):
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
        page_id = self.next_page_id
        self.next_page_id += 1
        return page_id
    
    def get_total_pages(self) -> int:
        if not os.path.exists(self.data_file):
            return 0
        
        file_size = os.path.getsize(self.data_file)
        return file_size // self.page_size
    
    def get_file_info(self) -> dict:
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
        return record.get_field_value(self.key_column)

            
    def search(self, key: Any) -> OperationResult:
        self.performance.start_operation()
        
        leaf_node = self._find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            record = leaf_node.records[pos]
            return self.performance.end_operation([record])
        
        return self.performance.end_operation([])

    def insert(self, record: Record) -> OperationResult:
        self.performance.start_operation()
        
        key = self.get_key_value(record)
        
        leaf_node = self._find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            return self.performance.end_operation(False) 
            
        self._insert_recursive(self.root_page_id, key, record)
        return self.performance.end_operation(True)

    def delete(self, key: Any) -> OperationResult:
        self.performance.start_operation()
        
        leaf = self._find_leaf_node(key)
        pos = bisect.bisect_left(leaf.keys, key)
        
        if pos >= len(leaf.keys) or leaf.keys[pos] != key:
            return self.performance.end_operation(False) 
        
        leaf.keys.pop(pos)
        leaf.records.pop(pos)
        
        self._write_page(leaf.id, leaf)
        
        if leaf.id != self.root_page_id and leaf.is_underflow(self.min_keys):
            self._handle_leaf_underflow(leaf)
        
        root = self._load_page(self.root_page_id)
        if isinstance(root, ClusteredInternalNode) and len(root.keys) == 0:
            if len(root.children_ids) > 0:
                old_root_id = root.id
                self.root_page_id = root.children_ids[0]
                
                new_root = self._load_page(self.root_page_id)
                new_root.parent_id = None
                self._write_page(self.root_page_id, new_root)
                
                self._delete_page(old_root_id)
        
        return self.performance.end_operation(True)
    
    def range_search(self, start_key: Any, end_key: Any) -> OperationResult:
        self.performance.start_operation()
        
        results = []
        leaf = self._find_leaf_node(start_key)
        
        pos = bisect.bisect_left(leaf.keys, start_key)
        
        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_key:
                    return self.performance.end_operation(results)
                if leaf.keys[i] >= start_key:
                    results.append(leaf.records[i])
            
            if leaf.next_id is not None:
                leaf = self._load_page(leaf.next_id)
                pos = 0
            else:
                break
        
        return self.performance.end_operation(results)
    
    def _find_leaf_node(self, key: Any) -> ClusteredLeafNode:
        current = self._load_page(self.root_page_id)
        
        while isinstance(current, ClusteredInternalNode):
            pos = bisect.bisect_right(current.keys, key)
            child_id = current.children_ids[pos]  
            current = self._load_page(child_id)
        
        return current
    
    def _insert_recursive(self, node_id: int, key: Any, record: Record):
        node = self._load_page(node_id)
        
        if isinstance(node, ClusteredLeafNode):
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.records.insert(pos, record)
            
            self._write_page(node.id, node)
            
            if self._should_split(node):
                self._split_leaf(node)
        else:
            pos = bisect.bisect_right(node.keys, key)
            child_id = node.children_ids[pos]
            child = self._load_page(child_id)
            
            self._insert_recursive(child_id, key, record)
            
            node = self._load_page(node_id)
            child = self._load_page(child_id)
            
            if self._should_split(child):
                if isinstance(child, ClusteredLeafNode):
                    self._split_leaf(child)
                else:
                    self._split_internal(child)
    
    def _split_leaf(self, leaf: ClusteredLeafNode):
   
        mid = len(leaf.keys) // 2
        new_leaf = ClusteredLeafNode()
        
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.records = leaf.records[mid:]
        new_leaf.parent_id = leaf.parent_id 
        
        new_leaf.next_id = leaf.next_id
        new_leaf.previous_id = leaf.id
        
        if leaf.next_id is not None:
            next_leaf = self._load_page(leaf.next_id)
            next_leaf.previous_id = None 
        
        leaf.next_id = None
        
        leaf.keys = leaf.keys[:mid]
        leaf.records = leaf.records[:mid]
        
        
        new_leaf.id = self._allocate_page_id()
        
        
        leaf.next_id = new_leaf.id
        if new_leaf.next_id is not None:
            next_leaf = self._load_page(new_leaf.next_id)
            next_leaf.previous_id = new_leaf.id
            self._write_page(next_leaf.id, next_leaf)
        
        self._write_page(new_leaf.id, new_leaf)
        
        
        promote_key = new_leaf.keys[0]
        self._promote_key_and_write_left(leaf, promote_key, new_leaf.id)
    
    def _split_internal(self, internal: ClusteredInternalNode):
        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]
        
        new_internal = ClusteredInternalNode()
        
        new_internal.keys = internal.keys[mid + 1:]
        new_internal.children_ids = internal.children_ids[mid + 1:]  
        new_internal.parent_id = internal.parent_id  
        
        internal.keys = internal.keys[:mid]
        internal.children_ids = internal.children_ids[:mid + 1]  
        
        new_internal.id = self._allocate_page_id()
        
        for child_id in new_internal.children_ids:
            child = self._load_page(child_id)
            child.parent_id = new_internal.id
            self._write_page(child_id, child)
        
        self._write_page(new_internal.id, new_internal)
        
        
        self._promote_key_and_write_left(internal, promote_key, new_internal.id)
    
    def _promote_key_and_write_left(self, left_child: Node, key: Any, right_child_id: int):
   
        left_child_id = left_child.id
        
        
        if left_child.parent_id is None:
            new_root = ClusteredInternalNode()
            new_root.keys = [key]
            new_root.children_ids = [left_child_id, right_child_id]
            new_root.parent_id = None
            
            old_root_id = left_child_id
            new_root.id = 0  
            
            if old_root_id == 0:
                new_left_id = self._allocate_page_id()
                left_child.id = new_left_id
                left_child_id = new_left_id
                new_root.children_ids[0] = new_left_id
            
            left_child.parent_id = new_root.id
            right_child = self._load_page(right_child_id)
            right_child.parent_id = new_root.id
            
            self._write_page(left_child_id, left_child)
            self._write_page(right_child_id, right_child)
            self._write_page(new_root.id, new_root)
            
            self.root_page_id = 0
        else:
            parent = self._load_page(left_child.parent_id)
            
            
            if not isinstance(parent, ClusteredInternalNode):
                raise ValueError(f"Parent must be an internal node, got {type(parent)}")
            
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children_ids.insert(pos + 1, right_child_id)
            
            right_child = self._load_page(right_child_id)
            right_child.parent_id = parent.id
            self._write_page(right_child_id, right_child)
            
            self._write_page(left_child_id, left_child)
            self._write_page(parent.id, parent)
            
            if parent.is_full(self.max_keys):
                self._split_internal(parent)
    
    def _promote_key(self, left_child_id: int, key: Any, right_child_id: int):
        left_child = self._load_page(left_child_id)
        self._promote_key_and_write_left(left_child, key, right_child_id)
    
    def _handle_leaf_underflow(self, leaf: ClusteredLeafNode):
        if leaf.parent_id is None:
            return
        
        parent = self._load_page(leaf.parent_id)
        
        leaf_index = parent.children_ids.index(leaf.id)
        
        if leaf_index > 0:
            left_sibling_id = parent.children_ids[leaf_index - 1]
            left_sibling = self._load_page(left_sibling_id)
            if isinstance(left_sibling, ClusteredLeafNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return
        
        if leaf_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[leaf_index + 1]
            right_sibling = self._load_page(right_sibling_id)
            if isinstance(right_sibling, ClusteredLeafNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_leaf(leaf, right_sibling, parent, leaf_index)
                return
        
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
        borrowed_key = left_sibling.keys.pop()
        borrowed_record = left_sibling.records.pop()
        leaf.keys.insert(0, borrowed_key)
        leaf.records.insert(0, borrowed_record)
        parent.keys[leaf_index - 1] = leaf.keys[0]
        self._write_page(left_sibling.id, left_sibling)
        self._write_page(leaf.id, leaf)
        self._write_page(parent.id, parent)

    def _borrow_from_right_leaf(self, leaf: ClusteredLeafNode, right_sibling: ClusteredLeafNode,
                                parent: ClusteredInternalNode, leaf_index: int):
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_record = right_sibling.records.pop(0)
        leaf.keys.append(borrowed_key)
        leaf.records.append(borrowed_record)
        parent.keys[leaf_index] = right_sibling.keys[0] if right_sibling.keys else borrowed_key
        self._write_page(right_sibling.id, right_sibling)
        self._write_page(leaf.id, leaf)
        self._write_page(parent.id, parent)

    def _merge_leaf_with_left(self, leaf: ClusteredLeafNode, left_sibling: ClusteredLeafNode,
                              parent: ClusteredInternalNode, leaf_index: int):
        left_sibling.keys.extend(leaf.keys)
        left_sibling.records.extend(leaf.records)
        left_sibling.next_id = leaf.next_id
        if leaf.next_id is not None:
            next_leaf = self._load_page(leaf.next_id)
            next_leaf.previous_id = left_sibling.id
            self._write_page(next_leaf.id, next_leaf)
        parent.children_ids.pop(leaf_index)
        parent.keys.pop(leaf_index - 1)
        self._write_page(left_sibling.id, left_sibling)
        self._write_page(parent.id, parent)
        self._delete_page(leaf.id)
        if parent.id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _merge_leaf_with_right(self, leaf: ClusteredLeafNode, right_sibling: ClusteredLeafNode,
                               parent: ClusteredInternalNode, leaf_index: int):
        leaf.keys.extend(right_sibling.keys)
        leaf.records.extend(right_sibling.records)
        leaf.next_id = right_sibling.next_id
        if right_sibling.next_id is not None:
            next_leaf = self._load_page(right_sibling.next_id)
            next_leaf.previous_id = leaf.id
            self._write_page(next_leaf.id, next_leaf)
        parent.children_ids.pop(leaf_index + 1)
        parent.keys.pop(leaf_index)
        self._write_page(leaf.id, leaf)
        self._write_page(parent.id, parent)
        self._delete_page(right_sibling.id)
        if parent.id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _handle_internal_underflow(self, internal: ClusteredInternalNode):
        if internal.parent_id is None:
            return
        
        parent = self._load_page(internal.parent_id)
        internal_index = parent.children_ids.index(internal.id)
        
        if internal_index > 0:
            left_sibling_id = parent.children_ids[internal_index - 1]
            left_sibling = self._load_page(left_sibling_id)
            if isinstance(left_sibling, ClusteredInternalNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_internal(internal, left_sibling, parent, internal_index)
                return
        
        if internal_index < len(parent.children_ids) - 1:
            right_sibling_id = parent.children_ids[internal_index + 1]
            right_sibling = self._load_page(right_sibling_id)
            if isinstance(right_sibling, ClusteredInternalNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_internal(internal, right_sibling, parent, internal_index)
                return
        
        
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
        separator_key = parent.keys[internal_index - 1]
        internal.keys.insert(0, separator_key)
        borrowed_child_id = left_sibling.children_ids.pop()
        internal.children_ids.insert(0, borrowed_child_id)
        borrowed_child = self._load_page(borrowed_child_id)
        borrowed_child.parent_id = internal.id
        self._write_page(borrowed_child_id, borrowed_child)
        parent.keys[internal_index - 1] = left_sibling.keys.pop()
        self._write_page(left_sibling.id, left_sibling)
        self._write_page(internal.id, internal)
        self._write_page(parent.id, parent)

    def _borrow_from_right_internal(self, internal: ClusteredInternalNode, right_sibling: ClusteredInternalNode,
                                    parent: ClusteredInternalNode, internal_index: int):
        separator_key = parent.keys[internal_index]
        internal.keys.append(separator_key)
        borrowed_child_id = right_sibling.children_ids.pop(0)
        internal.children_ids.append(borrowed_child_id)
        borrowed_child = self._load_page(borrowed_child_id)
        borrowed_child.parent_id = internal.id
        self._write_page(borrowed_child_id, borrowed_child)
        parent.keys[internal_index] = right_sibling.keys.pop(0)
        self._write_page(right_sibling.id, right_sibling)
        self._write_page(internal.id, internal)
        self._write_page(parent.id, parent)

    def _merge_internal_with_left(self, internal: ClusteredInternalNode, left_sibling: ClusteredInternalNode,
                                  parent: ClusteredInternalNode, internal_index: int):
        separator_key = parent.keys[internal_index - 1]
        
        left_sibling.keys.append(separator_key)
        left_sibling.keys.extend(internal.keys)
        left_sibling.children_ids.extend(internal.children_ids)
        
        for child_id in internal.children_ids:
            child = self._load_page(child_id)
            child.parent_id = left_sibling.id
            self._write_page(child_id, child)
        
        parent.children_ids.pop(internal_index)
        parent.keys.pop(internal_index - 1)
        
        self._write_page(left_sibling.id, left_sibling)
        self._write_page(parent.id, parent)
        self._delete_page(internal.id)
        
        if parent.id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _merge_internal_with_right(self, internal: ClusteredInternalNode, right_sibling: ClusteredInternalNode,
                                   parent: ClusteredInternalNode, internal_index: int):
        separator_key = parent.keys[internal_index]
        
        internal.keys.append(separator_key)
        internal.keys.extend(right_sibling.keys)
        internal.children_ids.extend(right_sibling.children_ids)
        
        for child_id in right_sibling.children_ids:
            child = self._load_page(child_id)
            child.parent_id = internal.id
            self._write_page(child_id, child)
        
        parent.children_ids.pop(internal_index + 1)
        parent.keys.pop(internal_index)
        
        self._write_page(internal.id, internal)
        self._write_page(parent.id, parent)
        self._delete_page(right_sibling.id)
        
        if parent.id != self.root_page_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)
    
    def scan_all(self) -> OperationResult:
    
        self.performance.start_operation()
        results = []
        
        current = self._load_page(self.root_page_id)
        while isinstance(current, ClusteredInternalNode):
            if len(current.children_ids) > 0:
                current = self._load_page(current.children_ids[0])
            else:
                break
        
        while current is not None and isinstance(current, ClusteredLeafNode):
            results.extend(current.records)
            
            if current.next_id is not None:
                current = self._load_page(current.next_id)
            else:
                current = None
        
        return self.performance.end_operation(results)
    
    def drop_table(self):
        if os.path.exists(self.data_file):
            os.remove(self.data_file)
            
        self.next_page_id = 1
        self.root_page_id = 0
        
        root = ClusteredLeafNode()
        root.id = 0
        root.parent_id = None
        root.previous_id = None
        root.next_id = None
        
        with open(self.data_file, 'wb') as f:
            f.write(b'\x00' * self.page_size)
        
        self._write_page(0, root)
    
    def info_btree_clustered(self) -> dict:
        stats = {
            "order": self.order,
            "max_keys": self.max_keys,
            "min_keys": self.min_keys,
            "total_pages": self.get_total_pages(),
            "root_page_id": self.root_page_id,
            "next_page_id": self.next_page_id,
        }
        return stats

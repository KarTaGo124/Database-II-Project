from typing import Any, List, Optional, Tuple, Union
import bisect
import pickle
import os


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
        
        # for pages
        self.pages = {}
        self.next_page_id = 1
        self.root_page_id = 0
        self.root.id = 0
        self.pages[0] = self.root
        
        # for existen b+tree
        self.load_tree()
    def search(self, key: Any) -> Optional['Record']:
        leaf_node = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            return leaf_node.records[pos] 
        return None

    def insert(self, record: 'Record') -> bool:
        key = self.get_key_value(record)
        
        if self.search(key) is not None:
            return False  
            
        self.insert_recursive(self.root, key, record)
        self.save_tree()
        return True

    def insert_recursive(self, node: Node, key: Any, record: 'Record'):
        if isinstance(node, ClusteredLeafNode):
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.records.insert(pos, record)
            
            if node.is_full(self.max_keys):
                self.split_leaf(node)
        else:
            pos = bisect.bisect_right(node.keys, key)
            child = node.children[pos]
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
        
        if leaf != self.root and leaf.is_underflow(self.min_keys):
            self._handle_leaf_underflow(leaf)
        
        # root is empty
        if isinstance(self.root, ClusteredInternalNode) and len(self.root.keys) == 0:
            if len(self.root.children) > 0:
                old_root_id = self.root.id
                self.root = self.root.children[0]
                self.root.parent = None
                self.root_page_id = self.root.id
                
                if old_root_id in self.pages:
                    del self.pages[old_root_id]
        
        self.save_tree()
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
        
        if internal.id in self.pages:
            del self.pages[internal.id]
        
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
        
        if right_sibling.id in self.pages:
            del self.pages[right_sibling.id]
        
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
        self.pages[self.next_page_id] = new_leaf
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
        self.pages[self.next_page_id] = new_internal
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
            self.pages[self.next_page_id] = new_root
            self.next_page_id += 1
            self.root_page_id = new_root.id
            
        else:
            # insert in  parent
            parent = left_child.parent
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children.insert(pos + 1, right_child)
            right_child.parent = parent
            
            # Check if parent is now full
            if parent.is_full(self.max_keys):
                self.split_internal(parent)
                
    def find_leaf_node(self, key: Any) -> ClusteredLeafNode:
        current = self.root
        while isinstance(current, ClusteredInternalNode):
            pos = bisect.bisect_right(current.keys, key)
            current = current.children[pos]
        return current

    def get_key_value(self, record: 'Record') -> Any:
        return record.get_field_value(self.key_column)
    
    def range_search(self, start_key: Any, end_key: Any) -> List['Record']:
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
            
            leaf = leaf.next
            pos = 0
        
        return results
    
    def save_tree(self):
        try:
            with open(self.file_path, 'wb') as f:
                tree_data = {
                    'root_page_id': self.root_page_id,
                    'pages': self.pages,
                    'next_page_id': self.next_page_id,
                    'order': self.order,
                    'key_column': self.key_column,
                    'first_leaf_id': self.first_leaf.id if self.first_leaf else None
                }
                pickle.dump(tree_data, f)
        except Exception as e:
            print(f"Error saving clustered tree {e}")

    def load_tree(self):
        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, 'rb') as f:
                    tree_data = pickle.load(f)
                    self.root_page_id = tree_data['root_page_id']
                    self.pages = tree_data['pages']
                    self.next_page_id = tree_data['next_page_id']
                    self.root = self.pages[self.root_page_id]
                    
                    first_leaf_id = tree_data.get('first_leaf_id')
                    if first_leaf_id is not None and first_leaf_id in self.pages:
                        self.first_leaf = self.pages[first_leaf_id]
                    else:
                        current = self.root
                        while isinstance(current, ClusteredInternalNode):
                            if current.children:
                                current = current.children[0]
                            else:
                                break
                        self.first_leaf = current if isinstance(current, ClusteredLeafNode) else self.root
        except Exception as e:
            print(f"Error loading clustered tree {e}")

    def load_tree(self):
        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, 'rb') as f:
                    tree_data = pickle.load(f)
                    self.root_page_id = tree_data['root_page_id']
                    self.pages = tree_data['pages']
                    self.next_page_id = tree_data['next_page_id']
                    self.root = self.pages[self.root_page_id]
                    
                    # first_leaf pointer
                    first_leaf_id = tree_data.get('first_leaf_id')
                    if first_leaf_id is not None and first_leaf_id in self.pages:
                        self.first_leaf = self.pages[first_leaf_id]
                    else:
                        # first leaf manually
                        current = self.root
                        while isinstance(current, ClusteredInternalNode):
                            if current.children:
                                current = current.children[0]
                            else:
                                break
                        self.first_leaf = current if isinstance(current, ClusteredLeafNode) else self.root
        except Exception as e:
            print(f"Error loading clustered tree {e}")

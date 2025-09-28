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

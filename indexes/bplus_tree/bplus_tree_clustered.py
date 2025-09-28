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
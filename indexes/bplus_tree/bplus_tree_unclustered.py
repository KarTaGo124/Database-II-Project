from core.record import Table, Record
from bplus_tree import Node, LeafNode, InternalNode
import pickle
import os
from typing import Any, List, Optional, Tuple, Union
from dataclasses import dataclass
import bisect

class RecordPointer:
    def __init__(self, page_number: int, slot_number: int):
        self.page_number = page_number
        self.slot_number = slot_number

class BPlusTreeUnclusteredIndex:
    def __init__(self, order:int, index_column: str, file_path: str):
        self.index_column = index_column
        self.root = LeafNode()  # Root node of the B+ tree
        self.max_keys = order - 1  # Maximum keys for node
        self.order = order
        self.key_field = index_column
        self.file_path = file_path
        self.first_leaf = self.root  # Pointer to the first leaf node
    
    def search(self, key: Any) ->Optional[RecordPointer]:
        leaf_node = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            record_position = leaf_node.values[pos]
            return self.load_from_record_pointer(record_position)
        return None
    def insert(self, record: Record, record_pointer: RecordPointer):
        key = self.get_key_value(record)
        self.insert_recursive(self.root, key, record_pointer)
        self.save_tree()
    
    def inser_recursive(self, node: Node, key: Any, record_pointer: RecordPointer):
        if node.is_leaf:
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.values.insert(pos, record_pointer)
            if node.is_full(self.max_keys):
                self.split_leaf(node)
        else:
            index = bisect.bisect_right(node.keys, key)
            child = node.values[index]
            self.insert_recursive(child, key, record_pointer)
            if child.is_full(self.max_keys):
                self.split_internal(child)
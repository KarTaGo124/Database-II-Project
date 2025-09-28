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
    def __str__(self):
        return f"RecordPointer(page={self.page_number}, slot = {self.slot_number})"
    def __repr__(self):
        return self.__str__()
    def __eq__(self, other):
        return (isinstance(other, RecordPointer) and self.page_number == other.page_number
                and self.slot_number == other.slot_number
                ) 
    

class BPlusTreeUnclusteredIndex:
    def __init__(self, order:int, index_column: str, file_path: str):
        self.index_column = index_column
        self.root = LeafNode()  # Root node of the B+ tree
        self.order = order
        self.max_keys = order - 1
        self.min_keys = (order + 1) // 2 - 1
        self.key_field = index_column
        self.file_path = file_path
        self.first_leaf = self.root  # Pointer to the first leaf node

        self.pages = {}
        self.next_page_id = 1
        self.root_page_id = 1
        self.root.id = 0
        self.pages[0] = self.root
        
        self.load_tree()
    
    
    def search(self, key: Any) ->Optional[RecordPointer]:
        leaf_node = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            record_position = leaf_node.values[pos]
            return self.load_from_record_pointer(record_position)
        return None
    
    def insert(self, record: Record, record_pointer: RecordPointer)-> bool:
        key = self.get_key_value(record)
        if self.search(key) is not None:
            return False
        
        self.insert_recursive(self.root, key, record_pointer)
        self.save_tree()
        return True
    
    def insert_recursive(self, node: Node, key: Any, record_pointer: RecordPointer):
        """Recursively insert key and record pointer"""
        if isinstance(node, LeafNode):
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos,key)
            node.values.insert(pos, record_pointer)
            # Check if leaf node is full            
            if node.is_full(self.max_keys):
                self.split_leaf(node)
                
        else:
            pos = bisect.bisect_right(node.keys, key)
            
            child = node.children[pos]
            self.insert_recursive(child, key, record_pointer)
            
            if child.is_full(self.max_keys):
                if isinstance(child, LeafNode):
                    self.split_leaf(child)
                else:
                    self.split_internal(child)
     
    def update(self, old_key: Any, new_record: 'Record', new_record_pointer: RecordPointer) -> bool:               
        new_key = self.get_key_value(new_record)
        
        if old_key == new_key:
            leaf_node = self.find_leaf_node(old_key)
            pos = bisect.bisect_left(leaf_node.keys, old_key)
            if pos < len(leaf_node.keys) and leaf_node.keys[pos] == old_key:
                leaf_node.values[pos] = new_record_pointer
                self.save_tree()
                return True
            return False
        else:
            if self.delete(old_key):
                return self.insert(new_record, new_record_pointer)
            return False
    
    def delete(self, key: Any) -> bool:
        leaf = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf.keys, key)
        
        if pos > len(leaf.keys) or leaf.keys[pos] != key:
            return False
        # Remove form leaf
        leaf.keys.pop(pos)
        leaf.values.pop(pos)
        
        if leaf != self.root and leaf.is_underflow(self.min_keys):
            self.handle_leaf_underflow(leaf)
        
        if isinstance(self.root, InternalNode) and len(self.root.keys) == 0:
            if len(self.root.children) > 0:
                old_root_id = self.root.id
                self.root = self.root.children[0]
                self.root.parent = None
                self.root_page_id = self.root.id
                
                if old_root_id in self.pages:
                    del self.pages[old_root_id]
                
        self.save_tree()
        return True
    
    
    def handle_leaf_underflow(self, leaf:LeafNode):
        parent = leaf.parent
        if not parent:
            return
        
        leaf_index = parent.children.index(leaf)
        # borrown from left sibling
        if leaf_index > 0:
            left_sibling = parent.children[leaf_index - 1]
            if isinstance(left_sibling, LeafNode) and len(left_sibling.keys) > self.min_keys:
                self.borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return
        # borrown from right sibling    
        if leaf_index < len(parent.children) - 1:
            right_sibling = parent.children[leaf_index + 1]
            if isinstance(right_sibling, LeafNode) and len(right_sibling.keys) > self.min_keys:
                self.borrow_from_right_leaf(leaf, right_sibling, parent,leaf_index)
                return
        # merge if can not borrow
        if leaf_index > 0:
            # merge left
            left_sibling = parent.children[leaf_index - 1]
            if isinstance(left_sibling, LeafNode):
                self.merge_leaf_with_left(leaf, left_sibling, parent, leaf_index)
        else:
            # merge right
            right_sibling = parent.children[leaf_index + 1]
            if isinstance(right_sibling, LeafNode):
                self.merge_leaf_with_right(leaf, right_sibling,parent,leaf_index)
    
    def borrow_from_left_leaf(self, leaf:LeafNode, left_sibling:LeafNode, parent:InternalNode, leaf_index:int):
        # move last key form left to beggining leaf
        borrowed_key = left_sibling.keys.pop()
        borrowed_values = left_sibling.values.pop()
        
        leaf.keys.insert(0, borrowed_key)
        leaf.values.insert(0, borrowed_values)
        
        # update parent key
        parent.keys[leaf_index - 1] = leaf.keys[0]
    
    def borrow_from_right_leaf(self, leaf: LeafNode, right_sibling: LeafNode, parent: InternalNode, leaf_index:int)
        
                
            
from typing import Any, List, Optional, Tuple, Union
import bisect
import pickle
import os
import time
from ..core.performance_tracker import PerformanceTracker


class Node:
    def __init__(self, is_leaf: bool=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.parent = None  # Parent node
        self.id = None  # Unique identifier for the node (e.g., page number)
    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) > max_keys
    
    def is_underflow(self, min_keys:int)-> bool:
        return len(self.keys) < min_keys

class LeafNode(Node):
    def __init__(self):
        super().__init__(is_leaf=True)
        self.values = []  # List of RecordPointer objects
        self.previous = None  # Pointer to previous leaf node
        self.next = None  # Pointer to next leaf node


class InternalNode(Node):
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children = []  # Pointers to child nodes




class RecordPointer:
    def __init__(self, page_number: int, slot_number: int):
        self.page_number = page_number
        self.slot_number = slot_number

    def __str__(self):
        return f"RecordPointer(page={self.page_number}, slot={self.slot_number})"

    def __repr__(self):
        return self.__str__()
    
    def __eq__(self, other):
        return (isinstance(other, RecordPointer) and 
                self.page_number == other.page_number and 
                self.slot_number == other.slot_number)

class BPlusTreeUnclusteredIndex:
    def __init__(self, order: int, index_column: str, file_path: str):
        self.index_column = index_column
        self.root = LeafNode()  # Root node
        self.order = order
        self.max_keys = order - 1  # Maximum keys for node
        self.min_keys = (order + 1) // 2 - 1  # Minimum keys for non-root nodes
        self.key_field = index_column
        self.file_path = file_path
        self.first_leaf = self.root  # Pointer to the first leaf node
        
        # Performance tracking
        self.performance = PerformanceTracker()
        
        # for pages - pure disk I/O without cache
        self.pages = {}  # page_id  
        self.next_page_id = 1
        self.root_page_id = 0
        self.root.id = 0
        self.pages[0] = self.root
        
        # load bplustree if it exists
        self.load_tree()
        
    def load_page(self, page_id: int) -> Node:
        """Load a page from disk with performance tracking - NO CACHE"""
        # Track disk read EVERY TIME
        self.performance.track_read()
        
        # Simulate disk I/O delay (1ms for realistic testing)
        time.sleep(0.001)
        
        if page_id in self.pages:
            return self.pages[page_id]
        
        return None
    
    def write_page(self, page_id: int, page: Node):
        """Write a page to disk with performance tracking - NO CACHE"""
        # Track disk write EVERY TIME
        self.performance.track_write()
        
        # Simulate disk I/O delay
        time.sleep(0.001)
        
        self.pages[page_id] = page

    def search(self, key: Any) -> Optional[RecordPointer]:
        leaf_node = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf_node.keys, key)
        
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            record_pointer = leaf_node.values[pos]
            return record_pointer
        return None

    def insert(self, record, record_pointer: RecordPointer) -> bool:
        key = self.get_key_value(record)
        
        # search if exxists
        if self.search(key) is not None:
            return False 
            
        self.insert_recursive(self.load_page(self.root_page_id), key, record_pointer)
        self.save_tree()
        return True

    def insert_recursive(self, node: Node, key: Any, record_pointer: RecordPointer):
        if isinstance(node, LeafNode):
            #  insert in leaf node
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.values.insert(pos, record_pointer)
            
            # Write modified page back to disk
            self.write_page(node.id, node)
            
            # split if is full
            if node.is_full(self.max_keys):
                self.split_leaf(node)
        else:
            # correct child
            pos = bisect.bisect_right(node.keys, key)
            child_id = node.children[pos].id if hasattr(node.children[pos], 'id') else pos
            child = self.load_page(child_id)
            self.insert_recursive(child, key, record_pointer)
            
            # split if is full
            if child.is_full(self.max_keys):
                if isinstance(child, LeafNode):
                    self.split_leaf(child)
                else:
                    self.split_internal(child)

    def update(self, old_key: Any, new_record, new_record_pointer: RecordPointer) -> bool:
        new_key = self.get_key_value(new_record)
        
        # just update
        if old_key == new_key:
            leaf_node = self.find_leaf_node(old_key)
            pos = bisect.bisect_left(leaf_node.keys, old_key)
            
            if pos < len(leaf_node.keys) and leaf_node.keys[pos] == old_key:
                leaf_node.values[pos] = new_record_pointer
                self.save_tree()
                return True
            return False
        else:
            # delete and insert new
            if self.delete(old_key):
                return self.insert(new_record, new_record_pointer)
            return False

    def delete(self, key: Any) -> bool:
        leaf = self.find_leaf_node(key)
        pos = bisect.bisect_left(leaf.keys, key)
        
        if pos >= len(leaf.keys) or leaf.keys[pos] != key:
            return False  # key does not exists
        
        # remove from leaf
        leaf.keys.pop(pos)
        leaf.values.pop(pos)
        
        # underflow
        if leaf != self.root and leaf.is_underflow(self.min_keys):
            self.handle_leaf_underflow(leaf)
        
        # for root is empty
        if isinstance(self.root, InternalNode) and len(self.root.keys) == 0:
            if len(self.root.children) > 0:
                old_root_id = self.root.id
                self.root = self.root.children[0]
                self.root.parent = None
                self.root_page_id = self.root.id
                
                # remove old root
                if old_root_id in self.pages:
                    del self.pages[old_root_id]
        
        self.save_tree()
        return True

    def handle_leaf_underflow(self, leaf: LeafNode):
        parent = leaf.parent
        if not parent:
            return
        
        # leaf position in parent
        leaf_index = parent.children.index(leaf)
        
        # borrow from left sibling
        if leaf_index > 0:
            left_sibling = parent.children[leaf_index - 1]
            if isinstance(left_sibling, LeafNode) and len(left_sibling.keys) > self.min_keys:
                self.borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return
        
        # borrow from right sibling
        if leaf_index < len(parent.children) - 1:
            right_sibling = parent.children[leaf_index + 1]
            if isinstance(right_sibling, LeafNode) and len(right_sibling.keys) > self.min_keys:
                self.borrow_from_right_leaf(leaf, right_sibling, parent, leaf_index)
                return
        
        # merge in case can not borrow
        if leaf_index > 0:
            # merge with left sibling
            left_sibling = parent.children[leaf_index - 1]
            if isinstance(left_sibling, LeafNode):
                self.merge_leaf_with_left(leaf, left_sibling, parent, leaf_index)
        else:
            # merge with right sibling
            right_sibling = parent.children[leaf_index + 1]
            if isinstance(right_sibling, LeafNode):
                self.merge_leaf_with_right(leaf, right_sibling, parent, leaf_index)

    def borrow_from_left_leaf(self, leaf: LeafNode, left_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        # move left to leaf
        borrowed_key = left_sibling.keys.pop()
        borrowed_value = left_sibling.values.pop()
        
        leaf.keys.insert(0, borrowed_key)
        leaf.values.insert(0, borrowed_value)
        
        # update parent key
        parent.keys[leaf_index - 1] = leaf.keys[0]

    def borrow_from_right_leaf(self, leaf: LeafNode, right_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        # move from right to leaf
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_value = right_sibling.values.pop(0)
        
        leaf.keys.append(borrowed_key)
        leaf.values.append(borrowed_value)
        
        # update parent key
        parent.keys[leaf_index] = right_sibling.keys[0] if right_sibling.keys else borrowed_key

    def merge_leaf_with_left(self, leaf: LeafNode, left_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        # move from left_siblings to left
        left_sibling.keys.extend(leaf.keys)
        left_sibling.values.extend(leaf.values)
        
        # update pointers
        left_sibling.next = leaf.next
        if leaf.next:
            leaf.next.previous = left_sibling
        
        # remove leaf from parent
        parent.children.pop(leaf_index)
        parent.keys.pop(leaf_index - 1)
        
        # remove leaf from pages
        if leaf.id in self.pages:
            del self.pages[leaf.id]
        
        # underflow
        if parent != self.root and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def merge_leaf_with_right(self, leaf: LeafNode, right_sibling: LeafNode, parent: InternalNode, leaf_index: int):
        # move from right sibling to leaf
        leaf.keys.extend(right_sibling.keys)
        leaf.values.extend(right_sibling.values)
        
        # update pointers
        leaf.next = right_sibling.next
        if right_sibling.next:
            right_sibling.next.previous = leaf
        
        # remove right sibling from parent
        parent.children.pop(leaf_index + 1)
        parent.keys.pop(leaf_index)
        
        # remove right sibling from pages
        if right_sibling.id in self.pages:
            del self.pages[right_sibling.id]
        
        # underflow
        if parent != self.root and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def handle_internal_underflow(self, internal: InternalNode):
        parent = internal.parent
        if not parent:
            return
        
        # internal position in parent
        internal_index = parent.children.index(internal)
        
        # borrow from left sibling
        if internal_index > 0:
            left_sibling = parent.children[internal_index - 1]
            if isinstance(left_sibling, InternalNode) and len(left_sibling.keys) > self.min_keys:
                self.borrow_from_left_internal(internal, left_sibling, parent, internal_index)
                return
        
        # borrow from right sibling
        if internal_index < len(parent.children) - 1:
            right_sibling = parent.children[internal_index + 1]
            if isinstance(right_sibling, InternalNode) and len(right_sibling.keys) > self.min_keys:
                self.borrow_from_right_internal(internal, right_sibling, parent, internal_index)
                return
        
        # merge if can not borrow
        if internal_index > 0:
            # merge with left sibling
            left_sibling = parent.children[internal_index - 1]
            if isinstance(left_sibling, InternalNode):
                self._merge_internal_with_left(internal, left_sibling, parent, internal_index)
        else:
            # merge with right sibling
            right_sibling = parent.children[internal_index + 1]
            if isinstance(right_sibling, InternalNode):
                self._merge_internal_with_right(internal, right_sibling, parent, internal_index)

    def borrow_from_left_internal(self, internal: InternalNode, left_sibling: InternalNode, parent: InternalNode, internal_index: int):
        # separator key from parent
        separator_key = parent.keys[internal_index - 1]
        
        # move last key from left sibling to parent
        parent.keys[internal_index - 1] = left_sibling.keys.pop()
        
        # move last child from left sibling to internal
        borrowed_child = left_sibling.children.pop()
        borrowed_child.parent = internal
        
        # insert separator key and borrowed child to internal
        internal.keys.insert(0, separator_key)
        internal.children.insert(0, borrowed_child)

    def borrow_from_right_internal(self, internal: InternalNode, right_sibling: InternalNode, parent: InternalNode, internal_index: int):
        # separator key from parent
        separator_key = parent.keys[internal_index]
        
        # move the first key from right sibling to parent
        parent.keys[internal_index] = right_sibling.keys.pop(0)
        
        # move the first child from right sibling to internal
        borrowed_child = right_sibling.children.pop(0)
        borrowed_child.parent = internal
        
        # add separator key and borrowed child to internal
        internal.keys.append(separator_key)
        internal.children.append(borrowed_child)

    def _merge_internal_with_left(self, internal: InternalNode, left_sibling: InternalNode, parent: InternalNode, internal_index: int):
        # separator key from parent
        separator_key = parent.keys[internal_index - 1]
        
        # merge left_sibling + separator + internal
        left_sibling.keys.append(separator_key)
        left_sibling.keys.extend(internal.keys)
        left_sibling.children.extend(internal.children)
        
        # update parent pointers for moved children
        for child in internal.children:
            child.parent = left_sibling
        
        # remove internal from parent
        parent.children.pop(internal_index)
        parent.keys.pop(internal_index - 1)
        
        # remove internal from pages
        if internal.id in self.pages:
            del self.pages[internal.id]
        
        if parent != self.root and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def _merge_internal_with_right(self, internal: InternalNode, right_sibling: InternalNode, parent: InternalNode, internal_index: int):
        # separator key from parent
        separator_key = parent.keys[internal_index]
        
        # merge internal + separator + right_sibling
        internal.keys.append(separator_key)
        internal.keys.extend(right_sibling.keys)
        internal.children.extend(right_sibling.children)
        
        # update parent pointers for moved children
        for child in right_sibling.children:
            child.parent = internal
        
        # remove right sibling from parent
        parent.children.pop(internal_index + 1)
        parent.keys.pop(internal_index)
        
        # remove right sibling from pages
        if right_sibling.id in self.pages:
            del self.pages[right_sibling.id]
        
        if parent != self.root and parent.is_underflow(self.min_keys):
            self.handle_internal_underflow(parent)

    def split_leaf(self, leaf: LeafNode):
        mid = len(leaf.keys) // 2
        new_leaf = LeafNode()
        
        # move half the keys and values to new leaf
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        new_leaf.parent = leaf.parent
        
        # Update pointers
        new_leaf.next = leaf.next
        new_leaf.previous = leaf
        
        if leaf.next:
            leaf.next.previous = new_leaf
        leaf.next = new_leaf
        
        # keep first half in original leaf
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        
        new_leaf.id = self.next_page_id
        self.write_page(self.next_page_id, new_leaf)
        self.write_page(leaf.id, leaf)  # Write updated original leaf
        self.next_page_id += 1
        self.next_page_id += 1
        
        promote_key = new_leaf.keys[0]
        self.promote_key(leaf, promote_key, new_leaf)

    def split_internal(self, internal: InternalNode):
        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]
        
        new_internal = InternalNode()
        
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
        
        new_internal.id = self.next_page_id
        self.pages[self.next_page_id] = new_internal
        self.next_page_id += 1
        
        self.promote_key(internal, promote_key, new_internal)

    def promote_key(self, left_child: Node, key: Any, right_child: Node):
        if left_child.parent is None:
            # create new root
            new_root = InternalNode()
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
            # insert in parent
            parent = left_child.parent
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children.insert(pos + 1, right_child)
            right_child.parent = parent

            if parent.is_full(self.max_keys):
                self.split_internal(parent)

    def find_leaf_node(self, key: Any) -> LeafNode:
        current = self.load_page(self.root_page_id)
        while isinstance(current, InternalNode):
            pos = bisect.bisect_right(current.keys, key)
            child_id = current.children[pos].id if hasattr(current.children[pos], 'id') else pos
            current = self.load_page(child_id)
        return current

    def get_key_value(self, record) -> Any:
        return record.get_field_value(self.index_column)

    def range_search(self, start_key: Any, end_key: Any) -> List[RecordPointer]:
        results = []
        leaf = self.find_leaf_node(start_key)
        
        pos = bisect.bisect_left(leaf.keys, start_key)
        
        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_key:
                    return results
                if leaf.keys[i] >= start_key:
                    results.append(leaf.values[i])
            
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
                    'index_column': self.index_column,
                    'first_leaf_id': self.first_leaf.id if self.first_leaf else None
                }
                pickle.dump(tree_data, f)
        except Exception as e:
            print(f"Error saving tree: {e}")

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
                        while isinstance(current, InternalNode):
                            if current.children:
                                current = current.children[0]
                            else:
                                break
                        self.first_leaf = current if isinstance(current, LeafNode) else self.root
        except Exception as e:
            print(f"Error loading tree: {e}")

    def print_tree(self):
        def print_node(node, level=0):
            indent = "  " * level
            if isinstance(node, LeafNode):
                values_str = [str(v) for v in node.values]
                print(f"{indent}Leaf {node.id}: {node.keys} -> {values_str}")
            else:
                print(f"{indent}Internal {node.id}: {node.keys}")
                for child in node.children:
                    print_node(child, level + 1)
        
        print_node(self.root)
        
    def info_btree_unclustered(self) -> dict:
        stats = {
            'total_nodes': len(self.pages),
            'leaf_nodes': 0,
            'internal_nodes': 0,
            'total_keys': 0,
            'height': 0,
            'root_id': self.root_page_id
        }
        def traverse(node, depth=0):
            stats['height'] = max(stats['height'], depth)
            stats['total_keys'] += len(node.keys)
            
            if isinstance(node, LeafNode):
                stats['leaf_nodes'] += 1
            else:
                stats['internal_nodes'] += 1
                for child in node.children:
                    traverse(child, depth + 1)
        
        traverse(self.root)
        stats['height'] += 1  
        
        return stats

    def close(self):
        self.save_tree()

    def clear(self):
        self.root = LeafNode()
        self.first_leaf = self.root
        self.pages = {0: self.root}
        self.next_page_id = 1
        self.root_page_id = 0
        self.root.id = 0
        self.save_tree()
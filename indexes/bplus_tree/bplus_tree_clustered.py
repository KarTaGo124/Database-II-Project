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
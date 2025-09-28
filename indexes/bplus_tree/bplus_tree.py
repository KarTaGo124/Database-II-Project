"""B+ Tree Based for clustered and unclustered indexes.
"""

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
        



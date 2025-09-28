"""B+ Tree Based for clustered and unclustered indexes.
"""
class BPlusTreeUnclusteredIndex:
    def __init__(self, table_metadata: TableMetadata, index_column: str):
        self.index_column = index_column
        self.root = None  # Root node of the B+ tree
        self.order = 4  # Maximum number of children for internal nodes

class Node:
    def __init__(self, is_leaf: bool):
        self.is_leaf = is_leaf
        self.keys = []
        self.parent = None  # Parent node
        self.id = None  # Unique identifier for the node (e.g., page number)
    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) > max_keys

class LeafNode(Node):
    def __init__(self, is_leaf):
        super().__init__()
        self.children = []  # List of RecordPointer objects



class InternalNode(Node):
    def __init__(self, is_leaf):
        super().__init__()
        self.children = []  # Pointers to child nodes

class RecordPointer:
    def __init__(self, page_number: int, slot_number: int):
        self.page_number = page_number
        self.slot_number = slot_number



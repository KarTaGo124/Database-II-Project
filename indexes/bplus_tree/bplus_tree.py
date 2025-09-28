"""B+ Tree Based for clustered and unclustered indexes.
"""

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
        self.values = []  # List of RecordPointer objects
        self.previous = None  # Pointer to previous leaf node
        self.next = None  # Pointer to next leaf node


class InternalNode(Node):
    def __init__(self, is_leaf):
        super().__init__()
        self.children = []  # Pointers to child nodes




"""
import pickle
import os
from typing import Any, List, Optional, Tuple, Union
from dataclasses import dataclass
import bisect

class Record:
    """
    Clase base para representar un registro de la base de datos
    """
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def get_key_value(self, key_field: str) -> Any:
        """Obtiene el valor de un campo específico que actúa como clave"""
        return getattr(self, key_field, None)
    
    def to_dict(self) -> dict:
        """Convierte el record a diccionario para serialización"""
        return self.__dict__
    
    def __repr__(self):
        return f"Record({self.__dict__})"

class BPlusTreeNode:
    """
    Nodo base del B+ Tree
    """
    def __init__(self, is_leaf: bool = False):
        self.keys = []
        self.is_leaf = is_leaf
        self.parent = None
    
    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) >= max_keys

class BPlusTreeInternalNode(BPlusTreeNode):
    """
    Nodo interno del B+ Tree
    """
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children = []  # Referencias a nodos hijos

class BPlusTreeLeafNode(BPlusTreeNode):
    """
    Nodo hoja del B+ Tree
    """
    def __init__(self):
        super().__init__(is_leaf=True)
        self.values = []  # Para clustered: Records completos, para unclustered: referencias
        self.next_leaf = None  # Puntero al siguiente nodo hoja
        self.prev_leaf = None  # Puntero al nodo hoja anterior

class ClusteredBPlusTree:
    """
    B+ Tree Clustered - Los datos están ordenados físicamente según la clave
    """
    def __init__(self, order: int, key_field: str, file_path: str):
        self.order = order  # Grado del árbol
        self.max_keys = order - 1
        self.key_field = key_field  # Campo que actúa como clave primaria
        self.file_path = file_path
        self.root = BPlusTreeLeafNode()  # Inicialmente, la raíz es una hoja
        self.first_leaf = self.root  # Referencia al primer nodo hoja
    
    def search(self, key: Any) -> Optional[Record]:
        """Busca un registro por su clave"""
        leaf_node = self._find_leaf(key)
        
        # Búsqueda binaria en el nodo hoja
        pos = bisect.bisect_left(leaf_node.keys, key)
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            return leaf_node.values[pos]
        return None
    
    def range_search(self, start_key: Any, end_key: Any) -> List[Record]:
        """Búsqueda por rango - muy eficiente en B+ trees clustered"""
        results = []
        leaf_node = self._find_leaf(start_key)
        
        while leaf_node:
            for i, key in enumerate(leaf_node.keys):
                if start_key <= key <= end_key:
                    results.append(leaf_node.values[i])
                elif key > end_key:
                    return results
            leaf_node = leaf_node.next_leaf
        
        return results
    
    def insert(self, record: Record):
        """Inserta un nuevo registro"""
        key = record.get_key_value(self.key_field)
        self._insert_recursive(self.root, key, record)
        self._save_to_file()
    
    def _insert_recursive(self, node: BPlusTreeNode, key: Any, record: Record):
        """Inserción recursiva"""
        if node.is_leaf:
            # Insertar en nodo hoja
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.values.insert(pos, record)
            
            # Verificar si necesita división
            if node.is_full(self.max_keys):
                self._split_leaf(node)
        else:
            # Encontrar el hijo apropiado
            pos = bisect.bisect_left(node.keys, key)
            child = node.children[pos]
            self._insert_recursive(child, key, record)
    
    def _split_leaf(self, leaf: BPlusTreeLeafNode):
        """División de nodo hoja"""
        mid = len(leaf.keys) // 2
        
        # Crear nuevo nodo hoja
        new_leaf = BPlusTreeLeafNode()
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        new_leaf.next_leaf = leaf.next_leaf
        new_leaf.prev_leaf = leaf
        
        if leaf.next_leaf:
            leaf.next_leaf.prev_leaf = new_leaf
        leaf.next_leaf = new_leaf
        
        # Actualizar nodo original
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        
        # Promover clave al padre
        promote_key = new_leaf.keys[0]
        self._promote_key(leaf, promote_key, new_leaf)
    
    def _promote_key(self, left_child: BPlusTreeNode, key: Any, right_child: BPlusTreeNode):
        """Promoción de clave al nodo padre"""
        if left_child.parent is None:
            # Crear nueva raíz
            new_root = BPlusTreeInternalNode()
            new_root.keys = [key]
            new_root.children = [left_child, right_child]
            left_child.parent = new_root
            right_child.parent = new_root
            self.root = new_root
        else:
            parent = left_child.parent
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children.insert(pos + 1, right_child)
            right_child.parent = parent
            
            # Verificar si el padre necesita división
            if parent.is_full(self.max_keys):
                self._split_internal(parent)
    
    def _split_internal(self, internal: BPlusTreeInternalNode):
        """División de nodo interno"""
        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]
        
        # Crear nuevo nodo interno
        new_internal = BPlusTreeInternalNode()
        new_internal.keys = internal.keys[mid + 1:]
        new_internal.children = internal.children[mid + 1:]
        
        # Actualizar padres de los hijos
        for child in new_internal.children:
            child.parent = new_internal
        
        # Actualizar nodo original
        internal.keys = internal.keys[:mid]
        internal.children = internal.children[:mid + 1]
        
        # Promover clave
        self._promote_key(internal, promote_key, new_internal)
    
    def _find_leaf(self, key: Any) -> BPlusTreeLeafNode:
        """Encuentra el nodo hoja que debería contener la clave"""
        current = self.root
        
        while not current.is_leaf:
            pos = bisect.bisect_left(current.keys, key)
            current = current.children[pos]
        
        return current
    
    def _save_to_file(self):
        """Guarda el árbol en archivo"""
        with open(self.file_path, 'wb') as f:
            pickle.dump(self, f)
    
    def load_from_file(self):
        """Carga el árbol desde archivo"""
        if os.path.exists(self.file_path):
            with open(self.file_path, 'rb') as f:
                loaded_tree = pickle.load(f)
                self.root = loaded_tree.root
                self.first_leaf = loaded_tree.first_leaf

class UnclusteredBPlusTree:
    """
    B+ Tree Unclustered - Las hojas contienen referencias a los registros
    """
    def __init__(self, order: int, key_field: str, file_path: str, data_file_path: str):
        self.order = order
        self.max_keys = order - 1
        self.key_field = key_field
        self.file_path = file_path
        self.data_file_path = data_file_path  # Archivo donde están los datos reales
        self.root = BPlusTreeLeafNode()
        self.first_leaf = self.root
    
    def search(self, key: Any) -> Optional[Record]:
        """Busca un registro por su clave"""
        leaf_node = self._find_leaf(key)
        
        pos = bisect.bisect_left(leaf_node.keys, key)
        if pos < len(leaf_node.keys) and leaf_node.keys[pos] == key:
            record_position = leaf_node.values[pos]  # Posición en el archivo de datos
            return self._load_record_from_position(record_position)
        return None
    
    def insert(self, record: Record, record_position: int):
        """Inserta una referencia al registro"""
        key = record.get_key_value(self.key_field)
        self._insert_recursive(self.root, key, record_position)
        self._save_to_file()
    
    def _insert_recursive(self, node: BPlusTreeNode, key: Any, record_position: int):
        """Similar al clustered pero almacena posiciones en lugar de records"""
        if node.is_leaf:
            pos = bisect.bisect_left(node.keys, key)
            node.keys.insert(pos, key)
            node.values.insert(pos, record_position)
            
            if node.is_full(self.max_keys):
                self._split_leaf(node)
        else:
            pos = bisect.bisect_left(node.keys, key)
            child = node.children[pos]
            self._insert_recursive(child, key, record_position)
    
    def _load_record_from_position(self, position: int) -> Record:
        """Carga un registro desde su posición en el archivo de datos"""
        # Esta implementación dependerá de cómo almacenes los datos
        # Aquí un ejemplo básico:
        with open(self.data_file_path, 'rb') as f:
            f.seek(position)
            record_data = pickle.load(f)
            return record_data
    
    # Los métodos de división y promoción son similares al clustered
    def _split_leaf(self, leaf: BPlusTreeLeafNode):
        """Similar al clustered tree"""
        mid = len(leaf.keys) // 2
        
        new_leaf = BPlusTreeLeafNode()
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        new_leaf.next_leaf = leaf.next_leaf
        new_leaf.prev_leaf = leaf
        
        if leaf.next_leaf:
            leaf.next_leaf.prev_leaf = new_leaf
        leaf.next_leaf = new_leaf
        
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        
        promote_key = new_leaf.keys[0]
        self._promote_key(leaf, promote_key, new_leaf)
    
    def _promote_key(self, left_child: BPlusTreeNode, key: Any, right_child: BPlusTreeNode):
        """Similar al clustered tree"""
        if left_child.parent is None:
            new_root = BPlusTreeInternalNode()
            new_root.keys = [key]
            new_root.children = [left_child, right_child]
            left_child.parent = new_root
            right_child.parent = new_root
            self.root = new_root
        else:
            parent = left_child.parent
            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.children.insert(pos + 1, right_child)
            right_child.parent = parent
            
            if parent.is_full(self.max_keys):
                self._split_internal(parent)
    
    def _split_internal(self, internal: BPlusTreeInternalNode):
        """Similar al clustered tree"""
        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]
        
        new_internal = BPlusTreeInternalNode()
        new_internal.keys = internal.keys[mid + 1:]
        new_internal.children = internal.children[mid + 1:]
        
        for child in new_internal.children:
            child.parent = new_internal
        
        internal.keys = internal.keys[:mid]
        internal.children = internal.children[:mid + 1]
        
        self._promote_key(internal, promote_key, new_internal)
    
    def _find_leaf(self, key: Any) -> BPlusTreeLeafNode:
        """Encuentra el nodo hoja apropiado"""
        current = self.root
        
        while not current.is_leaf:
            pos = bisect.bisect_left(current.keys, key)
            current = current.children[pos]
        
        return current
    
    def _save_to_file(self):
        """Guarda el índice en archivo"""
        with open(self.file_path, 'wb') as f:
            pickle.dump(self, f)

# Ejemplo de uso
if __name__ == "__main__":
    # Crear algunos registros de ejemplo
    record1 = Record(id=1, name="Juan", age=25, city="Lima")
    record2 = Record(id=2, name="Maria", age=30, city="Arequipa")
    record3 = Record(id=3, name="Carlos", age=28, city="Cusco")
    
    # Crear B+ Tree clustered
    clustered_tree = ClusteredBPlusTree(order=4, key_field="id", file_path="clustered_index.pkl")
    
    # Insertar registros
    clustered_tree.insert(record1)
    clustered_tree.insert(record2)
    clustered_tree.insert(record3)
    
    # Buscar registro
    found_record = clustered_tree.search(2)
    print(f"Registro encontrado: {found_record}")
    
    # Búsqueda por rango
    range_results = clustered_tree.range_search(1, 3)
    print(f"Registros en rango [1-3]: {range_results}")
"""
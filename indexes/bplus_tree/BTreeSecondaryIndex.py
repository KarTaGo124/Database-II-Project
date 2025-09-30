from typing import Any, List
from .bplus_tree_unclustered import BPlusTreeUnclusteredIndex, RecordPointer
from ..core.record import Record
from ..core.performance_tracker import PerformanceTracker, OperationResult
import os
import pickle


class BTreeSecondaryIndex:

    
    def __init__(self, field_name: str, primary_index, filename: str, order: int = 4):
        self.field_name = field_name
        self.primary_index = primary_index
        self.filename = filename
        self.order = order
        self.performance = PerformanceTracker()
        
        # Obtener el tipo de la primary key desde la tabla
        if hasattr(primary_index, 'table'):
            self.primary_key_field = primary_index.table.key_field
            # Encontrar el tipo de la primary key
            for fname, ftype, fsize in primary_index.table.all_fields:
                if fname == self.primary_key_field:
                    self.primary_key_type = ftype
                    self.primary_key_size = fsize
                    break
        else:
            self.primary_key_type = "INT"
            self.primary_key_size = 4
        
        self.btree = BPlusTreeUnclusteredIndex(
            order=order,
            index_column=field_name,
            file_path=filename
        )
        
        # Mapeo externo: index_value -> [primary_keys]
        # Esto permite soportar primary keys de cualquier tipo
        self._value_to_primary_keys = {}
        self._load_mapping()
    
    def insert(self, record: Record) -> OperationResult:
        self.performance.start_operation()
        
        index_value = record.get_field_value(self.field_name)
        primary_key = record.get_key()
        
        # Usar RecordPointer como contenedor
        record_pointer = RecordPointer(
            page_number=0,
            slot_number=self._safe_hash(primary_key)
        )
        
        if index_value not in self._value_to_primary_keys:
            self._value_to_primary_keys[index_value] = []
        
        if primary_key not in self._value_to_primary_keys[index_value]:
            self._value_to_primary_keys[index_value].append(primary_key)
        
        if len(self._value_to_primary_keys[index_value]) == 1:
            success = self.btree.insert(record, record_pointer)
        else:
            success = True  
        
        self._save_mapping()
        return self.performance.end_operation(success)
    
    def search(self, value: Any) -> OperationResult:
        self.performance.start_operation()
        
        result_pointer = self.btree.search(value)
        
        if result_pointer is None:
            return self.performance.end_operation([])
        
        # Obtener las primary keys del mapeo externo
        primary_keys = self._value_to_primary_keys.get(value, [])
        
        return self.performance.end_operation(primary_keys)
    
    def range_search(self, start_value: Any, end_value: Any) -> OperationResult:
        self.performance.start_operation()
        
        # Recolectar todas las primary keys en el rango
        primary_keys = []
        
        # Recorrer las hojas del árbol en el rango
        leaf = self.btree.find_leaf_node(start_value)
        
        # Encontrar posicion inicial
        pos = 0
        for i, key in enumerate(leaf.keys):
            if key >= start_value:
                pos = i
                break
        
        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_value:
                    return self.performance.end_operation(primary_keys)
                
                if leaf.keys[i] >= start_value:
                    index_value = leaf.keys[i]
                    pks = self._value_to_primary_keys.get(index_value, [])
                    primary_keys.extend(pks)
            
            leaf = leaf.next
            pos = 0
        
        return self.performance.end_operation(primary_keys)
    
    def delete(self, record: Record) -> OperationResult:
        self.performance.start_operation()
        
        index_value = record.get_field_value(self.field_name)
        primary_key = record.get_key()
        
        # Remover del mapeo
        if index_value in self._value_to_primary_keys:
            if primary_key in self._value_to_primary_keys[index_value]:
                self._value_to_primary_keys[index_value].remove(primary_key)
                
                # Si no quedan más primary keys, eliminar del B+ Tree
                if len(self._value_to_primary_keys[index_value]) == 0:
                    del self._value_to_primary_keys[index_value]
                    success = self.btree.delete(index_value)
                else:
                    success = True
                
                self._save_mapping()
                return self.performance.end_operation(success)
        
        return self.performance.end_operation(False)
    
    def drop_index(self) -> List[str]:
        removed_files = []
        
        if os.path.exists(self.filename):
            os.remove(self.filename)
            removed_files.append(self.filename)
        
        mapping_file = self._get_mapping_filename()
        if os.path.exists(mapping_file):
            os.remove(mapping_file)
            removed_files.append(mapping_file)
        
        return removed_files
    
    def get_stats(self) -> dict:
        stats = self.btree.info_btree_unclustered()
        stats['mapping_entries'] = len(self._value_to_primary_keys)
        stats['total_primary_keys'] = sum(len(pks) for pks in self._value_to_primary_keys.values())
        return stats
    
    
    def _safe_hash(self, value: Any) -> int:
        if isinstance(value, int):
            return value
        elif isinstance(value, (str, bytes)):
            return abs(hash(value)) % (2**31)
        elif isinstance(value, float):
            return int(value) if value == int(value) else abs(hash(value)) % (2**31)
        else:
            return abs(hash(str(value))) % (2**31)
    
    def _get_mapping_filename(self) -> str:
        return self.filename + '.mapping'
    
    def _save_mapping(self):
        mapping_file = self._get_mapping_filename()
        try:
            with open(mapping_file, 'wb') as f:
                pickle.dump(self._value_to_primary_keys, f)
        except Exception as e:
            print(f"Error saving mapping: {e}")
    
    def _load_mapping(self):
        mapping_file = self._get_mapping_filename()
        if os.path.exists(mapping_file):
            try:
                with open(mapping_file, 'rb') as f:
                    self._value_to_primary_keys = pickle.load(f)
            except Exception as e:
                print(f"Error loading mapping: {e}")
                self._value_to_primary_keys = {}
        else:
            self._value_to_primary_keys = {}


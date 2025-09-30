from typing import Any, List, Optional
from .bplus_tree_clustered import BPlusTreeClusteredIndex
from ..core.record import Table, Record
from ..core.performance_tracker import PerformanceTracker, OperationResult
import os


class BTreePrimaryIndex:
    
    def __init__(self, table: Table, filename: str, order: int = 4):
        self.table = table
        self.filename = filename
        self.key_field = table.key_field
        self.order = order
        self.performance = PerformanceTracker()
        
        # Crear el B+ Tree clustered
        self.btree = BPlusTreeClusteredIndex(
            order=order,
            key_column=self.key_field,
            file_path=filename,
            record_class=Record
        )
    
    def insert(self, record: Record) -> OperationResult:
        self.performance.start_operation()
        success = self.btree.insert(record)
        return self.performance.end_operation(success)
    
    def search(self, key: Any) -> OperationResult:
        self.performance.start_operation()
        result = self.btree.search(key)
        return self.performance.end_operation(result)
    
    def search_without_metrics(self, key: Any) -> Optional[Record]:
        return self.btree.search(key)
    
    def delete(self, key: Any) -> OperationResult:
        self.performance.start_operation()
        success = self.btree.delete(key)
        return self.performance.end_operation(success)
    
    def range_search(self, start_key: Any, end_key: Any) -> OperationResult:
        self.performance.start_operation()
        results = self.btree.range_search(start_key, end_key)
        return self.performance.end_operation(results)
    
    def scanAll(self) -> List[Record]:
        records = []
        leaf = self.btree.first_leaf
        
        while leaf is not None:
            records.extend(leaf.records)
            leaf = leaf.next
        
        return records
    
    def drop_table(self) -> List[str]:
        removed_files = []
        if os.path.exists(self.filename):
            os.remove(self.filename)
            removed_files.append(self.filename)
        return removed_files
    
    def get_stats(self) -> dict:
        return self.btree.info_btree_clustered()


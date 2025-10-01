import os
from typing import Dict, Any, List, Tuple, Optional
from .record import Table, Record
from .performance_tracker import OperationResult, PerformanceTracker

from ..bplus_tree.bplus_tree_clustered import BPlusTreeClusteredIndex
from ..bplus_tree.bplus_tree_unclustered import BPlusTreeUnclusteredIndex

class DatabaseManager:

    INDEX_TYPES = {
        "SEQUENTIAL": {"primary": True, "secondary": False},
        "ISAM": {"primary": True, "secondary": True},
        "BTREE": {"primary": True, "secondary": True},
        "HASH": {"primary": False, "secondary": True},
        "RTREE": {"primary": False, "secondary": True}
    }

    def __init__(self, database_name: str = "default_db"):
        self.database_name = database_name
        self.tables = {}
        self.base_dir = os.path.join("data", "databases", database_name)
        os.makedirs(self.base_dir, exist_ok=True)

    def create_table(self, table: Table, primary_index_type: str = "ISAM", csv_filename: str = None):
        if not self._validate_primary_index(primary_index_type):
            raise ValueError(f"{primary_index_type} cannot be used as primary index")

        table_name = table.table_name
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists")

        table_info = {
            "table": table,
            "primary_index": None,
            "secondary_indexes": {},
            "primary_type": primary_index_type,
            "csv_filename": csv_filename
        }

        primary_index = self._create_primary_index(
            table, primary_index_type, csv_filename
        )

        if primary_index_type == "SEQUboranENTIAL":
            extra_fields = {"active": ("BOOL", 1)}
            table_with_active = Table(
                table_name=table.table_name,
                sql_fields=table.sql_fields,
                key_field=table.key_field,
                extra_fields=extra_fields
            )
            table_info["table"] = table_with_active

        table_info["primary_index"] = primary_index
        self.tables[table_name] = table_info
        return True

    def create_index(self, table_name: str, field_name: str, index_type: str, scan_existing: bool = True):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        if not self._validate_secondary_index(index_type):
            raise ValueError(f"{index_type} cannot be used as secondary index")

        table_info = self.tables[table_name]
        table = table_info["table"]

        if field_name == table.key_field:
            raise ValueError(f"Cannot create secondary index on primary key field '{field_name}'")

        field_info = self._get_field_info(table, field_name)
        if not field_info:
            raise ValueError(f"Field {field_name} not found in table {table_name}")

        if field_name in table_info["secondary_indexes"]:
            raise ValueError(f"Index on {field_name} already exists")

        secondary_index = self._create_secondary_index(
            table, field_name, index_type, table_info["csv_filename"]
        )

        table_info["secondary_indexes"][field_name] = {
            "index": secondary_index,
            "type": index_type
        }

        if scan_existing:
            primary_index = table_info["primary_index"]
            if hasattr(primary_index, 'scanAll'):
                try:
                    existing_records = primary_index.scanAll()
                    for record in existing_records:
                        # Handle B+ Tree unclustered differently
                        if hasattr(secondary_index, 'insert') and 'BPlusTreeUnclusteredIndex' in str(type(secondary_index)):
                            from ..bplus_tree.bplus_tree_unclustered import RecordPointer
                            # Create a record pointer for the record
                            record_pointer = RecordPointer(0, hash(record.get_key()) % 1000)
                            secondary_index.insert(record, record_pointer)
                        else:
                            secondary_index.insert(record)
                except Exception as e:
                    del table_info["secondary_indexes"][field_name]
                    if hasattr(secondary_index, 'drop_index'):
                        secondary_index.drop_index()
                    raise ValueError(f"Error indexing existing records: {e}")

        return True

    def insert(self, table_name: str, record: Record):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]

        # B+ Tree Clustered insert returns bool, we need to wrap it
        if hasattr(primary_index, 'performance'):
            primary_index.performance.start_operation()
        
        success = primary_index.insert(record)
        
        # Get performance metrics from the B+ Tree
        if hasattr(primary_index, 'performance'):
            primary_result = primary_index.performance.end_operation(success)
        else:
            primary_result = OperationResult(success, 0, 0, 0)

        total_reads = primary_result.disk_reads
        total_writes = primary_result.disk_writes
        total_time = primary_result.execution_time_ms

        # Handle secondary indexes
        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]
            
            # For unclustered B+ Tree, we need to create a RecordPointer
            if hasattr(secondary_index, 'insert') and 'BPlusTreeUnclusteredIndex' in str(type(secondary_index)):
                from ..bplus_tree.bplus_tree_unclustered import RecordPointer
                
                # Start performance tracking
                if hasattr(secondary_index, 'performance'):
                    secondary_index.performance.start_operation()
                
                # Use record's key as pointer (simplified)
                record_pointer = RecordPointer(0, hash(record.get_key()) % 1000)
                success_secondary = secondary_index.insert(record, record_pointer)
                
                # Get performance metrics
                if hasattr(secondary_index, 'performance'):
                    secondary_result = secondary_index.performance.end_operation(success_secondary)
                else:
                    secondary_result = OperationResult(success_secondary, 0, 0, 0)
            else:
                # For other secondary index types that return OperationResult
                secondary_result = secondary_index.insert(record)
            
            total_reads += secondary_result.disk_reads
            total_writes += secondary_result.disk_writes
            total_time += secondary_result.execution_time_ms

        return OperationResult(primary_result.data, total_time, total_reads, total_writes)

    def search(self, table_name: str, value, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name is None:
            primary_index = table_info["primary_index"]
            
            # Start performance tracking for this operation
            if hasattr(primary_index, 'performance'):
                primary_index.performance.start_operation()
            
            # For B+ Tree clustered, search returns Record directly
            if hasattr(primary_index, 'search') and 'BPlusTreeClusteredIndex' in str(type(primary_index)):
                record = primary_index.search(value)
                
                # Get performance metrics
                if hasattr(primary_index, 'performance'):
                    result = primary_index.performance.end_operation([record] if record else [])
                    return result
                else:
                    return OperationResult([record] if record else [], 0, 0, 0)
            else:
                # For other index types that return OperationResult
                result = primary_index.search(value)
                if result.data:
                    return OperationResult([result.data], result.execution_time_ms, result.disk_reads, result.disk_writes)
                else:
                    return OperationResult([], result.execution_time_ms, result.disk_reads, result.disk_writes)

        elif field_name in table_info["secondary_indexes"]:
            secondary_index = table_info["secondary_indexes"][field_name]["index"]
            primary_index = table_info["primary_index"]

            # Reset performance tracker for secondary index operation
            if hasattr(secondary_index, 'performance'):
                secondary_index.performance.start_operation()

            # For B+ Tree unclustered, search returns RecordPointer directly  
            if hasattr(secondary_index, 'search') and 'BPlusTreeUnclusteredIndex' in str(type(secondary_index)):
                record_pointer = secondary_index.search(value)
                
                # Get performance metrics from secondary index
                if hasattr(secondary_index, 'performance'):
                    secondary_result = secondary_index.performance.end_operation(record_pointer)
                    secondary_time = secondary_result.execution_time_ms
                    secondary_reads = secondary_result.disk_reads
                    secondary_writes = secondary_result.disk_writes
                else:
                    secondary_time = 0
                    secondary_reads = 0
                    secondary_writes = 0

                if not record_pointer:
                    return OperationResult([], secondary_time, secondary_reads, secondary_writes)

                # Now search in primary index using the primary key from record pointer
                # Start performance tracking for primary index operation
                if hasattr(primary_index, 'performance'):
                    primary_index.performance.start_operation()

                # For clustered B+ Tree, get the actual record
                if hasattr(primary_index, 'search') and 'BPlusTreeClusteredIndex' in str(type(primary_index)):
                    # Use record_pointer to get the primary key (simplified approach)
                    # In a real implementation, you'd need to properly map the record pointer
                    # For now, we'll do a full scan and filter by the indexed field value
                    if hasattr(primary_index, 'scanAll'):
                        all_records = primary_index.scanAll()
                        matching_records = []
                        for rec in all_records:
                            if rec.get_field_value(field_name) == value:
                                matching_records.append(rec)
                    else:
                        matching_records = []
                    
                    # Get primary performance metrics
                    if hasattr(primary_index, 'performance'):
                        primary_result = primary_index.performance.end_operation(matching_records)
                        primary_time = primary_result.execution_time_ms
                        primary_reads = primary_result.disk_reads
                        primary_writes = primary_result.disk_writes
                    else:
                        primary_time = 0
                        primary_reads = 0
                        primary_writes = 0

                    total_time = secondary_time + primary_time
                    total_reads = secondary_reads + primary_reads
                    total_writes = secondary_writes + primary_writes

                    return OperationResult(matching_records, total_time, total_reads, total_writes)
                else:
                    # For other primary index types
                    # Since we can't properly map record pointers back to records without a proper implementation,
                    # we'll do a full scan and filter by the indexed field value
                    if hasattr(primary_index, 'scanAll'):
                        all_records = primary_index.scanAll()
                        matching_records = []
                        for rec in all_records:
                            if rec.get_field_value(field_name) == value:
                                matching_records.append(rec)
                    else:
                        matching_records = []
                    
                    total_time = secondary_time
                    total_reads = secondary_reads
                    total_writes = secondary_writes
                    
                    return OperationResult(matching_records, total_time, total_reads, total_writes)
            else:
                # For other secondary index types that return OperationResult
                secondary_result = secondary_index.search(value)
                if not secondary_result.data:
                    return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes)

                total_reads = secondary_result.disk_reads
                total_writes = secondary_result.disk_writes
                total_time = secondary_result.execution_time_ms

                if secondary_result.data:
                    matching_records = []
                    for primary_key in secondary_result.data:
                        if hasattr(primary_index, 'search'):
                            primary_result = primary_index.search(primary_key)
                            # Handle OperationResult vs direct return
                            if hasattr(primary_result, 'data'):
                                if primary_result.data:
                                    matching_records.append(primary_result.data)
                            else:
                                if primary_result:
                                    matching_records.append(primary_result)
                else:
                    matching_records = []

                return OperationResult(matching_records, total_time, total_reads, total_writes)

        else:
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")

            primary_index = table_info["primary_index"]

            if hasattr(primary_index, 'scanAll'):
                primary_index.performance.start_operation()
                all_records = primary_index.scanAll()
                scan_result = primary_index.performance.end_operation(all_records)
            else:
                raise NotImplementedError(f"Full scan not supported for {table_info['primary_type']} index")

            matching_records = []
            for record in all_records:
                record_value = getattr(record, field_name, None)
                if record_value is not None:
                    if hasattr(record_value, 'decode'):
                        record_value = record_value.decode('utf-8').rstrip('\x00').rstrip()
                    else:
                        record_value = str(record_value).rstrip()

                    value_str = value.decode('utf-8').rstrip('\x00').rstrip() if hasattr(value, 'decode') else str(value).rstrip()

                    if record_value == value_str:
                        matching_records.append(record)

            return OperationResult(matching_records, scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)

    def range_search(self, table_name: str, start_key, end_key, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name is None:
            primary_index = table_info["primary_index"]
            
            # Start performance tracking for this operation
            if hasattr(primary_index, 'performance'):
                primary_index.performance.start_operation()
            
            # For B+ Tree clustered, range_search returns List[Record] directly
            if hasattr(primary_index, 'range_search') and 'BPlusTreeClusteredIndex' in str(type(primary_index)):
                records = primary_index.range_search(start_key, end_key)
                
                # Get performance metrics
                if hasattr(primary_index, 'performance'):
                    return primary_index.performance.end_operation(records)
                else:
                    return OperationResult(records, 0, 0, 0)
            else:
                # For other index types that return OperationResult
                return primary_index.range_search(start_key, end_key)

        elif field_name in table_info["secondary_indexes"]:
            secondary_index = table_info["secondary_indexes"][field_name]["index"]
            primary_index = table_info["primary_index"]

            # Reset performance tracker for secondary index operation
            if hasattr(secondary_index, 'performance'):
                secondary_index.performance.start_operation()

            # For B+ Tree unclustered, range_search returns List[RecordPointer] directly
            if hasattr(secondary_index, 'range_search') and 'BPlusTreeUnclusteredIndex' in str(type(secondary_index)):
                record_pointers = secondary_index.range_search(start_key, end_key)
                
                # Get performance metrics from secondary index
                if hasattr(secondary_index, 'performance'):
                    secondary_result = secondary_index.performance.end_operation(record_pointers)
                    secondary_time = secondary_result.execution_time_ms
                    secondary_reads = secondary_result.disk_reads
                    secondary_writes = secondary_result.disk_writes
                else:
                    secondary_time = 0
                    secondary_reads = 0
                    secondary_writes = 0

                if not record_pointers:
                    return OperationResult([], secondary_time, secondary_reads, secondary_writes)

                # Now get records from primary index
                # Start performance tracking for primary index operation
                if hasattr(primary_index, 'performance'):
                    primary_index.performance.start_operation()

                matching_records = []
                # For simplified implementation, we'll do a full scan and filter by range
                # In a real implementation, you'd need to properly map record pointers to records
                if hasattr(primary_index, 'scanAll'):
                    all_records = primary_index.scanAll()
                    for rec in all_records:
                        field_value = rec.get_field_value(field_name)
                        if start_key <= field_value <= end_key:
                            matching_records.append(rec)
                
                # Get primary performance metrics
                if hasattr(primary_index, 'performance'):
                    primary_result = primary_index.performance.end_operation(matching_records)
                    primary_time = primary_result.execution_time_ms
                    primary_reads = primary_result.disk_reads
                    primary_writes = primary_result.disk_writes
                else:
                    primary_time = 0
                    primary_reads = 0
                    primary_writes = 0

                total_time = secondary_time + primary_time
                total_reads = secondary_reads + primary_reads
                total_writes = secondary_writes + primary_writes

                return OperationResult(matching_records, total_time, total_reads, total_writes)
            else:
                # For other secondary index types that return OperationResult
                secondary_result = secondary_index.range_search(start_key, end_key)
                if not secondary_result.data:
                    return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes)

                total_reads = secondary_result.disk_reads
                total_writes = secondary_result.disk_writes
                total_time = secondary_result.execution_time_ms

                if secondary_result.data:
                    matching_records = []
                    for primary_key in secondary_result.data:
                        if hasattr(primary_index, 'search'):
                            primary_result = primary_index.search(primary_key)
                            # Handle OperationResult vs direct return
                            if hasattr(primary_result, 'data'):
                                if primary_result.data:
                                    matching_records.append(primary_result.data)
                            else:
                                if primary_result:
                                    matching_records.append(primary_result)
                else:
                    matching_records = []

                return OperationResult(matching_records, total_time, total_reads, total_writes)

        else:
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")

            primary_index = table_info["primary_index"]

            if hasattr(primary_index, 'scanAll'):
                primary_index.performance.start_operation()
                all_records = primary_index.scanAll()
                scan_result = primary_index.performance.end_operation(all_records)
            else:
                raise NotImplementedError(f"Full scan not supported for {table_info['primary_type']} index")

            matching_records = []
            field_type, _ = field_info

            for record in all_records:
                record_value = getattr(record, field_name, None)
                if record_value is not None:
                    # Manejar tipos de datos apropiadamente
                    if field_type == "FLOAT":
                        # Para FLOAT, comparar como números
                        try:
                            if hasattr(record_value, 'decode'):
                                record_value = float(record_value.decode('utf-8').rstrip('\x00'))
                            else:
                                record_value = float(record_value)
                            start_val = float(start_key)
                            end_val = float(end_key)
                            if start_val <= record_value <= end_val:
                                matching_records.append(record)
                        except (ValueError, TypeError):
                            continue
                    elif field_type == "INT":
                        # Para INT, comparar como enteros
                        try:
                            if hasattr(record_value, 'decode'):
                                record_value = int(record_value.decode('utf-8').rstrip('\x00'))
                            else:
                                record_value = int(record_value)
                            start_val = int(start_key)
                            end_val = int(end_key)
                            if start_val <= record_value <= end_val:
                                matching_records.append(record)
                        except (ValueError, TypeError):
                            continue
                    else:
                        # Para strings/otros tipos, comparar como strings
                        if hasattr(record_value, 'decode'):
                            record_value = record_value.decode('utf-8').rstrip('\x00').rstrip()
                        else:
                            record_value = str(record_value).rstrip()

                        start_str = start_key.decode('utf-8').rstrip('\x00').rstrip() if hasattr(start_key, 'decode') else str(start_key).rstrip()
                        end_str = end_key.decode('utf-8').rstrip('\x00').rstrip() if hasattr(end_key, 'decode') else str(end_key).rstrip()

                        if start_str <= record_value <= end_str:
                            matching_records.append(record)

            matching_records.sort(key=lambda r: getattr(r, field_name))
            return OperationResult(matching_records, scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)

    def delete(self, table_name: str, value, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name is None:
            primary_index = table_info["primary_index"]

            search_result = self.search(table_name, value)
            if not search_result.data:
                return OperationResult(False, search_result.execution_time_ms, search_result.disk_reads, search_result.disk_writes)

            record = search_result.data[0]
            total_reads = search_result.disk_reads
            total_writes = search_result.disk_writes
            total_time = search_result.execution_time_ms

            # Handle secondary indexes deletion first
            for fname, index_info in table_info["secondary_indexes"].items():
                secondary_index = index_info["index"]
                
                # Reset performance tracker for secondary index operation
                if hasattr(secondary_index, 'performance'):
                    secondary_index.performance.start_operation()
                
                # For B+ Tree unclustered, delete returns bool directly
                if hasattr(secondary_index, 'delete') and 'BPlusTreeUnclusteredIndex' in str(type(secondary_index)):
                    # For unclustered B+ Tree, we need to extract the key from the record for the indexed field
                    field_value = record.get_field_value(fname)
                    success = secondary_index.delete(field_value)
                    
                    # Get performance metrics
                    if hasattr(secondary_index, 'performance'):
                        secondary_result = secondary_index.performance.end_operation(success)
                    else:
                        secondary_result = OperationResult(success, 0, 0, 0)
                else:
                    # For other secondary index types that return OperationResult
                    secondary_result = secondary_index.delete(record)
                
                total_reads += secondary_result.disk_reads
                total_writes += secondary_result.disk_writes
                total_time += secondary_result.execution_time_ms

            # Reset performance tracker for primary index operation
            if hasattr(primary_index, 'performance'):
                primary_index.performance.start_operation()

            # Handle primary index deletion
            if hasattr(primary_index, 'delete') and 'BPlusTreeClusteredIndex' in str(type(primary_index)):
                success = primary_index.delete(value)
                
                # Get performance metrics
                if hasattr(primary_index, 'performance'):
                    delete_result = primary_index.performance.end_operation(success)
                else:
                    delete_result = OperationResult(success, 0, 0, 0)
            else:
                # For other primary index types that return OperationResult
                delete_result = primary_index.delete(value)
            
            total_reads += delete_result.disk_reads
            total_writes += delete_result.disk_writes
            total_time += delete_result.execution_time_ms

            return OperationResult(delete_result.data, total_time, total_reads, total_writes)

        else:
            search_result = self.search(table_name, value, field_name)

            if not search_result.data:
                return OperationResult(0, search_result.execution_time_ms, search_result.disk_reads, search_result.disk_writes)

            deleted_count = 0
            total_reads = search_result.disk_reads
            total_writes = search_result.disk_writes
            total_time = search_result.execution_time_ms

            records_to_delete = search_result.data

            for record in records_to_delete:
                delete_result = self.delete(table_name, record.get_key())
                if delete_result.data:
                    deleted_count += 1
                total_reads += delete_result.disk_reads
                total_writes += delete_result.disk_writes
                total_time += delete_result.execution_time_ms

            return OperationResult(deleted_count, total_time, total_reads, total_writes)

    def range_delete(self, table_name: str, start_key, end_key, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        search_result = self.range_search(table_name, start_key, end_key, field_name)

        if not search_result.data:
            return OperationResult(0, search_result.execution_time_ms, search_result.disk_reads, search_result.disk_writes)

        deleted_count = 0
        total_reads = search_result.disk_reads
        total_writes = search_result.disk_writes
        total_time = search_result.execution_time_ms

        for record in search_result.data:
            delete_result = self.delete(table_name, record.get_key())
            if delete_result.data:
                deleted_count += 1
            total_reads += delete_result.disk_reads
            total_writes += delete_result.disk_writes
            total_time += delete_result.execution_time_ms

        return OperationResult(deleted_count, total_time, total_reads, total_writes)

    def drop_index(self, table_name: str, field_name: str):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name not in table_info["secondary_indexes"]:
            raise ValueError(f"Index on field '{field_name}' not found in table '{table_name}'")

        secondary_index = table_info["secondary_indexes"][field_name]["index"]

        if hasattr(secondary_index, 'drop_index'):
            removed_files = secondary_index.drop_index()
        else:
            removed_files = []

        del table_info["secondary_indexes"][field_name]
        return removed_files

    def drop_table(self, table_name: str):
        if table_name not in self.tables:
            return False

        table_info = self.tables[table_name]
        removed_files = []

        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]
            if hasattr(secondary_index, 'drop_index'):
                removed_files.extend(secondary_index.drop_index())

        primary_index = table_info["primary_index"]
        if hasattr(primary_index, 'drop_table'):
            removed_files.extend(primary_index.drop_table())

        del self.tables[table_name]
        return removed_files

    def get_table_info(self, table_name: str):
        if table_name not in self.tables:
            return None

        table_info = self.tables[table_name]
        return {
            "table_name": table_name,
            "primary_type": table_info["primary_type"],
            "secondary_indexes": {
                field_name: index_info["type"]
                for field_name, index_info in table_info["secondary_indexes"].items()
            },
            "field_count": len(table_info["table"].all_fields),
            "csv_filename": table_info.get("csv_filename")
        }

    def list_tables(self):
        return list(self.tables.keys())

    def get_database_stats(self):
        stats = {
            "database_name": self.database_name,
            "table_count": len(self.tables),
            "tables": {}
        }

        for table_name, table_info in self.tables.items():
            table_stats = {
                "primary_type": table_info["primary_type"],
                "secondary_count": len(table_info["secondary_indexes"]),
                "secondary_types": list(table_info["secondary_indexes"].values())
            }

            try:
                primary_index = table_info["primary_index"]
                if hasattr(primary_index, 'scanAll'):
                    records = primary_index.scanAll()
                    table_stats["record_count"] = len(records) if records else 0
                else:
                    table_stats["record_count"] = 0
            except:
                table_stats["record_count"] = 0

            stats["tables"][table_name] = table_stats

        return stats

    def _validate_primary_index(self, index_type: str):
        return self.INDEX_TYPES.get(index_type, {}).get("primary", False)

    def _validate_secondary_index(self, index_type: str):
        return self.INDEX_TYPES.get(index_type, {}).get("secondary", False)

    def _get_field_info(self, table: Table, field_name: str):
        for fname, ftype, fsize in table.all_fields:
            if fname == field_name:
                return (ftype, fsize)
        return None

    def _create_primary_index(self, table: Table, index_type: str, csv_filename: str):
        if index_type == "ISAM":
            from ..isam.primary import ISAMPrimaryIndex

            primary_dir = os.path.join(self.base_dir, "primary")
            os.makedirs(primary_dir, exist_ok=True)
            primary_filename = os.path.join(primary_dir, "datos.dat")

            return ISAMPrimaryIndex(table, primary_filename)

        elif index_type == "SEQUENTIAL":
            from ..sequential_file.sequential_file import SequentialFile

            primary_dir = os.path.join(self.base_dir, "primary")
            os.makedirs(primary_dir, exist_ok=True)
            main_filename = os.path.join(primary_dir, "main.dat")
            aux_filename = os.path.join(primary_dir, "aux.dat")

            extra_fields = {"active": ("BOOL", 1)}
            table_with_active = Table(
                table_name=table.table_name,
                sql_fields=table.sql_fields,
                key_field=table.key_field,
                extra_fields=extra_fields
            )
            return SequentialFile(main_filename, aux_filename, table_with_active)

        elif index_type == "BTREE":
            primary_dir = os.path.join(self.base_dir, "primary")
            os.makedirs(primary_dir, exist_ok=True)
            primary_filename = os.path.join(primary_dir, "btree_primary.pkl")
            return BPlusTreeClusteredIndex(
                order=4,
                key_column=table.key_field,
                file_path=primary_filename,
                record_class=Record
            )


        raise NotImplementedError(f"Primary index type {index_type} not implemented yet")

    def _create_secondary_index(self, table: Table, field_name: str, index_type: str, csv_filename: str):
        field_type, field_size = self._get_field_info(table, field_name)

        if index_type == "ISAM":
            from ..obsolete.secondary import ISAMSecondaryIndexINT, ISAMSecondaryIndexCHAR, ISAMSecondaryIndexFLOAT

            secondary_dir = os.path.join(self.base_dir, "secondary")
            os.makedirs(secondary_dir, exist_ok=True)

            table_info = self.tables[table.table_name]
            primary_index = table_info["primary_index"]
            filename = os.path.join(secondary_dir, f"{table.table_name}_{field_name}_isam.dat")

            if field_type == "INT":
                return ISAMSecondaryIndexINT(field_name, primary_index, filename)
            elif field_type == "CHAR":
                return ISAMSecondaryIndexCHAR(field_name, field_size, primary_index, filename)
            elif field_type == "FLOAT":
                return ISAMSecondaryIndexFLOAT(field_name, primary_index, filename)
            else:
                raise NotImplementedError(f"ISAM secondary index para tipo {field_type} no implementado")
        elif index_type == "BTREE":
            secondary_dir = os.path.join(self.base_dir, "secondary")
            os.makedirs(secondary_dir, exist_ok=True)
            
            filename = os.path.join(secondary_dir, f"{table.table_name}_{field_name}_btree.pkl")
            
            return BPlusTreeUnclusteredIndex(
                order=4,
                index_column=field_name,
                file_path=filename
            )
        elif index_type == "HASH":
            from ..extendible_hashing.extendible_hashing import ExtendibleHashing

            secondary_dir = os.path.join(self.base_dir, "secondary")
            os.makedirs(secondary_dir, exist_ok=True)

            data_filename = os.path.join(secondary_dir, f"{table.table_name}_{field_name}")

            return ExtendibleHashing(data_filename, field_name, field_type, field_size, is_primary=False)
        elif index_type == "RTREE":
            raise NotImplementedError(f"R-Tree secondary index not implemented yet")

        raise NotImplementedError(f"Secondary index type {index_type} not implemented yet")

    def get_last_operation_metrics(self, table_name: str, index_type: str = "primary", field_name: str = None):
        if table_name not in self.tables:
            return None

        table_info = self.tables[table_name]

        if index_type == "primary":
            index = table_info["primary_index"]
        elif index_type == "secondary" and field_name:
            if field_name not in table_info["secondary_indexes"]:
                return None
            index = table_info["secondary_indexes"][field_name]["index"]
        else:
            return None

        if hasattr(index, 'performance') and hasattr(index.performance, 'last_result'):
            return index.performance.last_result
        return None

    def extract_metrics_from_result(self, result):
        if isinstance(result, OperationResult):
            return {
                "execution_time_ms": result.execution_time_ms,
                "disk_reads": result.disk_reads,
                "disk_writes": result.disk_writes,
                "total_disk_accesses": result.total_disk_accesses,
                "data": result.data
            }
        return {"data": result, "execution_time_ms": 0, "disk_reads": 0, "disk_writes": 0, "total_disk_accesses": 0}

    def print_operation_summary(self, result, operation_name: str = "Operation"):
        if isinstance(result, OperationResult):
            print(f"{operation_name} completed:")
            print(f"  Time: {result.execution_time_ms:.2f} ms")
            print(f"  Disk accesses: {result.total_disk_accesses} (R:{result.disk_reads}, W:{result.disk_writes})")
        else:
            print(f"{operation_name} completed (no metrics available)")



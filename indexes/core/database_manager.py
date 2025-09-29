import os
from typing import Dict, Any, List, Tuple, Optional
from .record import Table, Record
from .performance_tracker import OperationResult, PerformanceTracker


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

        table_info["primary_index"] = self._create_primary_index(
            table, primary_index_type, csv_filename
        )

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

        primary_result = primary_index.insert(record)

        total_reads = primary_result.disk_reads
        total_writes = primary_result.disk_writes
        total_time = primary_result.execution_time_ms

        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]
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
            result = primary_index.search(value)
            if result.data:
                return OperationResult([result.data], result.execution_time_ms, result.disk_reads, result.disk_writes)
            else:
                return OperationResult([], result.execution_time_ms, result.disk_reads, result.disk_writes)

        elif field_name in table_info["secondary_indexes"]:
            secondary_index = table_info["secondary_indexes"][field_name]["index"]
            primary_index = table_info["primary_index"]

            secondary_result = secondary_index.search(value)
            if not secondary_result.data:
                return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes)

            total_reads = secondary_result.disk_reads
            total_writes = secondary_result.disk_writes
            total_time = secondary_result.execution_time_ms

            if secondary_result.data:
                matching_records = []
                for primary_key in secondary_result.data:
                    primary_result = primary_index.search_without_metrics(primary_key)
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
            return primary_index.range_search(start_key, end_key)

        elif field_name in table_info["secondary_indexes"]:
            secondary_index = table_info["secondary_indexes"][field_name]["index"]
            primary_index = table_info["primary_index"]

            secondary_result = secondary_index.range_search(start_key, end_key)
            if not secondary_result.data:
                return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes)

            total_reads = secondary_result.disk_reads
            total_writes = secondary_result.disk_writes
            total_time = secondary_result.execution_time_ms

            if secondary_result.data:
                matching_records = []
                for primary_key in secondary_result.data:
                    primary_result = primary_index.search_without_metrics(primary_key)
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

            for fname, index_info in table_info["secondary_indexes"].items():
                secondary_index = index_info["index"]
                secondary_result = secondary_index.delete(record)
                total_reads += secondary_result.disk_reads
                total_writes += secondary_result.disk_writes
                total_time += secondary_result.execution_time_ms

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
            raise NotImplementedError(f"B+Tree index not implemented yet")

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
            raise NotImplementedError(f"B+Tree secondary index not implemented yet")
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


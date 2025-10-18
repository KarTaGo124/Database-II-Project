import os
from .record import Table, Record, IndexRecord
from .performance_tracker import OperationResult

from ..bplus_tree.bplus_tree_clustered import BPlusTreeClusteredIndex
from ..bplus_tree.bplus_tree_unclustered import BPlusTreeUnclusteredIndex
from ..isam.primary import ISAMPrimaryIndex
from ..extendible_hashing.extendible_hashing import ExtendibleHashing
from ..sequential_file.sequential_file import SequentialFile

class DatabaseManager:

    INDEX_TYPES = {
        "SEQUENTIAL": {"primary": True, "secondary": False},
        "ISAM": {"primary": True, "secondary": False},
        "BTREE": {"primary": True, "secondary": True},
        "HASH": {"primary": False, "secondary": True},
        "RTREE": {"primary": False, "secondary": True}
    }

    def __init__(self, database_name: str = None):
        self.tables = {}
        self.database_name = database_name or "default_db"
        self.base_dir = os.path.join("data", "database")
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

        if primary_index_type == "SEQUENTIAL":
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

        # ====== Construcción rápida: ordenar + "bulk mode" para reducir I/O ======
        if scan_existing:
            primary_index = table_info["primary_index"]
            if hasattr(primary_index, 'scan_all'):
                try:
                    # 1) Activar modo BULK si el índice lo soporta
                    if hasattr(secondary_index, "begin_bulk"):
                        secondary_index.begin_bulk()

                    scan_result = primary_index.scan_all()
                    existing_records = scan_result.data

                    field_type, field_size = field_info

                    def _sort_key(v):
                        if v is None:
                            return (0, "")
                        if isinstance(v, bytes):
                            try:
                                s = v.decode("utf-8", errors="ignore").rstrip("\x00")
                            except Exception:
                                s = str(v)
                            return (1, s)
                        if isinstance(v, str):
                            return (1, v)
                        return (2, v)

                    pairs = []
                    for record in existing_records:
                        raw_val = getattr(record, field_name, None)
                        if raw_val is None:
                            continue
                        # normalizar SIEMPRE la clave secundaria
                        sec_val = self._normalize_for_field(field_type, field_size, raw_val)
                        pk = record.get_key()
                        pairs.append((_sort_key(sec_val), sec_val, pk))

                    pairs.sort(key=lambda t: t[0])

                    for _sk, sec_val, pk in pairs:
                        index_record = IndexRecord(field_type, field_size)
                        index_record.set_index_data(sec_val, pk)  # sec_val ya normalizado
                        secondary_index.insert(index_record)

                except Exception as e:
                    # roll back del índice
                    del table_info["secondary_indexes"][field_name]
                    if hasattr(secondary_index, 'drop_index'):
                        secondary_index.drop_index()
                    raise ValueError(f"Error indexing existing records: {e}")
                finally:
                    # 2) Cerrar modo BULK (persistir metadata pendiente y flush final)
                    if hasattr(secondary_index, "end_bulk"):
                        secondary_index.end_bulk()

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

        breakdown = {
            "primary_metrics": {"reads": primary_result.disk_reads, "writes": primary_result.disk_writes, "time_ms": primary_result.execution_time_ms}
        }

        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]

            field_type, field_size = self._get_field_info(table_info["table"], field_name)
            secondary_value = getattr(record, field_name)
            primary_key = record.get_key()

            # normalizar SIEMPRE la clave secundaria antes de indexar
            secondary_value = self._normalize_for_field(field_type, field_size, secondary_value)

            index_record = IndexRecord(field_type, field_size)
            index_record.set_index_data(secondary_value, primary_key)

            secondary_result = secondary_index.insert(index_record)
            total_reads += secondary_result.disk_reads
            total_writes += secondary_result.disk_writes
            total_time += secondary_result.execution_time_ms

            breakdown[f"secondary_metrics_{field_name}"] = {
                "reads": secondary_result.disk_reads,
                "writes": secondary_result.disk_writes,
                "time_ms": secondary_result.execution_time_ms
            }

        return OperationResult(primary_result.data, total_time, total_reads, total_writes, primary_result.rebuild_triggered, breakdown)

    def search(self, table_name: str, value, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        # === BÚSQUEDA POR PRIMARY KEY ===
        if field_name is None:
            primary_index = table_info["primary_index"]
            result = primary_index.search(value)
            if result.data:
                return OperationResult([result.data], result.execution_time_ms, result.disk_reads, result.disk_writes)
            else:
                return OperationResult([], result.execution_time_ms, result.disk_reads, result.disk_writes)

        # === BÚSQUEDA POR ÍNDICE SECUNDARIO (si existe) ===
        elif field_name in table_info["secondary_indexes"]:
            secondary_index_info = table_info["secondary_indexes"][field_name]
            secondary_index = secondary_index_info["index"]
            primary_index = table_info["primary_index"]

            # Detectar tipo de campo
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")
            field_type, field_size = field_info

            # Para FLOAT: usar un rango pequeño alrededor del valor (tolerancia a precisión binaria)
            if field_type == "FLOAT":
                try:
                    qv = float(value)
                except (ValueError, TypeError):
                    return OperationResult([], 0, 0, 0)
                EPS = 1e-3  # tolerancia (0.001). Ajustable si lo deseas.
                secondary_result = secondary_index.range_search(qv - EPS, qv + EPS)
            else:
                # Igual que antes para otros tipos (exact match) con normalización
                norm = self._normalize_for_field(field_type, field_size, value)
                secondary_result = secondary_index.search(norm)

            if not secondary_result.data:
                breakdown = {
                    "primary_metrics": {"reads": 0, "writes": 0, "time_ms": 0},
                    "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
                }
                return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes, operation_breakdown=breakdown)

            total_reads = secondary_result.disk_reads
            total_writes = secondary_result.disk_writes
            total_time = secondary_result.execution_time_ms

            primary_lookup_reads = 0
            primary_lookup_writes = 0
            primary_lookup_time = 0

            matching_records = []
            for primary_key in secondary_result.data:
                primary_res = primary_index.search(primary_key)
                primary_lookup_reads += primary_res.disk_reads
                primary_lookup_writes += primary_res.disk_writes
                primary_lookup_time += primary_res.execution_time_ms
                if primary_res.data:
                    # Si es FLOAT y pedimos igualdad, filtramos con tolerancia final
                    if field_type == "FLOAT":
                        try:
                            qv = float(value)
                            rv = float(getattr(primary_res.data, field_name))
                            if abs(rv - qv) > 1e-3:
                                continue
                        except Exception:
                            continue
                    matching_records.append(primary_res.data)

            total_reads += primary_lookup_reads
            total_writes += primary_lookup_writes
            total_time += primary_lookup_time

            breakdown = {
                "primary_metrics": {"reads": primary_lookup_reads, "writes": primary_lookup_writes, "time_ms": primary_lookup_time},
                "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
            }
            return OperationResult(matching_records, total_time, total_reads, total_writes, operation_breakdown=breakdown)

        # === SIN ÍNDICE SECUNDARIO: full scan del primario ===
        else:
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")
            field_type, _ = field_info

            primary_index = table_info["primary_index"]
            if not hasattr(primary_index, 'scan_all'):
                raise NotImplementedError(f"Full scan not supported for {table_info['primary_type']} index")

            scan_result = primary_index.scan_all()
            all_records = scan_result.data

            matching_records = []
            if field_type == "FLOAT":
                # Comparación numérica con tolerancia
                try:
                    qv = float(value)
                except (ValueError, TypeError):
                    return OperationResult([], scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)
                EPS = 1e-3
                for r in all_records:
                    try:
                        rv = float(getattr(r, field_name))
                        if abs(rv - qv) <= EPS:
                            matching_records.append(r)
                    except Exception:
                        continue
            elif field_type == "INT":
                try:
                    qv = int(value)
                except (ValueError, TypeError):
                    return OperationResult([], scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)
                for r in all_records:
                    try:
                        rv = int(getattr(r, field_name))
                        if rv == qv:
                            matching_records.append(r)
                    except Exception:
                        continue
            else:
                # Fallback string exacto
                val_str = value.decode('utf-8').rstrip('\x00').rstrip() if hasattr(value, 'decode') else str(value).rstrip()
                for r in all_records:
                    rv = getattr(r, field_name, None)
                    if rv is None:
                        continue
                    rv_str = rv.decode('utf-8').rstrip('\x00').rstrip() if hasattr(rv, 'decode') else str(rv).rstrip()
                    if rv_str == val_str:
                        matching_records.append(r)

            return OperationResult(matching_records, scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)

    def range_search(self, table_name: str, start_key, end_key, field_name: str = None, spatial_type: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name is None:
            primary_index = table_info["primary_index"]
            return primary_index.range_search(start_key, end_key)

        elif field_name in table_info["secondary_indexes"]:
            secondary_info = table_info["secondary_indexes"][field_name]
            secondary_index = secondary_info["index"]
            secondary_type = secondary_info["type"]
            primary_index = table_info["primary_index"]

            if secondary_type == "HASH":
                raise NotImplementedError(f"Range search is not supported for HASH indexes (secondary index on '{field_name}'). Hash indexes are optimized for exact key lookups only.")
            
            if secondary_type == "RTREE":
                if spatial_type is None:
                    raise ValueError("spatial_type is required for R-Tree searches. Use 'radius' or 'knn'")
                secondary_result = secondary_index.range_search(start_key, end_key, spatial_type)
            else:
                secondary_result = secondary_index.range_search(start_key, end_key)
                
            if not secondary_result.data:
                breakdown = {
                    "primary_metrics": {"reads": 0, "writes": 0, "time_ms": 0},
                    "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
                }
                return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes, operation_breakdown=breakdown)

            total_reads = secondary_result.disk_reads
            total_writes = secondary_result.disk_writes
            total_time = secondary_result.execution_time_ms

            primary_lookup_reads = 0
            primary_lookup_writes = 0
            primary_lookup_time = 0

            if secondary_result.data:
                matching_records = []
                for primary_key in secondary_result.data:
                    primary_result = primary_index.search(primary_key)
                    primary_lookup_reads += primary_result.disk_reads
                    primary_lookup_writes += primary_result.disk_writes
                    primary_lookup_time += primary_result.execution_time_ms

                    if primary_result.data:
                        matching_records.append(primary_result.data)

                total_reads += primary_lookup_reads
                total_writes += primary_lookup_writes
                total_time += primary_lookup_time
            else:
                matching_records = []

            breakdown = {
                "primary_metrics": {"reads": primary_lookup_reads, "writes": primary_lookup_writes, "time_ms": primary_lookup_time},
                "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
            }

            return OperationResult(matching_records, total_time, total_reads, total_writes, operation_breakdown=breakdown)

        else:
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")

            primary_index = table_info["primary_index"]

            if hasattr(primary_index, 'scan_all'):
                scan_result = primary_index.scan_all()
                all_records = scan_result.data
            else:
                raise NotImplementedError(f"Full scan not supported for {table_info['primary_type']} index")

            matching_records = []
            field_type, _ = field_info

            for record in all_records:
                record_value = getattr(record, field_name, None)
                if record_value is not None:
                    if field_type == "FLOAT":
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
            primary_key = value

            breakdown = {}
            for fname in table_info["secondary_indexes"].keys():
                breakdown[f"secondary_metrics_{fname}"] = {"reads": 0, "writes": 0, "time_ms": 0}
            breakdown["primary_metrics"] = {"reads": search_result.disk_reads, "writes": search_result.disk_writes, "time_ms": search_result.execution_time_ms}

            total_reads = search_result.disk_reads
            total_writes = search_result.disk_writes
            total_time = search_result.execution_time_ms

            for fname, index_info in table_info["secondary_indexes"].items():
                secondary_index = index_info["index"]
                secondary_value = getattr(record, fname)
                # normalizar
                ftype, fsize = self._get_field_info(table_info["table"], fname)
                secondary_value = self._normalize_for_field(ftype, fsize, secondary_value)

                sec_result = secondary_index.delete(secondary_value, primary_key)

                breakdown[f"secondary_metrics_{fname}"]["reads"] += sec_result.disk_reads
                breakdown[f"secondary_metrics_{fname}"]["writes"] += sec_result.disk_writes
                breakdown[f"secondary_metrics_{fname}"]["time_ms"] += sec_result.execution_time_ms

                total_reads += sec_result.disk_reads
                total_writes += sec_result.disk_writes
                total_time += sec_result.execution_time_ms

            delete_result = primary_index.delete(value)

            breakdown["primary_metrics"]["reads"] += delete_result.disk_reads
            breakdown["primary_metrics"]["writes"] += delete_result.disk_writes
            breakdown["primary_metrics"]["time_ms"] += delete_result.execution_time_ms

            total_reads += delete_result.disk_reads
            total_writes += delete_result.disk_writes
            total_time += delete_result.execution_time_ms

            return OperationResult(delete_result.data, total_time, total_reads, total_writes, delete_result.rebuild_triggered, operation_breakdown=breakdown)

        elif field_name in table_info["secondary_indexes"]:
            primary_index = table_info["primary_index"]
            sec_info = table_info["secondary_indexes"][field_name]
            secondary_index = sec_info["index"]

            # 1) Buscar todos los PKs para esa clave en el secundario (agnóstico al tipo de índice)
            sec_search = secondary_index.search(value)
            deleted_pks = sec_search.data if isinstance(sec_search.data, list) else []

            # Métricas iniciales / breakdown
            breakdown = {}
            for fname in table_info["secondary_indexes"].keys():
                breakdown[f"secondary_metrics_{fname}"] = {"reads": 0, "writes": 0, "time_ms": 0}
            breakdown["primary_metrics"] = {"reads": 0, "writes": 0, "time_ms": 0}

            # Registrar las métricas de la búsqueda secundaria
            breakdown[f"secondary_metrics_{field_name}"]["reads"] += getattr(sec_search, "disk_reads", 0)
            breakdown[f"secondary_metrics_{field_name}"]["writes"] += getattr(sec_search, "disk_writes", 0)
            breakdown[f"secondary_metrics_{field_name}"]["time_ms"] += getattr(sec_search, "execution_time_ms", 0)

            if not deleted_pks:
                # Nada que borrar
                total_reads = getattr(sec_search, "disk_reads", 0)
                total_writes = getattr(sec_search, "disk_writes", 0)
                total_time = getattr(sec_search, "execution_time_ms", 0)
                return OperationResult(0, total_time, total_reads, total_writes, operation_breakdown=breakdown)

            deleted_count = 0
            total_reads  = getattr(sec_search, "disk_reads", 0)
            total_writes = getattr(sec_search, "disk_writes", 0)
            total_time   = getattr(sec_search, "execution_time_ms", 0)

            # 2) Por cada PK, borrar (valor, pk) del secundario y luego del primario
            for pk in deleted_pks:
                # 2a) Traer el registro para conocer los otros secundarios
                search_result = primary_index.search(pk)
                breakdown["primary_metrics"]["reads"]  += search_result.disk_reads
                breakdown["primary_metrics"]["writes"] += search_result.disk_writes
                breakdown["primary_metrics"]["time_ms"]+= search_result.execution_time_ms
                total_reads  += search_result.disk_reads
                total_writes += search_result.disk_writes
                total_time   += search_result.execution_time_ms

                if not search_result.data:
                    # El primario no lo tiene; intenta limpiar al menos el par (value, pk) en el índice actual
                    del_res = secondary_index.delete(value, pk)
                    breakdown[f"secondary_metrics_{field_name}"]["reads"]  += del_res.disk_reads
                    breakdown[f"secondary_metrics_{field_name}"]["writes"] += del_res.disk_writes
                    breakdown[f"secondary_metrics_{field_name}"]["time_ms"]+= del_res.execution_time_ms
                    total_reads  += del_res.disk_reads
                    total_writes += del_res.disk_writes
                    total_time   += del_res.execution_time_ms
                    continue

                record = search_result.data

                # 2b) Borrar del índice secundario de la condición (value, pk)
                del_res = secondary_index.delete(value, pk)
                breakdown[f"secondary_metrics_{field_name}"]["reads"]  += del_res.disk_reads
                breakdown[f"secondary_metrics_{field_name}"]["writes"] += del_res.disk_writes
                breakdown[f"secondary_metrics_{field_name}"]["time_ms"]+= del_res.execution_time_ms
                total_reads  += del_res.disk_reads
                total_writes += del_res.disk_writes
                total_time   += del_res.execution_time_ms

                # 2c) Borrar del resto de secundarios (si existen)
                for fname, idx_info in table_info["secondary_indexes"].items():
                    if fname == field_name:
                        continue
                    sec_idx = idx_info["index"]
                    sec_val = getattr(record, fname)
                    sec_del = sec_idx.delete(sec_val, pk)
                    breakdown[f"secondary_metrics_{fname}"]["reads"]  += sec_del.disk_reads
                    breakdown[f"secondary_metrics_{fname}"]["writes"] += sec_del.disk_writes
                    breakdown[f"secondary_metrics_{fname}"]["time_ms"]+= sec_del.execution_time_ms
                    total_reads  += sec_del.disk_reads
                    total_writes += sec_del.disk_writes
                    total_time   += sec_del.execution_time_ms

                # 2d) Borrar en el primario
                prim_del = primary_index.delete(pk)
                breakdown["primary_metrics"]["reads"]  += prim_del.disk_reads
                breakdown["primary_metrics"]["writes"] += prim_del.disk_writes
                breakdown["primary_metrics"]["time_ms"]+= prim_del.execution_time_ms
                total_reads  += prim_del.disk_reads
                total_writes += prim_del.disk_writes
                total_time   += prim_del.execution_time_ms

                if prim_del.data:
                    deleted_count += 1

            return OperationResult(deleted_count, total_time, total_reads, total_writes, operation_breakdown=breakdown)


        else:
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")

            primary_index = table_info["primary_index"]

            scan_result = primary_index.scan_all()
            all_records = scan_result.data

            matching_records = []
            field_type, field_size = field_info

            # comparar por igualdad (string) en full scan
            val_str = value.decode('utf-8').rstrip('\x00').rstrip() if hasattr(value, 'decode') else str(value).rstrip()
            for record in all_records:
                raw = getattr(record, field_name, None)
                raw_norm = self._normalize_for_field(field_type, field_size, raw)
                if isinstance(raw_norm, float):
                    try:
                        if abs(raw_norm - float(val_str)) <= 1e-3:
                            matching_records.append(record)
                    except Exception:
                        pass
                elif isinstance(raw_norm, int):
                    try:
                        if raw_norm == int(val_str):
                            matching_records.append(record)
                    except Exception:
                        pass
                else:
                    if str(raw_norm) == val_str:
                        matching_records.append(record)

            if not matching_records:
                return OperationResult(0, scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)

            deleted_count = 0
            total_reads = scan_result.disk_reads
            total_writes = scan_result.disk_writes
            total_time = scan_result.execution_time_ms

            secondary_delete_metrics = {}
            for fname in table_info["secondary_indexes"].keys():
                secondary_delete_metrics[fname] = {"reads": 0, "writes": 0, "time_ms": 0}

            primary_delete_reads = 0
            primary_delete_writes = 0
            primary_delete_time = 0

            for record in matching_records:
                pk = record.get_key()

                for fname, index_info in table_info["secondary_indexes"].items():
                    sec_index = index_info["index"]
                    sec_value = getattr(record, fname)
                    ft, fs = self._get_field_info(table_info["table"], fname)
                    sec_value = self._normalize_for_field(ft, fs, sec_value)

                    sec_result = sec_index.delete(sec_value, pk)
                    secondary_delete_metrics[fname]["reads"] += sec_result.disk_reads
                    secondary_delete_metrics[fname]["writes"] += sec_result.disk_writes
                    secondary_delete_metrics[fname]["time_ms"] += sec_result.execution_time_ms
                    total_reads += sec_result.disk_reads
                    total_writes += sec_result.disk_writes
                    total_time += sec_result.execution_time_ms

                prim_delete = primary_index.delete(pk)
                primary_delete_reads += prim_delete.disk_reads
                primary_delete_writes += prim_delete.disk_writes
                primary_delete_time += prim_delete.execution_time_ms
                total_reads += prim_delete.disk_reads
                total_writes += prim_delete.disk_writes
                total_time += prim_delete.execution_time_ms

                if prim_delete.data:
                    deleted_count += 1

            breakdown = {
                "primary_metrics": {
                    "reads": scan_result.disk_reads + primary_delete_reads,
                    "writes": scan_result.disk_writes + primary_delete_writes,
                    "time_ms": scan_result.execution_time_ms + primary_delete_time
                }
            }

            for fname, metrics in secondary_delete_metrics.items():
                breakdown[f"secondary_metrics_{fname}"] = metrics.copy()

            return OperationResult(deleted_count, total_time, total_reads, total_writes, operation_breakdown=breakdown)

    def range_delete(self, table_name: str, start_key, end_key, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]

        search_result = self.range_search(table_name, start_key, end_key, field_name)

        if not search_result.data:
            return OperationResult(0, search_result.execution_time_ms, search_result.disk_reads, search_result.disk_writes, operation_breakdown=search_result.operation_breakdown)

        deleted_count = 0
        total_reads = search_result.disk_reads
        total_writes = search_result.disk_writes
        total_time = search_result.execution_time_ms

        secondary_delete_metrics = {}
        for fname in table_info["secondary_indexes"].keys():
            secondary_delete_metrics[fname] = {"reads": 0, "writes": 0, "time_ms": 0}

        primary_delete_reads = 0
        primary_delete_writes = 0
        primary_delete_time = 0

        for record in search_result.data:
            primary_key = record.get_key()

            for fname, index_info in table_info["secondary_indexes"].items():
                sec_index = index_info["index"]
                sec_value = getattr(record, fname)
                ft, fs = self._get_field_info(table_info["table"], fname)
                sec_value = self._normalize_for_field(ft, fs, sec_value)

                sec_result = sec_index.delete(sec_value, primary_key)
                secondary_delete_metrics[fname]["reads"] += sec_result.disk_reads
                secondary_delete_metrics[fname]["writes"] += sec_result.disk_writes
                secondary_delete_metrics[fname]["time_ms"] += sec_result.execution_time_ms
                total_reads += sec_result.disk_reads
                total_writes += sec_result.disk_writes
                total_time += sec_result.execution_time_ms

            prim_delete = primary_index.delete(primary_key)
            primary_delete_reads += prim_delete.disk_reads
            primary_delete_writes += prim_delete.disk_writes
            primary_delete_time += prim_delete.execution_time_ms
            total_reads += prim_delete.disk_reads
            total_writes += prim_delete.disk_writes
            total_time += prim_delete.execution_time_ms

            if prim_delete.data:
                deleted_count += 1

        breakdown = {}

        if field_name is None:
            breakdown["primary_metrics"] = {
                "reads": search_result.disk_reads + primary_delete_reads,
                "writes": search_result.disk_writes + primary_delete_writes,
                "time_ms": search_result.execution_time_ms + primary_delete_time
            }

            for fname, metrics in secondary_delete_metrics.items():
                breakdown[f"secondary_metrics_{fname}"] = metrics.copy()

        elif search_result.operation_breakdown and "secondary_metrics" in search_result.operation_breakdown:
            breakdown[f"secondary_metrics_{field_name}"] = search_result.operation_breakdown["secondary_metrics"].copy()
            breakdown["primary_metrics"] = search_result.operation_breakdown["primary_metrics"].copy()

            if field_name in secondary_delete_metrics:
                breakdown[f"secondary_metrics_{field_name}"]["reads"] += secondary_delete_metrics[field_name]["reads"]
                breakdown[f"secondary_metrics_{field_name}"]["writes"] += secondary_delete_metrics[field_name]["writes"]
                breakdown[f"secondary_metrics_{field_name}"]["time_ms"] += secondary_delete_metrics[field_name]["time_ms"]

            for fname, metrics in secondary_delete_metrics.items():
                if fname != field_name:
                    breakdown[f"secondary_metrics_{fname}"] = metrics.copy()

            breakdown["primary_metrics"]["reads"] += primary_delete_reads
            breakdown["primary_metrics"]["writes"] += primary_delete_writes
            breakdown["primary_metrics"]["time_ms"] += primary_delete_time

        else:
            breakdown["primary_metrics"] = {
                "reads": search_result.disk_reads + primary_delete_reads,
                "writes": search_result.disk_writes + primary_delete_writes,
                "time_ms": search_result.execution_time_ms + primary_delete_time
            }

            for fname, metrics in secondary_delete_metrics.items():
                breakdown[f"secondary_metrics_{fname}"] = metrics.copy()

        return OperationResult(deleted_count, total_time, total_reads, total_writes, operation_breakdown=breakdown if breakdown else None)

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
                if hasattr(primary_index, 'scan_all'):
                    scan_result = primary_index.scan_all()
                    table_stats["record_count"] = len(scan_result.data) if scan_result.data else 0
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

    def _normalize_for_field(self, field_type: str, field_size: int, value):
        """Normaliza un valor para usarlo como clave de índice secundario."""
        if value is None:
            return None
        if field_type == "CHAR":
            if isinstance(value, (bytes, bytearray)):
                value = bytes(value).decode("utf-8", "ignore")
            return str(value).rstrip("\x00").strip()
        if field_type == "INT":
            if isinstance(value, (bytes, bytearray)):
                value = bytes(value).decode("utf-8", "ignore").strip()
            return int(value)
        if field_type == "FLOAT":
            if isinstance(value, (bytes, bytearray)):
                value = bytes(value).decode("utf-8", "ignore").strip()
            return float(value)
        # ARRAY/otros tipos: devolver tal cual
        return value

    def _create_primary_index(self, table: Table, index_type: str, csv_filename: str):
        if index_type == "ISAM":
            primary_dir = os.path.join(self.base_dir, table.table_name, f"primary_isam_{table.key_field}")
            os.makedirs(primary_dir, exist_ok=True)
            primary_filename = os.path.join(primary_dir, "datos.dat")
            return ISAMPrimaryIndex(table, primary_filename)

        elif index_type == "SEQUENTIAL":
            primary_dir = os.path.join(self.base_dir, table.table_name, f"primary_sequential_{table.key_field}")
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
            primary_dir = os.path.join(self.base_dir, table.table_name, f"primary_btree_{table.key_field}")
            os.makedirs(primary_dir, exist_ok=True)
            primary_filename = os.path.join(primary_dir, "btree_clustered")
            return BPlusTreeClusteredIndex(
                order=128,
                key_column=table.key_field,
                file_path=primary_filename,
                record_class=Record
            )

        raise NotImplementedError(f"Primary index type {index_type} not implemented yet")

    def _create_secondary_index(self, table: Table, field_name: str, index_type: str, csv_filename: str):
        field_type, field_size = self._get_field_info(table, field_name)

        if index_type == "BTREE":
            secondary_dir = os.path.join(self.base_dir, table.table_name, f"secondary_btree_{field_name}")
            os.makedirs(secondary_dir, exist_ok=True)

            filename = os.path.join(secondary_dir, "btree_unclustered")

            return BPlusTreeUnclusteredIndex(
                order=128,
                index_column=field_name,
                file_path=filename
            )
        elif index_type == "HASH":
            secondary_dir = os.path.join(self.base_dir, table.table_name, f"secondary_hash_{field_name}")
            os.makedirs(secondary_dir, exist_ok=True)
            data_filename = os.path.join(secondary_dir, "datos")
            return ExtendibleHashing(data_filename, field_name, field_type, field_size, is_primary=False)

        elif index_type == "RTREE":
            secondary_dir = os.path.join(self.base_dir, "secondary")
            os.makedirs(secondary_dir, exist_ok=True)
            
            if field_type != "ARRAY":
                raise ValueError(f"R-Tree indexes require ARRAY fields (spatial coordinates), got {field_type}")
            
            dimension = field_size
            
            table_info = self.tables[table.table_name]
            primary_index = table_info["primary_index"]
            filename = os.path.join(secondary_dir, f"{table.table_name}_{field_name}_rtree")
            
            from ..r_tree.r_tree import RTreeSecondaryIndex
            return RTreeSecondaryIndex(field_name, primary_index, filename, dimension=dimension)

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

    def scan_all(self, table_name: str):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]
        return primary_index.scan_all()

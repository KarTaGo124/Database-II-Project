import csv
from typing import Dict, Any, List, Tuple, Optional
from .plan_types import (
    CreateTablePlan, LoadDataPlan, SelectPlan, InsertPlan, DeletePlan,
    CreateIndexPlan, DropTablePlan, DropIndexPlan,
    ColumnDef, ColumnType, PredicateEq, PredicateBetween, PredicateInPointRadius, PredicateKNN
)
from indexes.core.record import Table, Record
from indexes.core.performance_tracker import OperationResult
from indexes.core.database_manager import DatabaseManager
from indexes.core.record import IndexRecord


class Executor:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def execute(self, plan):
        if isinstance(plan, CreateTablePlan):
            return self._create_table(plan)
        elif isinstance(plan, LoadDataPlan):
            return self._load_data(plan)
        elif isinstance(plan, SelectPlan):
            return self._select(plan)
        elif isinstance(plan, InsertPlan):
            return self._insert(plan)
        elif isinstance(plan, DeletePlan):
            return self._delete(plan)
        elif isinstance(plan, CreateIndexPlan):
            return self._create_index(plan)
        elif isinstance(plan, DropTablePlan):
            return self._drop_table(plan)
        elif isinstance(plan, DropIndexPlan):
            return self._drop_index(plan)
        else:
            raise NotImplementedError(f"Plan no soportado: {type(plan)}")

    def _col_to_physical(self, c: ColumnDef) -> Optional[Tuple[str, str, int]]:
        name = c.name
        kind = c.type.kind
        if kind == "INT":
            return (name, "INT", 4)
        if kind == "FLOAT":
            return (name, "FLOAT", 4)
        if kind == "VARCHAR":
            ln = c.type.length or 32
            return (name, "CHAR", ln)
        if kind == "DATE":
            return (name, "CHAR", 10)  # YYYY-MM-DD
        if kind == "ARRAY":
            dimensions = c.type.length or 2
            return (name, "ARRAY", dimensions)
        return None

    def _pick_primary(self, columns: List[ColumnDef]) -> Tuple[str, str]:
        pk_col = None
        for c in columns:
            if c.is_key:
                pk_col = c
                break
        if pk_col is None:
            for c in columns:
                if c.type.kind == "INT":
                    pk_col = c
                    break
        if pk_col is None:
            pk_col = columns[0]
        pk_name = pk_col.name
        idx_decl = (pk_col.index or "ISAM").upper()
        allowed_primary = {"ISAM", "SEQUENTIAL", "BTREE"}
        primary_index_type = idx_decl if idx_decl in allowed_primary else "ISAM"
        return pk_name, primary_index_type

    # ====== CREATE TABLE ======
    def _create_table(self, plan: CreateTablePlan):
        physical_fields: List[Tuple[str, str, int]] = []
        ignored_cols: List[str] = []
        secondary_decls: List[Tuple[str, str]] = []

        materialized = set()
        for c in plan.columns:
            phys = self._col_to_physical(c)
            if phys is None:
                ignored_cols.append(c.name)
            else:
                physical_fields.append(phys)
                materialized.add(c.name)
            if c.index and (not c.is_key) and (phys is not None):
                secondary_decls.append((c.name, c.index.upper()))

        if not physical_fields:
            raise ValueError("Ninguna columna soportada para almacenamiento físico")

        pk_field, primary_index_type = self._pick_primary(plan.columns)

        table = Table(
            table_name=plan.table,
            sql_fields=physical_fields,
            key_field=pk_field,
            extra_fields=None
        )
        self.db.create_table(table, primary_index_type=primary_index_type)

        unsupported: List[str] = []
        created_any = False
        for colname, idx_kind in secondary_decls:
            try:
                if self.db._validate_secondary_index(idx_kind):
                    # se crean (vacíos) aquí; durante LOAD los desactivaremos temporalmente
                    self.db.create_index(plan.table, colname, idx_kind, scan_existing=False)
                    created_any = True
                else:
                    unsupported.append(f"{colname}:{idx_kind}")
            except NotImplementedError:
                unsupported.append(f"{colname}:{idx_kind}(NotImpl)")
            except Exception as e:
                unsupported.append(f"{colname}:{idx_kind}({str(e)[:30]})")

        msg_parts = [f"OK: tabla {plan.table} creada (primario={primary_index_type}, key={pk_field})"]
        if ignored_cols:
            msg_parts.append(f"— Columnas no soportadas (ignoradas): {', '.join(ignored_cols)}")
        if unsupported or (secondary_decls and not created_any):
            if not unsupported:
                unsupported = [f"{c}:{k}" for c, k in secondary_decls]
            msg_parts.append(f"— Índices secundarios no soportados: {', '.join(unsupported)}")
        result_msg = " ".join(msg_parts)
        return OperationResult(result_msg, 0, 0, 0)

    # ====== helpers CSV ======
    def _defaults_for_field(self, ftype: str) -> Any:
        if ftype == "INT":
            return 0
        if ftype == "FLOAT":
            return 0.0
        if ftype == "CHAR":
            return ""
        if ftype == "BOOL":
            return False
        if ftype == "ARRAY":
            return (0.0, 0.0)
        return None

    def _cast_value(self, raw: str, ftype: str):
        if raw is None:
            return None
        raw = str(raw).strip()
        if raw == "":
            return self._defaults_for_field(ftype)
        if ftype == "INT":
            return int(raw)
        if ftype == "FLOAT":
            return float(raw)
        if ftype == "CHAR":
            return raw
        if ftype == "BOOL":
            return raw.lower() in ("1", "true", "t", "yes", "y", "si", "sí")
        return raw

    def _cast_date_ddmmyyyy_to_iso(self, s: str) -> str:
        s = (s or "").strip()
        if not s:
            return ""
        if "-" in s and len(s) == 10:
            return s
        parts = s.split("/")
        if len(parts) == 3:
            dd, mm, yyyy = parts
            dd = dd.zfill(2)
            mm = mm.zfill(2)
            return f"{yyyy}-{mm}-{dd}"
        return s

    def _guess_delimiter(self, header_line: str) -> str:
        return ";" if header_line.count(";") >= header_line.count(",") else ","

    def _spanish_header_mapping(self, headers: List[str]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for h in headers:
            hl = (h or "").strip().lower()
            if ("id" in hl) and ("venta" in hl or "id" == hl or hl.startswith("id ")):
                mapping[hl] = "id"; continue
            if "nombre" in hl:   mapping[hl] = "nombre";   continue
            if "cantidad" in hl: mapping[hl] = "cantidad"; continue
            if "precio" in hl:   mapping[hl] = "precio";   continue
            if "fecha" in hl:    mapping[hl] = "fecha";    continue
        return mapping

    # ====== LOAD DATA FROM FILE ======
    def _load_data(self, plan: LoadDataPlan):
        info = self.db.get_table_info(plan.table)
        if not info:
            raise ValueError(f"Tabla {plan.table} no existe; crea la tabla primero con CREATE TABLE")

        table_obj = self.db.tables[plan.table]["table"]
        phys_fields = table_obj.all_fields
        key_field = table_obj.key_field

        inserted = duplicates = cast_err = 0

        # ===== depuración =====
        PROGRESS_EVERY = 100
        MAX_ROWS = None
        import time
        t0 = time.perf_counter()
        last_t = t0
        none_key_rows = 0

        # === detectar delimitador
        try:
            with open(plan.filepath, "r", encoding="utf-8-sig", newline="") as fprobe:
                sample = fprobe.read(4096)
                import csv as _csv
                try:
                    delimiter = _csv.Sniffer().sniff(sample, delimiters=",;|\t").delimiter
                except Exception:
                    first_line = sample.splitlines()[0] if sample else ""
                    delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","
        except UnicodeDecodeError:
            with open(plan.filepath, "r", encoding="latin-1", newline="") as fprobe:
                sample = fprobe.read(4096)
                import csv as _csv
                try:
                    delimiter = _csv.Sniffer().sniff(sample, delimiters=",;|\t").delimiter
                except Exception:
                    first_line = sample.splitlines()[0] if sample else ""
                    delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","

        def _norm(s: str) -> str:
            return (s or "").replace("\ufeff", "").strip().lower()

        # === Desactivar secundarios mientras inserto
        tbl_info = self.db.tables[plan.table]
        saved_secondaries = tbl_info["secondary_indexes"]
        tbl_info["secondary_indexes"] = {}

        # === Inserción rápida (solo primario)
        try:
            try:
                f = open(plan.filepath, "r", encoding="utf-8-sig", newline="")
            except UnicodeDecodeError:
                f = open(plan.filepath, "r", encoding="latin-1", newline="")

            with f:
                reader = csv.reader(f, delimiter=delimiter)
                header = next(reader, None)
                if not header:
                    tbl_info["secondary_indexes"] = saved_secondaries
                    return OperationResult("CSV vacío: insertados=0", 0, 0, 0)

                header_norm = [_norm(h) for h in header]
                header_idx = {h: i for i, h in enumerate(header_norm)}

                syn_map = self._spanish_header_mapping(header)
                syn_map_norm = {_norm(k): v for k, v in syn_map.items()}

                user_map: Dict[str, Any] = {}
                if getattr(plan, "column_mappings", None):
                    for logical_col, csv_name_or_list in plan.column_mappings.items():
                        if isinstance(csv_name_or_list, list):
                            user_map[logical_col] = [_norm(x) for x in csv_name_or_list]
                        else:
                            user_map[logical_col] = _norm(csv_name_or_list)

                def _csv_index_for(logical_name: str) -> Optional[int]:
                    ln = _norm(logical_name)
                    if logical_name in user_map and isinstance(user_map[logical_name], str):
                        return header_idx.get(user_map[logical_name], None)
                    if ln in header_idx:
                        return header_idx[ln]
                    for h_norm, i in header_idx.items():
                        target = syn_map_norm.get(h_norm)
                        if target and _norm(target) == ln:
                            return i
                    compact = ln.replace(" ", "")
                    for h_norm, i in header_idx.items():
                        if h_norm.replace(" ", "") == compact:
                            return i
                    return None

                user_fields = [(name, ftype, fsize) for (name, ftype, fsize) in phys_fields if name not in ['active']]

                for row_idx, row_values in enumerate(reader, 1):
                    rec = Record(phys_fields, key_field)
                    ok_row = True

                    for field_name, field_type, field_size in user_fields:
                        try:
                            if field_type == "ARRAY" and field_name in user_map and isinstance(user_map[field_name], list):
                                csv_column_names_norm = user_map[field_name]
                                array_values: List[float] = []
                                for csv_norm in csv_column_names_norm:
                                    idx = header_idx.get(csv_norm)
                                    if idx is not None and idx < len(row_values):
                                        val = self._cast_value(row_values[idx], "FLOAT")
                                        array_values.append(val if val is not None else 0.0)
                                    else:
                                        array_values.append(0.0)
                                while len(array_values) < field_size:
                                    array_values.append(0.0)
                                array_values = array_values[:field_size]
                                rec.set_field_value(field_name, tuple(array_values))
                                continue

                            csv_idx = _csv_index_for(field_name)
                            raw = row_values[csv_idx] if (csv_idx is not None and csv_idx < len(row_values)) else None

                            if field_type == "CHAR" and field_name == "fecha":
                                raw = self._cast_date_ddmmyyyy_to_iso(str(raw) if raw is not None else "")

                            val = self._cast_value(raw, field_type)

                            if field_type == "ARRAY":
                                if isinstance(val, (list, tuple)):
                                    val = tuple(val)
                                else:
                                    val = tuple([0.0] * field_size)

                            rec.set_field_value(field_name, val)
                        except Exception:
                            ok_row = False
                            break

                    if 'active' in [name for (name, _, _) in phys_fields]:
                        rec.set_field_value('active', True)

                    if not ok_row:
                        cast_err += 1
                        continue

                    key_val = getattr(rec, key_field, None)
                    if key_val is None:
                        none_key_rows += 1

                    try:
                        res = self.db.insert(plan.table, rec)
                        if hasattr(res, "data") and (res.data is False):
                            duplicates += 1
                        else:
                            inserted += 1
                    except Exception:
                        cast_err += 1
                        continue

                    if (row_idx % PROGRESS_EVERY) == 0:
                        now = time.perf_counter()
                        dt = now - last_t
                        rate = PROGRESS_EVERY / dt if dt > 0 else float("inf")
                        total_dt = now - t0
                        print(f"[LOAD] {row_idx} filas, +{PROGRESS_EVERY} en {dt:.2f}s (~{rate:.1f} filas/s), total {total_dt:.2f}s; "
                              f"ins={inserted}, dup={duplicates}, err={cast_err}, none_key={none_key_rows}")
                        last_t = now

                    if MAX_ROWS is not None and row_idx >= MAX_ROWS:
                        break
        finally:
            # === Reconstrucción de índices secundarios con UN SOLO SCAN y progreso
            import time as _t
            print("[BUILD] Escaneando primario una vez para poblar índices secundarios…")
            scan_res = self.db.scan_all(plan.table)
            all_recs = scan_res.data or []
            print(f"[BUILD] Total registros a indexar: {len(all_recs)}")

            # Limpia secundarios y vuelve a crearlos vacíos, pero insertando en ORDEN por la clave secundaria
            tbl_info["secondary_indexes"] = {}
            for field_name, idx_info in saved_secondaries.items():
                idx_type = idx_info["type"]
                print(f"[BUILD] Creando índice {idx_type} sobre {field_name}…")
                self.db.create_index(plan.table, field_name, idx_type, scan_existing=False)

                sec = self.db.tables[plan.table]["secondary_indexes"][field_name]["index"]
                ftype, fsize = self._get_field_info_full(plan.table, field_name)

                # --- clave de ordenación estable (convierte bytes a str si es necesario)
                def _sec_key(r):
                    v = getattr(r, field_name)
                    if isinstance(v, (bytes, bytearray)):
                        v = bytes(v).decode("utf-8", "ignore").rstrip("\x00").strip()
                    return v

                # Ordena por la clave secundaria para minimizar splits/I/O
                pairs = [(_sec_key(r), r.get_key()) for r in all_recs]
                pairs.sort(key=lambda x: x[0])

                t0_build = _t.perf_counter()
                PROG = 1000
                for i, (sec_val, pk) in enumerate(pairs, 1):
                    idx_rec = IndexRecord(ftype, fsize)
                    idx_rec.set_index_data(sec_val, pk)
                    sec.insert(idx_rec)
                    if (i % PROG) == 0:
                        dt = _t.perf_counter() - t0_build
                        print(f"[BUILD] {field_name}: {i}/{len(pairs)} en {dt:.2f}s (~{i/max(dt,1e-9):.1f} ins/s)")
                dt_total = _t.perf_counter() - t0_build
                print(f"[BUILD] {field_name}: listo en {dt_total:.2f}s (reg/s ≈ {len(pairs)/max(dt_total,1e-9):.1f})")


        total_ms = (time.perf_counter() - t0) * 1000.0
        msg = f"CSV cargado: insertados={inserted}, duplicados={duplicates}, cast_err={cast_err}, none_key={none_key_rows}"
        return OperationResult(msg, total_ms, 0, 0)

    def _get_field_info_full(self, table_name: str, field_name: str):
        tinfo = self.db.tables[table_name]["table"]
        for fname, ftype, fsize in tinfo.all_fields:
            if fname == field_name:
                return ftype, fsize
        raise ValueError(f"Field {field_name} not found in table {table_name}")

    # ====== SELECT ======
    def _get_ftype(self, table: str, col: str) -> Optional[str]:
        tinfo = self.db.tables.get(table)
        if not tinfo:
            return None
        for (n, ftype, _sz) in tinfo["table"].all_fields:
            if n == col:
                return ftype
        return None

    def _project_records(self, records: List[Record], columns: Optional[List[str]]) -> List[Dict[str, Any]]:
        if not records:
            return []

        def _fmt(v):
            # --- NUEVO: formateo amable para floats y colecciones de floats (solo presentación)
            if isinstance(v, float):
                return round(v, 2)
            if isinstance(v, (list, tuple)):
                return type(v)(round(x, 2) if isinstance(x, float) else x for x in v)
            if isinstance(v, bytes):
                try:
                    return v.decode("utf-8").rstrip("\x00").strip()
                except UnicodeDecodeError:
                    return v.decode("utf-8", errors="replace").rstrip("\x00").strip()
            return v

        out = []
        names = [n for (n, _, _) in records[0].value_type_size]
        pick = names if (columns is None) else columns
        for r in records:
            obj = {}
            for c in pick:
                val = getattr(r, c, None)
                obj[c] = _fmt(val)
            out.append(obj)
        return out

    def _select(self, plan: SelectPlan):
        table = plan.table
        where = plan.where

        if where is None:
            res = self.db.scan_all(table)
            projected_data = self._project_records(res.data, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

        if isinstance(where, PredicateEq):
            col = where.column
            val = where.value

            res = self.db.search(table, val, field_name=col)
            data_list = res.data if isinstance(res.data, list) else ([res.data] if res.data else [])
            projected_data = self._project_records(data_list, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

        if isinstance(where, PredicateBetween):
            col = where.column
            lo = where.lo
            hi = where.hi
            res = self.db.range_search(table, lo, hi, field_name=col)
            projected_data = self._project_records(res.data, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

        if isinstance(where, (PredicateInPointRadius, PredicateKNN)):
            col = where.column

            if isinstance(where, PredicateInPointRadius):
                res = self.db.range_search(plan.table, list(where.point), where.radius, field_name=col, spatial_type="radius")
            else:
                res = self.db.range_search(plan.table, list(where.point), where.k, field_name=col, spatial_type="knn")

            projected_data = self._project_records(res.data, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

        raise NotImplementedError("Predicado WHERE no soportado")

    # ====== INSERT ======
    def _insert(self, plan: InsertPlan):
        tinfo = self.db.get_table_info(plan.table)
        if not tinfo:
            raise ValueError(f"Tabla {plan.table} no existe")

        table_obj = self.db.tables[plan.table]["table"]
        phys_fields = table_obj.all_fields
        key_field = table_obj.key_field
        names = [n for (n, _, _) in phys_fields]

        rec = Record(phys_fields, key_field)

        if plan.columns is None:
            values = plan.values
            padded = list(values[:len(names)])
            if len(padded) < len(names):
                padded += [None] * (len(names) - len(padded))
            for (name, ftype, _), val in zip(phys_fields, padded):
                v = val
                if v is None:
                    if name == "active" and self.db.tables[plan.table]["primary_type"] == "SEQUENTIAL":
                        v = True
                    else:
                        v = self._defaults_for_field(ftype)
                if name == "fecha" and isinstance(v, str):
                    v = self._cast_date_ddmmyyyy_to_iso(v)
                if ftype == "INT" and v is not None:
                    v = int(v)
                elif ftype == "FLOAT" and v is not None:
                    v = float(v)
                elif ftype == "ARRAY" and v is not None:
                    if isinstance(v, (list, tuple)):
                        v = tuple(float(x) for x in v)
                rec.set_field_value(name, v)
        else:
            for (name, ftype, _) in phys_fields:
                rec.set_field_value(name, self._defaults_for_field(ftype))
            for c, v in zip(plan.columns, plan.values):
                idx = names.index(c)
                _, ftype, _ = phys_fields[idx]
                vv = v
                if c == "fecha" and isinstance(vv, str):
                    vv = self._cast_date_ddmmyyyy_to_iso(vv)
                if ftype == "INT" and vv is not None:
                    vv = int(vv)
                elif ftype == "FLOAT" and vv is not None:
                    vv = float(vv)
                elif ftype == "ARRAY" and vv is not None:
                    if isinstance(vv, (list, tuple)):
                        vv = tuple(float(x) for x in vv)
                rec.set_field_value(c, vv)

        res = self.db.insert(plan.table, rec)

        success_msg = "OK" if bool(res.data) else "Duplicado/No insertado"
        return OperationResult(success_msg, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

    def _delete(self, plan: DeletePlan):
        tinfo = self.db.get_table_info(plan.table)
        if not tinfo:
            raise ValueError(f"Tabla {plan.table} no existe")

        where = plan.where
        if not isinstance(where, (PredicateEq, PredicateBetween)):
            raise NotImplementedError("DELETE soporta = y BETWEEN por ahora")

        if isinstance(where, PredicateEq):
            col = where.column
            val = where.value
            pk_name = self.db.tables[plan.table]["table"].key_field
            res = self.db.delete(plan.table, val, field_name=(None if col == pk_name else col))

            data = res.data
            if isinstance(data, bool):
                deleted = 1 if data else 0
            else:
                try:
                    deleted = int(data)
                except Exception:
                    deleted = 0
            result_msg = f"OK ({deleted} registros)"
            return OperationResult(result_msg, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)
        else:
            col = where.column
            lo = where.lo
            hi = where.hi
            res = self.db.range_delete(plan.table, lo, hi, field_name=col)

            data = res.data
            try:
                deleted = int(data)
            except Exception:
                deleted = 0
            result_msg = f"OK ({deleted} registros)"
            return OperationResult(result_msg, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

    # ====== CREATE INDEX ======
    def _create_index(self, plan: CreateIndexPlan):
        try:
            if not self.db._validate_secondary_index(plan.index_type.upper()):
                return OperationResult(f"ERROR: Tipo de índice '{plan.index_type}' no soportado", 0, 0, 0)

            self.db.create_index(plan.table, plan.column, plan.index_type.upper())
            return OperationResult(f"OK: Índice creado en {plan.table}.{plan.column} usando {plan.index_type.upper()}", 0, 0, 0)
        except Exception as e:
            return OperationResult(f"ERROR: {e}", 0, 0, 0)

    # ====== DROP TABLE ======
    def _drop_table(self, plan: DropTablePlan):
        try:
            if plan.table not in self.db.tables:
                return OperationResult(f"ERROR: Tabla '{plan.table}' no existe", 0, 0, 0)

            removed_files = self.db.drop_table(plan.table)
            files_info = f" (archivos eliminados: {len(removed_files)})" if removed_files else ""

            return OperationResult(f"OK: Tabla '{plan.table}' eliminada{files_info}", 0, 0, 0)
        except Exception as e:
            return OperationResult(f"ERROR: {e}", 0, 0, 0)

    # ====== DROP INDEX ======
    def _drop_index(self, plan: DropIndexPlan):
        try:
            field_name = plan.index_name.replace("idx_", "") if plan.index_name.startswith("idx_") else plan.index_name

            for table_name, table_data in self.db.tables.items():
                if "secondary_indexes" in table_data and field_name in table_data["secondary_indexes"]:
                    removed_files = self.db.drop_index(table_name, field_name)
                    files_info = f" (archivos eliminados: {len(removed_files)})" if removed_files else ""
                    return OperationResult(f"OK: Índice eliminado en campo '{field_name}'{files_info}", 0, 0, 0)

            return OperationResult(f"ERROR: Índice '{plan.index_name}' no encontrado", 0, 0, 0)
        except Exception as e:
            return OperationResult(f"ERROR: {e}", 0, 0, 0)

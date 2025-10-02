import csv
from typing import Dict, Any, List, Tuple, Optional
from .plan_types import (
    CreateTablePlan, LoadFromCSVPlan, SelectPlan, InsertPlan, DeletePlan,
    CreateIndexPlan, DropTablePlan, DropIndexPlan,
    ColumnDef, ColumnType, PredicateEq, PredicateBetween, PredicateInPointRadius, PredicateKNN
)
from indexes.core.record import Table, Record
from indexes.core.performance_tracker import OperationResult


class Executor:
    def __init__(self, db_manager):
        self.db = db_manager

    def execute(self, plan):
        if isinstance(plan, CreateTablePlan):
            return self._create_table(plan)
        elif isinstance(plan, LoadFromCSVPlan):
            return self._create_from_file(plan)
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
        if kind == "ARRAY_FLOAT":
            return None  # no soportado por record.py
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
        allowed_primary = {"ISAM", "SEQUENTIAL"}
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
                    self.db.create_index(plan.table, colname, idx_kind, scan_existing=False)
                    created_any = True
                else:
                    unsupported.append(f"{colname}:{idx_kind}")
            except NotImplementedError as e:
                unsupported.append(f"{colname}:{idx_kind}(NotImpl)")
            except Exception as e:
                unsupported.append(f"{colname}:{idx_kind}({str(e)[:30]})")

        msg_parts = [f"OK: tabla {plan.table} creada (primario={primary_index_type}, key={pk_field})"]
        if ignored_cols:
            msg_parts.append(f"— Columnas no soportadas (ignoradas): {', '.join(ignored_cols)}")
        if unsupported or (secondary_decls and not created_any):
            if not unsupported:  # declarados pero ninguno pudo crearse
                unsupported = [f"{c}:{k}" for c, k in secondary_decls]
            msg_parts.append(f"— Índices secundarios no soportados: {', '.join(unsupported)}")
        result_msg = " ".join(msg_parts)
        return OperationResult(result_msg, 0, 0, 0)  # CREATE TABLE doesn't involve significant disk I/O in our model

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
        # convierte "24/10/2024" -> "2024-10-24"; si ya está en "YYYY-MM-DD" lo deja
        s = (s or "").strip()
        if not s:
            return ""
        if "-" in s and len(s) == 10:
            # asume YYYY-MM-DD
            return s
        # soportar DD/MM/YYYY (con o sin cero a la izquierda)
        parts = s.split("/")
        if len(parts) == 3:
            dd, mm, yyyy = parts
            dd = dd.zfill(2)
            mm = mm.zfill(2)
            return f"{yyyy}-{mm}-{dd}"
        return s  # fallback sin romper

    def _guess_delimiter(self, header_line: str) -> str:
        # simple heurística ; o ,  (prioriza ';' si aparece)
        return ";" if header_line.count(";") >= header_line.count(",") else ","

    def _spanish_header_mapping(self, headers: List[str]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for h in headers:
            hl = (h or "").strip().lower()
            # id
            if ("id" in hl) and ("venta" in hl or "id" == hl or hl.startswith("id ")):
                mapping[hl] = "id"
                continue
            # nombre
            if "nombre" in hl:
                mapping[hl] = "nombre"
                continue
            # cantidad
            if "cantidad" in hl:
                mapping[hl] = "cantidad"
                continue
            # precio
            if "precio" in hl:
                mapping[hl] = "precio"
                continue
            # fecha
            if "fecha" in hl:
                mapping[hl] = "fecha"
                continue
        return mapping

    # ====== CSV LOAD ======
    def _create_from_file(self, plan: LoadFromCSVPlan):
        info = self.db.get_table_info(plan.table)
        if not info:
            raise ValueError(f"Tabla {plan.table} no existe; crea la tabla antes de cargar CSV")

        table_obj = self.db.tables[plan.table]["table"]
        phys_fields = table_obj.all_fields
        key_field = table_obj.key_field

        phys_index: Dict[str, Tuple[str, str, int]] = {
            name.lower(): (name, ftype, fsize) for (name, ftype, fsize) in phys_fields
        }

        inserted = duplicates = cast_err = 0

        with open(plan.filepath, "r", encoding="utf-8", newline="") as fh_probe:
            first_line = fh_probe.readline()
            delimiter = self._guess_delimiter(first_line)

        with open(plan.filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if not reader.fieldnames:
                return "CSV cargado: insertados=0, duplicados=0, cast_err=0"

            header_map = self._spanish_header_mapping(reader.fieldnames)

            for row in reader:
                row_lower = {(k or "").strip().lower(): v for k, v in row.items()}
                rec = Record(phys_fields, key_field)
                ok_row = True

                for name_lower, (phys_name, ftype, _fsize) in phys_index.items():
                    try:
                        raw = None
                        if name_lower in row_lower:
                            raw = row_lower[name_lower]
                        else:
                            raw = None
                            for h_sp_lower, phys in header_map.items():
                                if phys == phys_name and h_sp_lower in row_lower:
                                    raw = row_lower[h_sp_lower]
                                    break

                        if phys_name == "fecha":
                            raw = self._cast_date_ddmmyyyy_to_iso(str(raw) if raw is not None else "")

                        val = self._cast_value(raw, ftype)
                        rec.set_field_value(phys_name, val)
                    except Exception:
                        ok_row = False
                        break

                if not ok_row:
                    cast_err += 1
                    continue

                try:
                    res = self.db.insert(plan.table, rec)
                    if hasattr(res, "data") and (res.data is False):
                        duplicates += 1
                    else:
                        inserted += 1
                except Exception:
                    cast_err += 1
                    continue

        return f"CSV cargado: insertados={inserted}, duplicados={duplicates}, cast_err={cast_err}"

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
        out = []
        names = [n for (n, _, _) in records[0].value_type_size]
        pick = names if (columns is None) else columns
        for r in records:
            obj = {}
            for c in pick:
                val = getattr(r, c, None)
                if isinstance(val, bytes):
                    val = val.decode("utf-8").rstrip("\x00").strip()
                obj[c] = val
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
            raise NotImplementedError("Predicados espaciales no soportados")

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
                return f"ERROR: Tipo de índice '{plan.index_type}' no soportado para índices secundarios"

            self.db.create_index(plan.table, plan.column, plan.index_type.upper())
            return f"OK: Índice creado en {plan.table}.{plan.column} usando {plan.index_type.upper()}"
        except Exception as e:
            return f"ERROR: {e}"

    # ====== DROP TABLE ======
    def _drop_table(self, plan: DropTablePlan):
        try:
            if plan.table not in self.db.tables:
                return f"ERROR: Tabla '{plan.table}' no existe"

            removed_files = self.db.drop_table(plan.table)
            files_info = f" (archivos eliminados: {len(removed_files)})" if removed_files else ""

            return f"OK: Tabla '{plan.table}' eliminada{files_info}"
        except Exception as e:
            return f"ERROR: {e}"

    # ====== DROP INDEX ======
    def _drop_index(self, plan: DropIndexPlan):
        try:
            field_name = plan.index_name.replace("idx_", "") if plan.index_name.startswith("idx_") else plan.index_name

            for table_name, table_data in self.db.tables.items():
                if "secondary_indexes" in table_data and field_name in table_data["secondary_indexes"]:
                    removed_files = self.db.drop_index(table_name, field_name)
                    files_info = f" (archivos eliminados: {len(removed_files)})" if removed_files else ""
                    return f"OK: Índice eliminado en campo '{field_name}'{files_info}"

            return f"ERROR: Índice '{plan.index_name}' no encontrado"
        except Exception as e:
            return f"ERROR: {e}"
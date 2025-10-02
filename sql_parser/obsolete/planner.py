#Esto es para el PLAN DE EJECUCIÓN
from typing import Optional, Dict
from .plan_types import (
    SelectPlan, PredicateEq, PredicateBetween, ExplainPlan
)

class Catalog:
    def __init__(self):
        self.tables = {}  # name -> dict(rows, pages, width, indexes={col: kind}, key='id')
    def register_table(self, name, rows, pages, width, indexes):
        # indexes: dict(col -> "ISAM"|"BTREE"|"HASH"|...)
        self.tables[name] = {
            "rows": rows, "pages": pages, "width": width,
            "indexes": indexes
        }
    def get_indexes(self, table) -> Dict[str, str]:
        t = self.tables.get(table, {})
        return t.get("indexes", {})

class PlanNode:
    def __init__(self, kind: str, table: str, index_col: Optional[str]=None, index_kind: Optional[str]=None, op: Optional[str]=None):
        self.kind = kind              # "IndexScan" | "SeqScan"
        self.table = table
        self.index_col = index_col    # columna de índice
        self.index_kind = index_kind  # ISAM/BTREE/HASH
        self.op = op                  # "==", "BETWEEN"

def plan_select(sel: SelectPlan, cat: Catalog) -> PlanNode:
    idx = cat.get_indexes(sel.table) if cat else {}
    # Sin WHERE => SeqScan
    if sel.where is None:
        return PlanNode("SeqScan", table=sel.table, op=None)

    w = sel.where

    # Igualdad
    if isinstance(w, PredicateEq):
        col = w.column
        if col in idx:
            kind = idx[col]
            if kind == "ISAM":
                return PlanNode("IndexScan", sel.table, col, "ISAM", op="==")
            if kind == "BTREE":
                return PlanNode("IndexScan", sel.table, col, "BTREE", op="==")
            if kind == "HASH":
                # Hash solo para igualdad
                return PlanNode("IndexScan", sel.table, col, "HASH", op="==")
        # si no hay índice para esa col:
        return PlanNode("SeqScan", sel.table, op="equality")

    # BETWEEN
    if isinstance(w, PredicateBetween):
        col = w.column
        if col in idx and idx[col] == "BTREE":
            return PlanNode("IndexScan", sel.table, col, "BTREE", op="BETWEEN")
        # Hash no sirve para rango
        return PlanNode("SeqScan", sel.table, op="BETWEEN")
    return PlanNode("SeqScan", sel.table, op="filter")

def format_plan(p: PlanNode) -> str:
    if p.kind == "SeqScan":
        if p.op:
            return f"Seq Scan\n  -> filter: {p.op}"
        return "Seq Scan"
    if p.kind == "IndexScan":
        extra = " (equality only)" if p.index_kind == "HASH" and p.op == "==" else ""
        return f"Index Scan using {p.index_kind}{extra}\n  -> index_col={p.index_col}, op={p.op}"
    return f"{p.kind}"

def physical_explain(db_manager, sel_plan) -> str:
    table = sel_plan.table
    where = sel_plan.where

    def _pk_name(db, table: str) -> str:
        return db.tables[table]["table"].key_field

    def _primary_type(db, table: str) -> str:
        info = db.get_table_info(table) or {}
        return str(info.get("primary_type", "ISAM")).upper()

    def _has_secondary(db, table: str, col: str) -> bool:
        sec = db.tables.get(table, {}).get("secondary_indexes", {}) or {}
        return col in sec and sec[col] is not None

    pk = _pk_name(db_manager, table)
    pk_typ = _primary_type(db_manager, table)

    if where is None:
        primary = db_manager.tables[table]["primary_index"]
        if hasattr(primary, "scan_all"):
            return f"Index Full Scan using {pk_typ}\n  -> full scan (scan_all)"
        return "Seq Scan\n  -> full table"

    if isinstance(where, PredicateEq):
        col = where.column or pk
        if col == pk:
            return f"Index Scan using {pk_typ}\n  -> index_col={pk}, op=== "
        if _has_secondary(db_manager, table, col):
            return f"Index Scan using {col}\n  -> index_col={col}, op=== "
        return "Seq Scan\n  -> filter: equality"

    if isinstance(where, PredicateBetween):
        col = where.column or pk
        if col == pk:
            return f"Index Range Scan using {pk_typ}\n  -> index_col={pk}, op=BETWEEN"
        if _has_secondary(db_manager, table, col):
            return f"Index Range Scan using {col}\n  -> index_col={col}, op=BETWEEN"
        return "Seq Scan\n  -> filter: BETWEEN"

    return "Seq Scan\n  -> filter: (predicado no soportado físicamente)"

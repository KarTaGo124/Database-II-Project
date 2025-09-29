#Esto es para el PLAN DE EJECUCIÓN
from typing import Optional, Dict
from .planes import (
    SelectPlan, PredicateEq, PredicateBetween, ExplainPlan
)

class Catalog:
    def __init__(self):
        self.tables = {}  # name -> dict(rows, pages, width, indexes={col: kind}, key='id')
    def register_table(self, name, rows, pages, width, indexes):
        # indexes: dict(col -> "ISAM"|"BTREE"|"EXTENDIBLE"|...)
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
        self.index_kind = index_kind  # ISAM/BTREE/EXTENDIBLE
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
            if kind == "EXTENDIBLE":
                # Hash solo para igualdad
                return PlanNode("IndexScan", sel.table, col, "EXTENDIBLE", op="==")
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
        extra = " (equality only)" if p.index_kind == "EXTENDIBLE" and p.op == "==" else ""
        return f"Index Scan using {p.index_kind}{extra}\n  -> index_col={p.index_col}, op={p.op}"
    return f"{p.kind}"

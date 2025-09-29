from .plan_types import (
    ColumnType, ColumnDef,
    CreateTablePlan, LoadFromCSVPlan,
    SelectPlan, InsertPlan, DeletePlan,
    CreateIndexPlan, DropTablePlan, DropIndexPlan,
    PredicateEq, PredicateBetween, PredicateInPointRadius, PredicateKNN,
)

from lark import Lark, Transformer, Token
from typing import Any, List

# Cargar gramática desde archivo grammar.lark
with open(__file__.replace("parser.py", "grammar.lark"), "r", encoding="utf-8") as f:
    _GRAMMAR = f.read()

_PARSER = Lark(_GRAMMAR, start="start", parser="lalr")


# helpers
def _to_int_or_float(s: str) -> Any:
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return float(s)

def _tok2str(x) -> str:
    if isinstance(x, Token):
        return x.value
    return str(x)


class _T(Transformer):
    # ==== TIPOS ====
    def t_int(self, _):      return ColumnType("INT")
    def t_float(self, _):    return ColumnType("FLOAT")
    def t_date(self, _):     return ColumnType("DATE")
    def t_varchar(self, it): return ColumnType("VARCHAR", int(_tok2str(it[0])))
    def t_array(self, _):    return ColumnType("ARRAY_FLOAT")

    # ==== LITERALES / BÁSICOS ====
    def int_lit(self, items):
        tok = items[0]
        return int(tok.value if isinstance(tok, Token) else str(tok))

    def float_lit(self, items):
        tok = items[0]
        return float(tok.value if isinstance(tok, Token) else str(tok))

    def number(self, items):
        tok = items[0]
        s = tok.value if isinstance(tok, Token) else str(tok)
        return _to_int_or_float(s)

    def string(self, items):
        s = items[0]
        if isinstance(s, Token):
            # para quitar comillas de ESCAPED_STRING
            return s.value[1:-1]
        return str(s)

    def literal(self, items):
        return items[0]

    def null(self, _): return None

    def ident_or_string(self, items):
        x = items[0]
        if isinstance(x, Token):
            if x.type == "IDENT":
                return x.value
            return x.value[1:-1]
        return str(x)

    # ==== LISTAS ====
    def col_list(self, items):
        return [str(x) for x in items]

    # ==== CREATE TABLE ====
    def coldef(self, items):
        name = _tok2str(items[0])
        coltype = items[1]
        is_key = False
        index = None
        VALID = {"SEQ", "ISAM", "BTREE", "RTREE", "HASH"}
        for it in items[2:]:
            if it == "KEY":
                is_key = True
            elif it is None:
                continue
            else:
                s = _tok2str(it)
                if s in VALID:
                    index = s
        return ColumnDef(name=name, type=coltype, is_key=is_key, index=index)

    def index_kind(self, items):  # INDEX_KIND -> str
        return str(items[0])

    def create_table(self, items):
        table = _tok2str(items[0])
        columns = items[1:]
        return CreateTablePlan(table=table, columns=columns)

    # ==== CREATE FROM FILE ====
    def create_from_file(self, items):
        table = _tok2str(items[0])
        filepath = self.ident_or_string([items[1]])  # asegura string limpio
        idx_kind = str(items[2])

        cols: List[str] = []
        for it in items[3:]:
            if isinstance(it, list):
                cols.extend([_tok2str(x) for x in it])
            else:
                cols.append(_tok2str(it))
        if not cols:
            cols = None

        return LoadFromCSVPlan(table=table, filepath=filepath, index_kind=idx_kind, index_cols=cols)

    # ==== SELECT ====
    def select_all(self, _): return None
    def select_cols(self, items):
        cols = items[0]
        return cols if isinstance(cols, list) else [str(cols)]

    # punto (x,y)
    def point(self, items):
        x = float(items[0])
        y = float(items[1])
        return (x, y)

    def pred_eq(self, items):
        return PredicateEq(column=str(items[0]), value=items[1])

    def pred_between(self, items):
        return PredicateBetween(column=str(items[0]), lo=items[1], hi=items[2])

    def pred_in(self, items):
        col = str(items[0])
        pt = items[1]
        radius = float(items[2])
        return PredicateInPointRadius(column=col, point=pt, radius=radius)

    def pred_nearest(self, items):
        col = str(items[0])
        pt = items[1]            # (x, y)
        k = int(items[2])
        return PredicateKNN(column=col, point=pt, k=k)

    def select_stmt(self, items):
        cols_or_none = items[0]
        table = _tok2str(items[1])
        where = items[2] if len(items) > 2 else None
        return SelectPlan(table=table, columns=cols_or_none, where=where)


    # ==== INSERT ====
    def insert_stmt(self, items):
        table = _tok2str(items[0])
        rest = [x for x in items[1:] if x is not None]
        if rest and isinstance(rest[0], list):   # con lista de columnas
            cols = rest[0]
            vals = rest[1:]
        else:                                     # sin lista de columnas
            cols = None
            vals = rest

        return InsertPlan(table=table, columns=cols, values=vals)


    # ==== DELETE ====
    def delete_stmt(self, items):
        table = _tok2str(items[0])
        where = items[1] if len(items) > 1 else None
        return DeletePlan(table=table, where=where)

    # ==== CREATE INDEX ====
    def create_index(self, items):
        table = _tok2str(items[0])
        column = _tok2str(items[1])
        index_type = _tok2str(items[2])
        return CreateIndexPlan(index_name=column, table=table, column=column, index_type=index_type)

    # ==== DROP TABLE ====
    def drop_table(self, items):
        table = _tok2str(items[0])
        return DropTablePlan(table=table)

    # ==== DROP INDEX ====
    def drop_index(self, items):
        index_name = _tok2str(items[0])
        return DropIndexPlan(index_name=index_name)

    def start(self, items):
        return items

_TRANSFORMER = _T()

def parse(sql: str):
    sql = sql.strip().rstrip(";")
    tree = _PARSER.parse(sql)
    res = _TRANSFORMER.transform(tree)
    return res if isinstance(res, list) else [res]

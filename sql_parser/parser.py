from .plan_types import (
    ColumnType, ColumnDef,
    CreateTablePlan, LoadDataPlan,
    SelectPlan, InsertPlan, DeletePlan,
    CreateIndexPlan, DropTablePlan, DropIndexPlan,
    PredicateEq, PredicateBetween, PredicateInPointRadius, PredicateKNN,
)

from lark import Lark, Transformer, Token, Tree
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
    def t_array_2d(self, _): return ColumnType("ARRAY", 2)
    def t_array_nd(self, it): return ColumnType("ARRAY", int(_tok2str(it[0])))

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
            # quitar comillas de ESCAPED_STRING
            return s.value[1:-1]
        return str(s)

    def literal(self, items):
        return items[0]

    def null(self, _): return None

    def spatial_point(self, items):
        # por si tu gramática usa esta regla en algún lugar
        return tuple(items)

    def array_lit(self, items):
        # asume que subreglas ya devuelven la lista
        return items[0]

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
        VALID = {"SEQUENTIAL", "ISAM", "BTREE", "RTREE", "HASH"}
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

    # ==== LOAD DATA FROM FILE ====
    def load_data(self, items):
        # Formatos soportados:
        # LOAD DATA FROM FILE "path.csv" INTO ventas
        # LOAD DATA FROM FILE "path.csv" INTO ventas MAP ( campo:["x","y"], nombre:"Nombre", precio:"Precio" )
        filepath = self.ident_or_string([items[0]])
        table = _tok2str(items[1])

        mappings = None
        if len(items) > 2:
            tmp = {}
            # items[2:] pueden ser tuplas (campo, lista/str) o a veces Trees/listas según gramática
            for mapping in items[2:]:
                if mapping is None:
                    continue
                # normaliza mapeo
                k, v = self._normalize_pair(mapping)
                if k is None:
                    continue
                tmp[k] = v
            mappings = tmp or None

        return LoadDataPlan(table=table, filepath=filepath, column_mappings=mappings)

    def column_mapping(self, items):
        # Garantiza salida: (str, list[str]) o (str, str) -> la capa superior normaliza a list si corresponde
        array_field = _tok2str(items[0])
        if len(items) == 2 and isinstance(items[1], list):
            csv_columns = [_tok2str(it) for it in items[1]]
        else:
            csv_columns = [_tok2str(it) for it in items[1:]]
        return (array_field, csv_columns)

    # ==== SELECT ====
    def select_all(self, _): return None

    def select_cols(self, items):
        cols = items[0]
        return cols if isinstance(cols, list) else [str(cols)]

    def point(self, items):
        # tu versión devolvía None; corregido para devolver (x, y)
        return tuple(float(item) for item in items)

    def pred_eq(self, items):
        return PredicateEq(column=str(items[0]), value=items[1])

    def pred_between(self, items):
        return PredicateBetween(column=str(items[0]), lo=items[1], hi=items[2])

    def pred_in(self, items):
        col = str(items[0])
        pt = items[1]          # (x, y)
        radius = float(items[2])
        return PredicateInPointRadius(column=col, point=pt, radius=radius)

    def pred_nearest(self, items):
        col = str(items[0])
        pt = items[1]          # (x, y)
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
        else:                                    # sin lista de columnas
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

    # ==== helpers internos para MAP(...) ====
    def _normalize_pair(self, node):
        """
        Devuelve (clave:str, valor:list[str] o str) o (None, None) si no se puede normalizar.
        Acepta:
          - ('coords', ['x','y'])
          - ('nombre', ['Nombre'])
          - ('nombre', 'Nombre')
          - listas/árboles equivalentes de Lark
        """
        # tuple/list de longitud 2
        if isinstance(node, (list, tuple)) and len(node) == 2:
            k = _tok2str(node[0])
            v = node[1]
            if isinstance(v, (list, tuple)):
                v2 = [_tok2str(x) for x in v]
            else:
                v2 = _tok2str(v)
            return k, v2

        # dict accidental
        if isinstance(node, dict):
            # tomar primer item
            for k, v in node.items():
                if isinstance(v, (list, tuple)):
                    v2 = [_tok2str(x) for x in v]
                else:
                    v2 = _tok2str(v)
                return _tok2str(k), v2
            return None, None

        # Tree -> intentar con hijos
        if isinstance(node, Tree) and getattr(node, "children", None):
            return self._normalize_pair(node.children)

        # Token/cadena suelta no válida
        return None, None


_TRANSFORMER = _T()

def parse(sql: str):
    sql = sql.strip().rstrip(";")
    tree = _PARSER.parse(sql)
    res = _TRANSFORMER.transform(tree)
    return res if isinstance(res, list) else [res]

from planes import (
    ColumnType, ColumnDef, CreateTablePlan, LoadFromCSVPlan,
    PredicateEq, PredicateBetween, PredicateSpatialIn, PredicateKNN,
    SelectPlan, InsertPlan, DeletePlan
)
from lark import Lark, Transformer, v_args
import os

# cargamos la gramática a partir de grammar.lark

with open("./sql_parser/grammar.lark") as f:
    sql_grammar = f.read()

parser = Lark(sql_grammar, start="start", parser="lalr")

# TO DO...
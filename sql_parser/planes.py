from dataclasses import dataclass
from typing import List, Optional, Tuple, Any

# Tipos/Columnas

@dataclass
class ColumnType:
    kind: str                 # "INT" | "FLOAT" | "DATE" | "VARCHAR" | "ARRAY_FLOAT"
    length: Optional[int] = None

@dataclass
class ColumnDef:
    name: str
    type: ColumnType
    is_key: bool = False
    index: Optional[str] = None  # "ISAM" | "BTREE" | "RTREE" | "SEQ" | "EXTENDIBLE"

# Planes

@dataclass
class CreateTablePlan:
    table: str
    columns: List[ColumnDef]

@dataclass
class LoadFromCSVPlan:
    table: str
    filepath: str
    index_kind: str           # por ejemplo: "ISAM"
    index_cols: List[str]     # por ejemplo: ["id"] o ["(id","nombre)"] ya normalizados a ["id","nombre"]

@dataclass
class PredicateEq:
    column: str
    value: Any

@dataclass
class PredicateBetween:
    column: str
    lo: Any
    hi: Any

@dataclass
class PredicateInPointRadius:
    column: str
    point: Tuple[float, float]
    radius: float

@dataclass
class PredicateKNN:
    column: str
    point: Tuple[float, float]
    k: int

@dataclass
class SelectPlan:
    table: str
    columns: Optional[List[str]]
    where: Optional[Any]

@dataclass
class InsertPlan:
    table: str
    columns: Optional[List[str]]
    values: List[Any]

@dataclass
class DeletePlan:
    table: str
    where: Any

@dataclass
class ExplainPlan:
    inner: Any

from dataclasses import dataclass
from typing import List, Optional, Union, Tuple, Any

@dataclass
class ColumnType:
    kind: str                   # INT | FLOAT | DATE | VARCHAR | ARRAY_FLOAT
    length: Optional[int] = None

@dataclass
class ColumnDef:
    name: str
    type: ColumnType
    is_key: bool = False
    index: Optional[str] = None  # SEQ | ISAM | BTREE | RTREE | EXTENDIBLE

@dataclass
class CreateTablePlan:
    table: str
    columns: List[ColumnDef]

@dataclass
class LoadFromCSVPlan:
    table: str
    path: str
    index_kind: str
    index_key: Union[str, Tuple[str, str]]  # 'id' o ('lat','lng')

@dataclass
class PredicateEq:
    column: str
    value: Any

@dataclass
class PredicateBetween:
    column: str
    low: Any
    high: Any

@dataclass
class PredicateSpatialIn:
    column: str
    x: float
    y: float
    radius: Optional[float] = None   # usado en búsquedas por rango espacial (point, radio)

@dataclass
class PredicateKNN:
    column: str
    x: float
    y: float
    k: int                           # número de vecinos más cercanos

# Union de todos los predicados posibles
Where = Union[PredicateEq, PredicateBetween, PredicateSpatialIn, PredicateKNN]

@dataclass
class SelectPlan:
    table: str
    columns: Optional[List[str]]     # None => SELECT *
    where: Optional[Where] = None

@dataclass
class InsertPlan:
    table: str
    values: List[Any]

@dataclass
class DeletePlan:
    table: str
    where: Where

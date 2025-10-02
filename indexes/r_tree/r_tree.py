import math
import os
from rtree import index
from typing import List, Tuple, Optional
from ..core.record import Record
from ..core.performance_tracker import PerformanceTracker, OperationResult

class RTreeSecondaryIndex:
    def __init__(self, field_name: str, primary_index, filename: str, dimension: int = 2):
        self.field_name = field_name
        self.primary_index = primary_index
        self.filename = filename
        self.dimension = dimension
        self.performance = PerformanceTracker()
        
        p = index.Property()
        p.dimension = dimension
        if filename:
            self.idx = index.Index(filename, properties=p)
        else:
            self.idx = index.Index(properties=p)
    
    def insert(self, record: Record) -> OperationResult:
        self.performance.start_operation()
        
        try:
            coords = getattr(record, self.field_name, None)
            if coords is None:
                coords = record.__dict__.get(self.field_name)
            
            if coords is None:
                raise ValueError(f"Campo {self.field_name} no encontrado en el record")
            
            if not isinstance(coords, (list, tuple)):
                raise ValueError(f"Campo {self.field_name} debe ser lista o tupla de coordenadas")
            
            if len(coords) != self.dimension:
                raise ValueError(f"Campo {self.field_name} debe tener {self.dimension} dimensiones")
            
            coords = list(coords)
            primary_key = record.get_key()
            
            bbox = tuple(coords + coords)
            self.idx.insert(primary_key, bbox)
            
            self.performance.track_write()
            return self.performance.end_operation(True)
            
        except Exception as e:
            print(f"ERROR AL INSERTAR EN RTREE: {e}")
            return self.performance.end_operation(False)
    
    def search(self, value) -> OperationResult:
        self.performance.start_operation()
        
        try:
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"Valor de búsqueda debe ser lista o tupla de coordenadas")
            
            if len(value) != self.dimension:
                raise ValueError(f"Valor de búsqueda debe tener {self.dimension} dimensiones")
            
            bbox = tuple(list(value) + list(value))
            candidate_ids = list(self.idx.intersection(bbox))
            
            self.performance.track_read()
            return self.performance.end_operation(candidate_ids)
            
        except Exception as e:
            print(f"ERROR AL BUSCAR EN RTREE: {e}")
            return self.performance.end_operation([])
    
    def range_search(self, start_key, end_key) -> OperationResult:
        raise NotImplementedError(
            "Range search is not supported for R-Tree spatial indexes. "
            "R-Tree is optimized for spatial queries. "
            "Use radius_search(center, radius) or knn_search(coords, k) instead."
        )
    
    def knn_search(self, coords: List[float], k: int) -> OperationResult:
        self.performance.start_operation()
        
        try:
            if not isinstance(coords, (list, tuple)) or len(coords) != self.dimension:
                raise ValueError(f"Coordenadas deben tener {self.dimension} dimensiones")
            
            if k <= 0:
                raise ValueError("k debe ser mayor que 0")
            
            bbox = tuple(list(coords) + list(coords))
            nearest_pks = list(self.idx.nearest(bbox, k * 2))
            
            self.performance.track_read()
            return self.performance.end_operation(nearest_pks)
            
        except Exception as e:
            print(f"ERROR EN KNN SEARCH: {e}")
            return self.performance.end_operation([])
    
    def radius_search(self, center: List[float], radius: float) -> OperationResult:
        self.performance.start_operation()
        
        try:
            if not isinstance(center, (list, tuple)) or len(center) != self.dimension:
                raise ValueError(f"Centro debe tener {self.dimension} dimensiones")
            
            if radius < 0:
                raise ValueError("Radio debe ser mayor o igual a 0")
            
            min_coords = [c - radius for c in center]
            max_coords = [c + radius for c in center]
            bbox = tuple(min_coords + max_coords)
            
            candidate_pks = list(self.idx.intersection(bbox))
            
            self.performance.track_read()
            return self.performance.end_operation(candidate_pks)
            
        except Exception as e:
            print(f"ERROR EN RADIUS SEARCH: {e}")
            return self.performance.end_operation([])
    
    def delete(self, record: Record) -> OperationResult:
        self.performance.start_operation()
        
        try:
            primary_key = record.get_key()
            
            coords = getattr(record, self.field_name, None)
            if coords is None:
                raise ValueError(f"No se pudo obtener el campo {self.field_name} del record")
            
            if not isinstance(coords, (list, tuple)) or len(coords) != self.dimension:
                raise ValueError(f"Coordenadas deben tener {self.dimension} dimensiones")
            
            bbox = tuple(list(coords) + list(coords))
            self.idx.delete(primary_key, bbox)
            
            self.performance.track_write()
            return self.performance.end_operation(True)
            
        except Exception as e:
            print(f"ERROR AL ELIMINAR EN RTREE: {e}")
            return self.performance.end_operation(False)
    
    def _euclidean_distance(self, p1: List[float], p2: List[float]) -> float:
        if len(p1) != len(p2):
            raise ValueError("Puntos deben tener la misma dimensión")
        return math.sqrt(sum((p1[i] - p2[i]) ** 2 for i in range(len(p1))))
    
    def drop_index(self):
        removed_files = []
        
        try:
            self.idx.close()
            
            for ext in ['.dat', '.idx']:
                filepath = f"{self.filename}{ext}"
                if os.path.exists(filepath):
                    os.remove(filepath)
                    removed_files.append(filepath)
        except Exception as e:
            print(f"ERROR AL ELIMINAR ARCHIVOS DEL ÍNDICE R-Tree: {e}")
        
        return removed_files
    
    def close(self):
        try:
            self.idx.close()
        except Exception as e:
            print(f"ERROR AL CERRAR EL ÍNDICE R-Tree: {e}")
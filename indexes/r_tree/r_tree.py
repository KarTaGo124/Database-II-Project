# R Tree Index
import math
import pickle
import os
from rtree import index
from typing import List, Tuple, Any, Optional



class RTree:
    def __init__(self, dimension: int = 2, filename: str = None):
    #pq no pongo? m ni M, esto se debe a que la libreria de rtree ya usa valores por defecto que estan optimizados para la mayoria de los casos
        self.dimension = dimension
        self.filename = filename

        p = index.Property() #estocrea el objeto 
        p.dimension = dimension 
        if filename:
            self.idx = index.Index(filename, properties=p)
            self.metadata_file = f"{filename}_metadata.pkl"
        else:
            self.idx = index.Index(properties=p)
            self.metadata_file = None

        self.records = {}
        
        if self.metadata_file and os.path.exists(self.metadata_file):
            self.load_metadata() # basicamente carga el archivo si existe para no perder los datos
    
    def count(self) -> int:
        return len(self.records)
    
    def get_all_recs(self) -> List[dict]:
        return list(self.records.values())
    
    def load_metadata(self):
        pass

    def save_metadata(self):
        pass

    def search(self, id_record: int) -> Optional[dict]: #optional porque puede que no lo encuentre
        pass

    #def rangeSearch ( con el point, radio)

    #def rangeSearch (con el point, k)

    #def rangeSearch (begin-key, end-key)

    def add():
        pass

    def remove():
        pass

    def distancia_euclidiana(self, punto1: Tuple[float], punto2: Tuple[float]) -> float:
        if len(punto1) != len(punto2):
            raise ValueError("ERROR: LOS PUNTOS NO TIENEN LA MISMA DIMENSION")
        suma = 0
        for i in range(len(punto1)):
            suma += (punto1[i] - punto2[i]) ** 2
        return math.sqrt(suma)

    def punto_a_rectangulo():
        pass

    def punto_con_radio():
        pass


    

        
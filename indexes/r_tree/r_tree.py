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
            self._load_metadata() # basicamente carga el archivo si existe para no perder los datos
        


        
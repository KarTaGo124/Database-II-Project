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
        try:
            with open(self.metadata_file, 'rb') as f:
                self.records = pickle.load(f) # carga el diccionario
        except Exception as e:
            print(f"ERROR AL CARGAR METADATA: {e}")

    def save_metadata(self):
        try: 
            with open(self.metadata_file, 'wb') as f:
                pickle.dump(self.records, f) # guarda el diccionario de records (literalmente asi se llama el metodo)
        except Exception as e:
            print(f"ERROR AL GUARDAR METADATA: {e}")

    def search(self, id_record: int) -> Optional[dict]: #optional porque puede que no lo encuentre
        return self.records.get(id_record)

    #def rangeSearch ( con el point, radio)
    def rangeSearch(self, punto: List[float], radio: float) -> List[dict]:
        if radio<0:
            raise ValueError("EL RADIO DEBE SER MAYOR O IGUAL A 0")
        min_c = []
        max_c =[]
        for coordeada in punto:
            min_c.append(coordeada - radio)
            max_c.append(coordeada + radio)
        caja = tuple(min_c + max_c)
        id_vecinos = list(self.idx.intersection(caja))
        vecinos_dentro_del_radio = []
        #es importantisimo esto, aqui filtro los que estan dentro del radio, pq la caja puede traer puntos que no estan en el radio
        for id_record in id_vecinos:
            record = self.records.get(id_record)
            if record:
                dist = self.distancia_euclidiana(tuple(punto), tuple(record['punto']))
                if dist <= radio:
                    resultado = record.copy()
                    resultado['distancia'] = dist
                    vecinos_dentro_del_radio.append(resultado)

        return vecinos_dentro_del_radio

    #def rangeSearch (con el point, k) le voy a poner KNN pq no se como diferenciar si es con radio o con k
    def KNN(self, punto: List[float], k: int) -> List[dict]:
        if k<=0:
            raise ValueError("K DEBE SER MAYOR A 0")
        caja = self.punto_a_rectangulo(punto)
        id_k_vecinos = list(self.idx.nearest(caja, k))
        k_vecinos = []
        for id_record in id_k_vecinos:
            record = self.records.get(id_record)
            if record:
                dist = self.distancia_euclidiana(tuple(punto), tuple(record['punto']))
                resultado = record.copy()
                resultado['distancia'] = dist
                k_vecinos.append(resultado)

        k_vecinos.sort(key=lambda x: x['distancia'])
        return k_vecinos[:k]


    #def rangeSearch (begin-key, end-key) va?????  no tiene sentido en rtree 

    def add(self, id_record: int, punto: List[float], record) -> bool:
        try: 
            caja = self.punto_a_rectangulo(punto)
            self.idx.insert(id_record, caja) 
            self.records[id_record] = { 'id': id_record, 'punto': punto, 'record': record}
            if self.metadata_file:
                self.save_metadata()
            return True
        except Exception as e:
            print(f"ERROR AL AÑADIR: {e}")
            return False

    def remove(self, id_record: int) -> bool:
        record = self.records.get(id_record)
        if not record:
            print("ERROR: EL ID NO EXISTE")
            return False
        try:
            caja = self.punto_a_rectangulo(record['punto'])
            self.idx.delete(id_record, caja)
            del self.records[id_record]

            if self.metadata_file:
                self.save_metadata()
            return True
        except Exception as e:
            print(f"ERROR AL ELIMINAR: {e}")
            return False

    def distancia_euclidiana(self, punto1: Tuple[float], punto2: Tuple[float]) -> float:
        if len(punto1) != len(punto2):
            raise ValueError("ERROR: LOS PUNTOS NO TIENEN LA MISMA DIMENSION")
        suma = 0
        for i in range(len(punto1)):
            suma += (punto1[i] - punto2[i]) ** 2
        return math.sqrt(suma)

    def punto_a_rectangulo(self, punto: List[float]) -> Tuple:
        # el rtree (la libreria) piensa con cajas, no con puntos por ejemplo el punto (3,4) en realidad es la caja (3,4,3,4) y asi
        if len(punto) != self.dimension:
            raise ValueError(f"El punto debe tener {self.dimension} dimensiones")
        return tuple(punto + punto)

    def close(self):
        if self.metadata_file:
            self.save_metadata()
        self.idx.close()
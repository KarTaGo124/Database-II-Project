import math
import pickle
import os
from rtree import index
from typing import List, Tuple, Any, Optional

class RTree:
    def __init__(self, dimension: int = 2, filename: str = None):
        self.dimension = dimension
        self.filename = filename

        p = index.Property()
        p.dimension = dimension 
        if filename:
            self.idx = index.Index(filename, properties=p)
            self.metadata_file = f"{filename}_metadata.pkl"
        else:
            self.idx = index.Index(properties=p)
            self.metadata_file = None
    
    def _load_all_metadata(self) -> dict:
        if self.metadata_file and os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"ERROR AL CARGAR METADATA: {e}")
                return {}
        return {}
    
    def _save_all_metadata(self, records: dict):
        if self.metadata_file:
            try:
                with open(self.metadata_file, 'wb') as f:
                    pickle.dump(records, f)
            except Exception as e:
                print(f"ERROR AL GUARDAR METADATA: {e}")
    
    def count(self) -> int:
        records = self._load_all_metadata()
        return len(records)
    
    def get_all_recs(self) -> List[dict]:
        records = self._load_all_metadata()
        return list(records.values())
    
    def search(self, id_record: int) -> Optional[dict]:
        records = self._load_all_metadata()
        return records.get(id_record)

    def rangeSearch(self, punto: List[float], radio: float) -> List[dict]:
        if radio < 0:
            raise ValueError("EL RADIO DEBE SER MAYOR O IGUAL A 0")
        
        min_c = []
        max_c = []
        for coordeada in punto:
            min_c.append(coordeada - radio)
            max_c.append(coordeada + radio)
        
        caja = tuple(min_c + max_c)
        id_vecinos = list(self.idx.intersection(caja))
        vecinos_dentro_del_radio = []
        
        records = self._load_all_metadata()
        
        for id_record in id_vecinos:
            record = records.get(id_record)
            if record:
                dist = self.distancia_euclidiana(tuple(punto), tuple(record['punto']))
                if dist <= radio:
                    resultado = record.copy()
                    resultado['distancia'] = dist
                    vecinos_dentro_del_radio.append(resultado)

        return vecinos_dentro_del_radio

    def KNN(self, punto: List[float], k: int) -> List[dict]:
        if k <= 0:
            raise ValueError("K DEBE SER MAYOR A 0")
        
        caja = self.punto_a_rectangulo(punto)
        id_k_vecinos = list(self.idx.nearest(caja, k))
        k_vecinos = []
        
        records = self._load_all_metadata()
        
        for id_record in id_k_vecinos:
            record = records.get(id_record)
            if record:
                dist = self.distancia_euclidiana(tuple(punto), tuple(record['punto']))
                resultado = record.copy()
                resultado['distancia'] = dist
                k_vecinos.append(resultado)

        k_vecinos.sort(key=lambda x: x['distancia'])
        return k_vecinos[:k]

    def insert(self, id_record: int, punto: List[float], record) -> bool:
        try: 
            caja = self.punto_a_rectangulo(punto)
            self.idx.insert(id_record, caja)
            
            records = self._load_all_metadata()
            records[id_record] = {'id': id_record, 'punto': punto, 'record': record}
            self._save_all_metadata(records)
            
            return True
        except Exception as e:
            print(f"ERROR AL AÑADIR: {e}")
            return False

    def delete(self, id_record: int) -> bool:
        records = self._load_all_metadata()
        record = records.get(id_record)
        
        if not record:
            print("ERROR: EL ID NO EXISTE")
            return False
        
        try:
            caja = self.punto_a_rectangulo(record['punto'])
            self.idx.delete(id_record, caja)
            del records[id_record]
            self._save_all_metadata(records)
            
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
        if len(punto) != self.dimension:
            raise ValueError(f"El punto debe tener {self.dimension} dimensiones")
        return tuple(punto + punto)

    def close(self):
        self.idx.close()
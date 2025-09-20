import csv
import os
import struct
from typing import List

def making_format(list_of_types: List[List[str, str, int]]) -> str:
    FORMAT = ""
    for element in list_of_types:
        field_type = element[1]
        field_size = element[2]
        if field_type == "int":
            FORMAT += "i"
        elif field_type == "float":
            FORMAT += "f"
        elif field_type == "str":
            FORMAT += f"{field_size}s"
        elif field_type == "char":
            FORMAT += "c"
        elif field_type == "bool":
            FORMAT += "?"
        elif field_type.lower().startswith("array"):
            array_format = calc_array_format(field_type, field_size)
            FORMAT += array_format
        else:
            raise ValueError(f"Tipo no encontrado, verificar valor ingresado: {field_type}")
    return FORMAT

# aqui no se q poner pq no seria estatico
def calc_array_format(field_type: str, field_size: int) -> str:
    if field_type == "array[int]":
        pass
    elif field_type == "array[float]":
        pass
    elif field_type == "array[char]":
        pass
    elif field_type == "array[bool]":
        pass
    elif field_type == "array[str]":
        pass
    else:
        raise ValueError(f"Tipo de array no encontrado, verificar valor ingresado: {field_type}")

def procesar_dato(nombre_tipo_tamano: List[str, str, int]):
    if nombre_tipo_tamano[1] == "int":
        return  nombre_tipo_tamano[0]
    elif nombre_tipo_tamano[1] == "float":
        return  nombre_tipo_tamano[0]
    elif nombre_tipo_tamano[1] == "str":
        return  nombre_tipo_tamano[0].encode('utf-8').ljust(nombre_tipo_tamano[2], b'\x00')
    elif nombre_tipo_tamano[1] == "char":
        return nombre_tipo_tamano[0].encode('utf-8')
    elif nombre_tipo_tamano[1] == "bool":
        return  nombre_tipo_tamano[0]
    elif nombre_tipo_tamano[1].lower().startswith("array"):
        pass #no se que hacer pq no es estatico
    else :
        raise ValueError(f"Tipo no encontrado, verificar valor ingresado: {nombre_tipo_tamano[1]}")
    

class Record:
    FORMAT = ""
    RECORD_SIZE = 0
    key_field = "" # clave para el ordenamiento
    value_type_size = [] # es una tupla de 3 de (valor, tipo, size) para cada campo

    def __init__(self, list_of_types: List[List[str, str, int]], key_fieldd: str):
        self.FORMAT = making_format(list_of_types)
        self.RECORD_SIZE = struct.calcsize(self.FORMAT)
        self.value_type_size = [(element[0], element[1], element[2]) for element in list_of_types]
        self.key_field = key_fieldd
        self.active = True

    def pack(self) -> bytes:
        procesados = []
        for item in self.value_type_size:
            valor = getattr(self, item[0])
            procesados.append(procesar_dato(valor, item[1],item[2]))

        procesados.append(self.active)
        format_with_active = self.FORMAT + "?"
        return struct.pack(format_with_active, *procesados)

    @staticmethod
    def unpack(data):
        fields = struct.unpack(Record.FORMAT, data)
        return Record() # mori falta hacer esto bien
    
    def get_key(self):
        return getattr(self, self.key_field) #retorna dinamicamente el valor del campo clave


class SequentialFile:
    def __init__(self, main_file: str, aux_file: str, record_class: Record, k_rec=None):
        self.main_file = main_file
        self.aux_file = aux_file
        self.record_class = record_class
        self.k = k_rec
        self.read_count = 0
        self.write_count = 0

        if not os.path.exists(self.main_file):
            open(self.main_file, 'wb').close()
        if not os.path.exists(self.aux_file):
            open(self.aux_file, 'wb').close()

    def reset_counters(self):
        self.read_count = 0
        self.write_count = 0

    def get_stats(self):
        return {
            "Lecturas": self.read_count,
            "Escrituras": self.write_count,
            "Total": self.read_count + self.write_count
        }
    
    def get_file_size(self, filename: str) -> int: # esto devuelve el numero de records en el archivo (general main o aux)
        if not os.path.exists(filename):
            return 0
        file_size = os.path.getsize(filename)
        return file_size // self.record_class.RECORD_SIZE
    

    def show_all_records_from_main_and_aux(self) -> List[Record]:

        records = []
        
        with open(self.main_file, 'rb') as f:
            while data := f.read(self.record_class.RECORD_SIZE):
                self.read_count += 1
                rec = self.record_class.unpack(data)
                if rec.active:
                    records.append(rec)
        
        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while data := f.read(self.record_class.RECORD_SIZE):
                    self.read_count += 1
                    rec = self.record_class.unpack(data)
                    if rec.active:
                        records.append(rec)
        
        return records
    
    def rebuild(self):
        records = []

        with open(self.main_file, 'rb') as f:
            while data := f.read(self.record_class.RECORD_SIZE):
                self.read_count += 1
                rec = self.record_class.unpack(data)
                if rec.active:
                    records.append(rec)

        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while data := f.read(self.record_class.RECORD_SIZE):
                    self.read_count += 1
                    rec = self.record_class.unpack(data)
                    if rec.active:
                        records.append(rec)
            os.remove(self.aux_file)

        records.sort(key=lambda r: r.get_key())

        with open(self.main_file, 'wb') as f:
            for record in records:
                f.write(record.pack())
                self.write_count += 1
        
        open(self.aux_file, 'wb').close()



    def add(self, registro : Record):
        with open(self.main_file, 'rb') as f:
            while data := f.read(self.record_class.RECORD_SIZE):
                self.read_count += 1
                rec = self.record_class.unpack(data)
                if rec.get_key() == registro.get_key() and rec.active:
                    return False, f"Error: Registro con clave {registro.get_key()} ya existe en main_file.", rec
        
        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while data := f.read(self.record_class.RECORD_SIZE):
                    self.read_count += 1
                    rec = self.record_class.unpack(data)
                    if rec.get_key() == registro.get_key() and rec.active:
                        return False, f"Error: Registro con clave {registro.get_key()} ya existe en aux_file.", rec

        registro.active = True  # implementar luego esto *la parte del active y no active)
        with open(self.aux_file, 'ab') as f:
            f.write(registro.pack())
            self.write_count += 1
        
        aux_size = self.get_file_size(self.aux_file)
        if aux_size > self.k:
            self.rebuild()
            return True, f"Registro con clave {registro.get_key()} insertado (migrado a main_file por reconstrucción).", registro

        return True, f"Registro con clave {registro.get_key()} insertado en aux_file.", registro
        
    
    def search(key):
        pass

    def rangeSearch(begin_key, end_key):
        pass

    def remove(key):
        pass

    
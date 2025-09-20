import csv
import os
import struct
import List


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


class Record:
    FORMAT = ""
    RECORD_SIZE = 0
    key_field = ""
    value_type_size = [] # es una tupla de 3 de (valor, tipo, size) para cada campo

    def __init__(self, list_of_types: List[List[str, str, int]]):
        self.FORMAT = making_format(list_of_types)
        self.RECORD_SIZE = struct.calcsize(self.FORMAT)
        self.value_type_size = [(element[0], element[1], element[2]) for element in list_of_types]

    def pack(self) -> bytes:
        procesados = []
        for item in self.value_type_size:
            valor = getattr(self, item[0])
            procesados.append(procesar_dato(valor, item[1],item[2]))
        return struct.pack(self.FORMAT, *procesados)
    
    @staticmethod
    def unpack(data):
        fields = struct.unpack(Record.FORMAT, data)
        return Record() # mori

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

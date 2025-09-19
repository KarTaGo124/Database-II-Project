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
    if field_type == "array[float]":
        pass
    if field_type == "array[char]":
        pass
    if field_type == "array[bool]":
        pass
    if field_type == "array[str]":
        pass
    raise ValueError(f"Tipo de array no encontrado, verificar valor ingresado: {field_type}")


class Record:
    FORMAT = ""
    RECORD_SIZE = 0
    value_type_size = [] # es una tupla de 3 de (valor, tipo, size) para cada campo

    def __init__(self, list_of_types: List[List[str, str, int]]):
        self.FORMAT = making_format(list_of_types)
        self.RECORD_SIZE = struct.calcsize(self.FORMAT)
        self.value_type_size = [(element[0], element[1], element[2]) for element in list_of_types]

    def pack(self) -> bytes:
        procesados = []
        for item in self.value_type_size:
            procesados.append(procesar_dato(item))
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
    

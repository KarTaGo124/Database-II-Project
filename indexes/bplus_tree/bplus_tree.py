import csv
import os
import struct
from typing import List

class TableMetadata:
    def __init__(self, table_name: str, list_of_types: List[List[str, str, int]], key_field: str):
        self.table_name = table_name
        self.list_of_types = list_of_types
        self.key_field = key_field
        self.record = Record(list_of_types, key_field)
        self.record_size = self.record.RECORD_SIZE + 1 # +1 por el campo active
    
class Record:
    FORMAT = ""
    RECORD_SIZE = 0
    key_field = "" # clave para el ordenamiento
    value_type_size = [] # es una tupla de 3 de (nombrevariable, tipo, size) para cada campo

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
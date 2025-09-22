import struct
import os
from typing import List, Tuple

class TableMetadata:
    def __init__(self, table_name: str, list_of_types: List[Tuple[str, str, int]], key_field: str):
        self.table_name = table_name
        self.list_of_types = list_of_types
        self.key_field = key_field
        self.record = Record(list_of_types, key_field)
        self.record_size = self.record.RECORD_SIZE + 1  # +1 por el campo active

class Record:
    def __init__(self, list_of_types: List[Tuple[str, str, int]], key_field: str):
        self.FORMAT = self._make_format(list_of_types)
        self.RECORD_SIZE = struct.calcsize(self.FORMAT)
        self.value_type_size = [(element[0], element[1], element[2]) for element in list_of_types]
        self.key_field = key_field
        self.active = True
        
        # Inicializar todos los campos como None
        for field_name, _, _ in self.value_type_size:
            setattr(self, field_name, None)

    def _make_format(self, list_of_types):
        """Genera el formato de struct dinámicamente"""
        format_str = ""
        for _, field_type, field_size in list_of_types:
            if field_type == "INT":
                format_str += "i"
            elif field_type == "FLOAT":
                format_str += "f"
            elif field_type == "CHAR":
                format_str += f"{field_size}s"
            elif field_type == "ARRAY":
                # Para arrays de FLOAT de dimensiones fijas
                format_str += f"{field_size}f"  # "2f" para 2 dimensiones
        return format_str

    def set_values(self, **kwargs):
        """Método flexible para asignar valores a cualquier campo"""
        for field_name, value in kwargs.items():
            if hasattr(self, field_name):
                setattr(self, field_name, value)
            else:
                raise AttributeError(f"Campo {field_name} no existe en el registro")
    def pack(self) -> bytes:
        """Empaqueta el registro para Sequential File"""
        processed_values = []
        for field_name, field_type, field_size in self.value_type_size:
            value = getattr(self, field_name)
            if field_type == "ARRAY":
                # Para arrays, agregar cada elemento individualmente
                if len(value) != field_size:
                    raise ValueError(f"Array debe tener {field_size} dimensiones")
                processed_values.extend(value)
            else:
                processed_values.append(self._process_value(value, field_type, field_size))
        processed_values.append(self.active)
        
        format_with_active = self.FORMAT + "?"
        return struct.pack(format_with_active, *processed_values)

    def _process_value(self, value, field_type: str, field_size: int):
        """Procesa cada valor según su tipo"""
        if field_type == "CHAR":
            return value.ljust(field_size).encode('utf-8')[:field_size]
        elif field_type == "INT":
            return int(value)
        elif field_type == "FLOAT":
            return float(value)
        return value

    def get_key(self, key_field: str = None):
        """Para ISAM, B+Tree, Hash - Obtiene cualquier campo como clave"""
        if key_field is None:
            key_field = self.key_field
        return getattr(self, key_field)

    def get_spatial_key(self, spatial_field: str):
        """Para RTree - Obtiene coordenadas geoespaciales"""
        return getattr(self, spatial_field)

    @classmethod
    def unpack(cls, data: bytes, list_of_types: List[Tuple[str, str, int]], key_field: str):
        """Desempaqueta bytes en un Record"""
        # Primero crear instancia vacía
        record = cls(list_of_types, key_field)
        
        format_str = record.FORMAT + "?"
        unpacked_data = struct.unpack(format_str, data)
        
        # Asignar valores a los campos
        for i, (field_name, field_type, field_size) in enumerate(record.value_type_size):
            value = unpacked_data[i]
            setattr(record, field_name, value)
        
        record.active = unpacked_data[-1]
        return record
    
    def __str__(self):
        """Representación legible del record"""
        fields = []
        for field_name, field_type, field_size in self.value_type_size:
            value = getattr(self, field_name)
            if field_type == "CHAR" and value:
                # Limpiar bytes nulos para CHAR
                if isinstance(value, bytes):
                    value = value.decode('utf-8').rstrip('\x00')
            fields.append(f"{field_name}: {value}")
        
        status = "Active" if self.active else "Deleted"
        return f"Record({', '.join(fields)}, Status: {status})"

    def __repr__(self):
        """Representación técnica del record"""
        return self.__str__()

    def print_detailed(self):
        """Impresión detallada con tipos"""
        print(f"Record Details")
        print(f"Key Field: {self.key_field}")
        print(f"Status: {'Active' if self.active else 'Deleted'}")
        print(f"Size: {self.RECORD_SIZE} bytes")
        print("Fields:")
        for field_name, field_type, field_size in self.value_type_size:
            value = getattr(self, field_name)
            if field_type == "CHAR" and isinstance(value, bytes):
                value = value.decode('utf-8').rstrip('\x00')
            print(f"  {field_name} ({field_type}[{field_size}]): {value}")
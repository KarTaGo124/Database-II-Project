import struct
import os
from typing import List, Tuple

class TableMetadata:
    def __init__(self, table_name: str, list_of_types: List[Tuple[str, str, int]], key_field: str):
        self.table_name = table_name
        self.list_of_types = list_of_types
        self.key_field = key_field

class Record:
    def __init__(self, list_of_types: List[Tuple[str, str, int]], key_field: str):
        self.list_of_types = list_of_types
        self.key_field = key_field
        self.FORMAT = self._make_format(list_of_types)
        self.RECORD_SIZE = struct.calcsize(self.FORMAT + "?")  # +1 por el campo active
        self.value_type_size = [(element[0], element[1], element[2]) for element in list_of_types]
        self.active = True
        
        # Inicializar todos los campos como None
        for field_name, _, _ in self.value_type_size:
            setattr(self, field_name, None)

    def _make_format(self, list_of_types):
        format_str = ""
        for _, field_type, field_size in list_of_types:
            if field_type == "INT":
                format_str += "i"
            elif field_type == "FLOAT":
                format_str += "f"
            elif field_type == "CHAR":
                format_str += f"{field_size}s"
            elif field_type == "ARRAY":
                format_str += f"{field_size}f"
        return format_str

    def set_values(self, **kwargs):
        for field_name, value in kwargs.items():
            if hasattr(self, field_name):
                setattr(self, field_name, value)
            else:
                raise AttributeError(f"Campo {field_name} no existe en el registro")

    def pack(self) -> bytes:
        processed_values = []
        for field_name, field_type, field_size in self.value_type_size:
            value = getattr(self, field_name)
            if field_type == "ARRAY":
                if value is None:
                    processed_values.extend([0.0] * field_size)
                else:
                    if len(value) != field_size:
                        raise ValueError(f"Array debe tener {field_size} dimensiones")
                    processed_values.extend(value)
            else:
                processed_values.append(self._process_value(value, field_type, field_size))
        
        processed_values.append(self.active)
        
        format_with_active = self.FORMAT + "?"
        return struct.pack(format_with_active, *processed_values)

    def _process_value(self, value, field_type: str, field_size: int):
        if value is None:
            if field_type == "CHAR":
                return b'\x00' * field_size
            elif field_type == "INT":
                return 0
            elif field_type == "FLOAT":
                return 0.0
        
        if field_type == "CHAR":
            if isinstance(value, str):
                return value.ljust(field_size).encode('utf-8')[:field_size]
            return value
        elif field_type == "INT":
            return int(value)
        elif field_type == "FLOAT":
            return float(value)
        return value

    def get_key(self, key_field: str = None):
        if key_field is None:
            key_field = self.key_field
        return getattr(self, key_field)

    def get_spatial_key(self, spatial_field: str):
        return getattr(self, spatial_field)

    @classmethod
    def unpack(cls, data: bytes, list_of_types: List[Tuple[str, str, int]], key_field: str):
        record = cls(list_of_types, key_field)
        
        format_str = record.FORMAT + "?"
        unpacked_data = struct.unpack(format_str, data)
        
        data_index = 0
        for field_name, field_type, field_size in record.value_type_size:
            if field_type == "ARRAY":
                array_values = []
                for i in range(field_size):
                    array_values.append(unpacked_data[data_index])
                    data_index += 1
                setattr(record, field_name, array_values)
            else:
                value = unpacked_data[data_index]
                if field_type == "CHAR" and isinstance(value, bytes):
                    value = value.decode('utf-8').rstrip('\x00')
                setattr(record, field_name, value)
                data_index += 1
        
        record.active = unpacked_data[-1]
        return record
    
    def __str__(self):
        fields = []
        for field_name, field_type, field_size in self.value_type_size:
            value = getattr(self, field_name)
            if field_type == "CHAR" and value and isinstance(value, bytes):
                value = value.decode('utf-8').rstrip('\x00')
            fields.append(f"{field_name}: {value}")
        
        status = "Active" if self.active else "Deleted"
        return f"Record({', '.join(fields)}, Status: {status})"

    def __repr__(self):
        return self.__str__()

    def print_detailed(self):
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
import csv
import os
import struct
import List


def making_format(list_of_types: List[List[str, str, int]]) -> str:
    FORMAT = ""
    for element in list_of_types:
        field_name = element[0]
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


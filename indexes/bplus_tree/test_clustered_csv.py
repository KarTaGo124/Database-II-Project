import csv
from bplus_tree_clustered import BPlusTreeClusteredIndex
import os

CSV_PATH = 'data/datasets/sales_dataset_unsorted.csv'
ORDER = 4
KEY_COLUMN = 'ID de la venta'
FILE_PATH = 'test_bplustree_clustered_unsorted.pkl'

class DummyRecord:
    def __init__(self, key, nombre, cantidad, precio, fecha):
        self.key = key
        self.nombre = nombre
        self.cantidad = cantidad
        self.precio = precio
        self.fecha = fecha
    def get_field_value(self, field):
        if field == 'ID de la venta':
            return self.key
        elif field == 'Nombre producto':
            return self.nombre
        elif field == 'Cantidad vendida':
            return self.cantidad
        elif field == 'Precio unitario':
            return self.precio
        elif field == 'Fecha de venta':
            return self.fecha
        else:
            raise AttributeError(f"Campo {field} no existe")
    def __repr__(self):
        return f"DummyRecord(key={self.key}, nombre={self.nombre}, cantidad={self.cantidad}, precio={self.precio}, fecha={self.fecha})"

if os.path.exists(FILE_PATH):
    os.remove(FILE_PATH)

bpt = BPlusTreeClusteredIndex(order=ORDER, key_column=KEY_COLUMN, file_path=FILE_PATH, record_class=DummyRecord)

with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    id_col = reader.fieldnames[0]
    for i, row in enumerate(reader):
        rec = DummyRecord(
            int(row[id_col]),
            row['Nombre producto'],
            int(row['Cantidad vendida']),
            float(row['Precio unitario']),
            row['Fecha de venta']
        )
        assert bpt.insert(rec), f"No se pudo insertar venta {row[id_col]}"

print("Carga de sales_dataset_unsorted.csv en B+ Tree Clustered completada correctamente.")

print("Prueba búsqueda de algunos IDs:")
for test_id in [403, 56, 107, 402, 117]:
    result = bpt.search(test_id)
    print(f"Buscar ID {test_id}: {result}")
    assert result is not None, f"No se encontró el ID {test_id}"

delete_id = 403
print(f"Borrando ID {delete_id}")
assert bpt.delete(delete_id), f"No se pudo borrar el ID {delete_id}"
assert bpt.search(delete_id) is None, f"El ID {delete_id} no fue borrado correctamente"

print("Prueba range_search:")
start_id = 56
end_id = 117
range_results = bpt.range_search(start_id, end_id)
print(f"IDs en el rango {start_id} a {end_id}: {range_results}")
assert len(range_results) > 0, "No se encontraron resultados en el rango"

print("Todas las pruebas del B+ Tree Clustered con datos del CSV pasaron correctamente.")

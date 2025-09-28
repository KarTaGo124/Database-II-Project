from bplus_tree_clustered import BPlusTreeClusteredIndex, ClusteredLeafNode, ClusteredInternalNode
import os

class DummyRecord:
    def __init__(self, key, value):
        self.key = key
        self.value = value
    def get_field_value(self, field):
        return getattr(self, field)
    def __repr__(self):
        return f"DummyRecord(key={self.key}, value={self.value})"

ORDER = 4
KEY_COLUMN = 'key'
FILE_PATH = 'test_bplustree_clustered.pkl'

if os.path.exists(FILE_PATH):
    os.remove(FILE_PATH)

bpt = BPlusTreeClusteredIndex(order=ORDER, key_column=KEY_COLUMN, file_path=FILE_PATH, record_class=DummyRecord)

records = [DummyRecord(i, f"valor_{i}") for i in [10, 20, 30, 40, 50]]
for rec in records:
    assert bpt.insert(rec), f"No se pudo insertar {rec}"

for rec in records:
    found = bpt.search(rec.key)
    assert found == rec, f"Búsqueda falló para {rec.key}"

rec_dup = DummyRecord(10, "valor_duplicado")
assert not bpt.insert(rec_dup), "Insertó duplicado"

assert bpt.delete(20), "No se pudo borrar 20"
assert bpt.search(20) is None, "No se borró correctamente 20"

result = bpt.range_search(10, 40)
print("Range search 10-40:", result)
assert all(isinstance(r, DummyRecord) for r in result), "Range search no devolvió registros"

print("Pruebas básicas de B+ Tree Clustered completadas correctamente.")

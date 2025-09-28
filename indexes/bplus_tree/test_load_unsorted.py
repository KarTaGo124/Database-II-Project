import csv
from bplus_tree_unclustered import BPlusTreeUnclusteredIndex, RecordPointer
from core.record import Record
import os

ORDER = 4
CSV_PATH = 'data/datasets/sales_dataset_unsorted.csv'
FILE_PATH = 'test_bplustree_unsorted_dataset.pkl'

if os.path.exists(FILE_PATH):
    os.remove(FILE_PATH)

with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    print('Columns:', reader.fieldnames)
    id_col = reader.fieldnames[0]
    fields = [
        (id_col, 'INT', 4),
        ('Nombre producto', 'CHAR', 64),
        ('Cantidad vendida', 'INT', 4),
        ('Precio unitario', 'FLOAT', 8),
        ('Fecha de venta', 'CHAR', 16)
    ]
    bpt = BPlusTreeUnclusteredIndex(order=ORDER, index_column=id_col, file_path=FILE_PATH)
    inserted_ids = []
    for i, row in enumerate(reader):
        rec = Record(fields, id_col)
        rec.set_field_value(id_col, int(row[id_col]))
        rec.set_field_value('Nombre producto', row['Nombre producto'])
        rec.set_field_value('Cantidad vendida', int(row['Cantidad vendida']))
        rec.set_field_value('Precio unitario', float(row['Precio unitario']))
        rec.set_field_value('Fecha de venta', row['Fecha de venta'])
        ptr = RecordPointer(page_number=0, slot_number=i)
        assert bpt.insert(rec, ptr), f"No se pudo insertar venta {row[id_col]}"
        inserted_ids.append(int(row[id_col]))

print("Load of sales_dataset_unsorted.csv into B+ Tree Unclustered completed.")

print("Tests of search for some IDs:")
for test_id in inserted_ids[:5] + inserted_ids[-5:]:
    result = bpt.search(test_id)
    print(f"find ID {test_id}: {result}")
    assert result is not None, f" can not find ID {test_id}"

delete_id = inserted_ids[0]
print(f"Delete ID {delete_id}")
assert bpt.delete(delete_id), f"can not delete  ID {delete_id}"
assert bpt.search(delete_id) is None, f"El ID {delete_id} can not delete"

print("Test range_search:")
start_id = min(inserted_ids)
end_id = start_id + 10
range_results = bpt.range_search(start_id, end_id)
print(f"IDs in range {start_id} to {end_id}: {range_results}")
assert len(range_results) > 0, "No results"

print("Info")
stats = bpt.info_btree_unclustered()
print("info of the b plus tree:", stats)

print("Finished")
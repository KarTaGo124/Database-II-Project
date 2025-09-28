"""Simple tests to check the code of the b plus tree unclustered"""

from bplus_tree_unclustered import BPlusTreeUnclusteredIndex, RecordPointer
from core.record import Record
import os

ORDER = 4
INDEX_COLUMN = 'id'
FILE_PATH = 'test_bplustree_unclustered.pkl'

if os.path.exists(FILE_PATH):
    os.remove(FILE_PATH)

bpt = BPlusTreeUnclusteredIndex(order=ORDER, index_column=INDEX_COLUMN, file_path=FILE_PATH)

records = []
for id_value in [10, 20, 30, 40, 50]:
    rec = Record([('id', 'INT', 4)], 'id')
    rec.set_field_value('id', id_value)
    records.append(rec)
pointers = [RecordPointer(page_number=i, slot_number=i) for i in range(len(records))]

for rec, ptr in zip(records, pointers):
    assert bpt.insert(rec, ptr), f"could not insert {rec}"

# test for search
for rec, ptr in zip(records, pointers):
    found = bpt.search(rec.get_field_value('id'))
    assert found == ptr, f"Not found {rec.get_field_value('id')}"

# test for duplicate
rec_dup = Record([('id', 'INT', 4)], 'id')
rec_dup.set_field_value('id', 10)
assert not bpt.insert(rec_dup, RecordPointer(99, 99)), "duplicate"

# test dellete
assert bpt.delete(20), "can not delete 20"
assert bpt.search(20) is None, "can not delete 20"

print("Finished some tests")
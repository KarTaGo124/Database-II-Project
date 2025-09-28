import os
import sys
import shutil
import csv
import random
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table, Record

def test_sequential_isam_comprehensive():
    print("=== Test Completo: Sequential File Primary + ISAM Secondary ===")

    db_name = "test_seq_isam_full"
    if os.path.exists(f"data/databases/{db_name}"):
        shutil.rmtree(f"data/databases/{db_name}")

    db_manager = DatabaseManager(db_name)

    table = Table(
        table_name="sales",
        sql_fields=[
            ("sale_id", "INT", 4),
            ("product_name", "CHAR", 50),
            ("quantity", "INT", 4),
            ("unit_price", "FLOAT", 4)
        ],
        key_field="sale_id"
    )

    print("1. Creando tabla con Sequential File como primary index...")
    db_manager.create_table(table, primary_index_type="SEQUENTIAL")

    print("2. Creando índices secundarios ISAM...")
    db_manager.create_index("sales", "product_name", "ISAM")
    db_manager.create_index("sales", "quantity", "ISAM")

    print("3. Cargando TODOS los datos desde CSV...")
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'datasets', 'sales_dataset_unsorted.csv')

    inserted_count = 0
    start_time = time.time()

    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')

        for i, row in enumerate(reader):
            try:
                table_info = db_manager.tables["sales"]
                primary_index = table_info["primary_index"]
                record = Record(primary_index.table.all_fields, primary_index.table.key_field)

                id_key = next(k for k in row.keys() if 'ID de la venta' in k)

                sale_data = {
                    "sale_id": int(row[id_key]),
                    "product_name": row['Nombre producto'][:50],
                    "quantity": int(row['Cantidad vendida']),
                    "unit_price": float(row['Precio unitario']),
                    "active": True
                }

                record.set_values(**sale_data)
                db_manager.insert("sales", record)
                inserted_count += 1

                if inserted_count % 100 == 0:
                    print(f"   Insertados: {inserted_count} registros")

            except Exception as e:
                print(f"   Error insertando registro {i}: {e}")

    load_time = time.time() - start_time
    print(f"\n4. Total insertado: {inserted_count} registros en {load_time:.2f}s")

    # Test stats del Sequential File
    print("\n5. Stats del Sequential File...")
    table_info = db_manager.tables["sales"]
    primary_index = table_info["primary_index"]
    stats = primary_index.get_stats()
    print(f"   I/O Stats: {stats}")
    print(f"   k (log n): {primary_index.k}")
    print(f"   Total records: {primary_index.total_records}")
    print(f"   Deleted count: {primary_index.deleted_count}")

    # Test búsquedas por primary key
    print("\n6. Test búsquedas por primary key...")
    test_ids = [403, 56, 107, 402, 117, 858, 681, 314, 33, 999999]  # El último no existe

    start_time = time.time()
    for test_id in test_ids:
        result = db_manager.search("sales", test_id)
        if result:
            print(f"   ID {test_id}: {result.product_name.decode().strip()}")
        else:
            print(f"   ID {test_id}: No encontrado")
    search_time = time.time() - start_time
    print(f"   Tiempo 10 búsquedas: {search_time:.4f}s")

    # Test búsquedas por secondary index (product_name)
    print("\n7. Test búsquedas por secondary index (product_name)...")
    test_products = ["Drone", "Laptop", "Monitor", "Mouse", "NoExiste"]

    for product in test_products:
        search_name = product.encode().ljust(50, b'\x00')
        result = db_manager.search_by_secondary("sales", "product_name", search_name)
        if result and len(result) > 0:
            print(f"   '{product}': {len(result)} resultados, primer ID: {result[0].sale_id}")
        else:
            print(f"   '{product}': No encontrado")

    # Test búsquedas por secondary index (quantity)
    print("\n8. Test búsquedas por secondary index (quantity)...")
    test_quantities = [1, 5, 10, 20, 999]

    for qty in test_quantities:
        result = db_manager.search_by_secondary("sales", "quantity", qty)
        if result and len(result) > 0:
            print(f"   Cantidad {qty}: {len(result)} resultados")
        else:
            print(f"   Cantidad {qty}: No encontrado")

    # Test range searches
    print("\n9. Test range searches...")
    ranges = [(1, 100), (100, 500), (500, 1000), (800, 900)]

    for start, end in ranges:
        start_time = time.time()
        results = db_manager.range_search("sales", start, end)
        range_time = time.time() - start_time
        print(f"   Range ({start}-{end}): {len(results)} resultados en {range_time:.4f}s")

    # Test deletes
    print("\n10. Test eliminaciones...")
    delete_ids = [403, 56, 107]  # IDs que sabemos que existen

    for del_id in delete_ids:
        success = db_manager.delete("sales", del_id)
        if success:
            print(f"   Eliminado ID {del_id}")
            # Verificar que ya no existe
            result = db_manager.search("sales", del_id)
            if result is None:
                print(f"     Verificado: ID {del_id} ya no existe")
            else:
                print(f"     ERROR: ID {del_id} aún existe después de eliminar")
        else:
            print(f"   ERROR: No se pudo eliminar ID {del_id}")

    # Stats finales
    print("\n11. Stats finales...")
    final_stats = primary_index.get_stats()
    print(f"   I/O Stats finales: {final_stats}")
    print(f"   k final: {primary_index.k}")
    print(f"   Total records final: {primary_index.total_records}")
    print(f"   Deleted count final: {primary_index.deleted_count}")

    # Test scan all
    print("\n12. Test scan all...")
    all_records = primary_index.scanAll()
    print(f"   Total records activos: {len(all_records)}")

    # Verificar que no hay records con active=False en scanAll
    active_count = sum(1 for r in all_records if r.active)
    print(f"   Records con active=True: {active_count}")

    if active_count == len(all_records):
        print("   OK: scanAll solo retorna records activos")
    else:
        print("   ERROR: scanAll retorna records inactivos")

    print("\n13. Información final de la tabla...")
    info = db_manager.get_table_info("sales")
    print(f"   {info}")

    print("\n=== Test completo terminado ===")

if __name__ == "__main__":
    test_sequential_isam_comprehensive()
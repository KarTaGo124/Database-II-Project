#!/usr/bin/env python3

import os
import sys
import csv
import time
import random
import shutil
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table, Record

def clean_data_files():
    test_db_path = "data/databases/test_performance_comparison"
    if os.path.exists(test_db_path):
        shutil.rmtree(test_db_path)

def load_full_dataset(db_manager, table):
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'datasets', 'sales_dataset_unsorted.csv')

    inserted_count = 0
    total_insert_time = 0
    total_disk_accesses = 0

    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')

        for i, row in enumerate(reader):
            try:
                record = Record(table.all_fields, table.key_field)

                id_key = next(k for k in row.keys() if 'ID de la venta' in k)

                sale_data = {
                    "id_venta": int(row[id_key]),
                    "nombre_producto": row['Nombre producto'][:50],
                    "cantidad_vendida": int(row['Cantidad vendida']),
                    "precio_unitario": float(row['Precio unitario'])
                }

                record.set_values(**sale_data)
                result = db_manager.insert("sales", record)

                total_insert_time += result.execution_time_ms
                total_disk_accesses += result.total_disk_accesses
                inserted_count += 1

                if inserted_count % 200 == 0:
                    print(f"   Insertados: {inserted_count} registros")

            except Exception as e:
                print(f"   Error insertando registro {i}: {e}")
                break

    return inserted_count, total_insert_time, total_disk_accesses

def performance_comparison_test():
    print("=" * 80)
    print("TEST PERFORMANCE: BÚSQUEDAS INDEXADAS vs NO INDEXADAS")
    print("=" * 80)

    clean_data_files()

    db_manager = DatabaseManager("test_performance_comparison")

    table = Table(
        table_name="sales",
        sql_fields=[
            ("id_venta", "INT", 4),
            ("nombre_producto", "CHAR", 50),
            ("cantidad_vendida", "INT", 4),
            ("precio_unitario", "FLOAT", 4)
        ],
        key_field="id_venta"
    )

    print("1. Creando tabla con índice primario ISAM...")
    db_manager.create_table(table, primary_index_type="ISAM")

    print("2. Creando índice secundario SOLO en 'nombre_producto'...")
    db_manager.create_index("sales", "nombre_producto", "ISAM")
    print("   - nombre_producto: INDEXADO")
    print("   - cantidad_vendida: NO INDEXADO")
    print("   - precio_unitario: NO INDEXADO")

    print("\n3. Cargando dataset completo (1000 registros)...")
    inserted_count, total_insert_time, total_disk_accesses = load_full_dataset(db_manager, table)
    print(f"   Total insertado: {inserted_count} registros")
    print(f"   Tiempo total: {total_insert_time/1000:.2f}s")
    print(f"   Accesos totales: {total_disk_accesses}")

    print("\n" + "=" * 60)
    print("COMPARACIÓN DE BÚSQUEDAS INDIVIDUALES")
    print("=" * 60)

    print("\n4. Búsqueda por PRIMARY KEY (id_venta)")
    primary_key = 500
    result = db_manager.search("sales", primary_key)
    print(f"   ID {primary_key}: {'Encontrado' if result.data else 'No encontrado'}")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n5. Búsqueda por SECONDARY INDEX (nombre_producto)")
    product_name = "Laptop"
    result = db_manager.search("sales", product_name, "nombre_producto")
    found_count = len(result.data) if result.data else 0
    print(f"   Producto '{product_name}': {found_count} resultados")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n6. Búsqueda por ATRIBUTO NO INDEXADO (cantidad_vendida)")
    quantity = 10
    result = db_manager.search("sales", quantity, "cantidad_vendida")
    found_count = len(result.data) if result.data else 0
    print(f"   Cantidad {quantity}: {found_count} resultados")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n7. Búsqueda por ATRIBUTO NO INDEXADO (precio_unitario)")
    price = 100.0
    result = db_manager.search("sales", price, "precio_unitario")
    found_count = len(result.data) if result.data else 0
    print(f"   Precio {price}: {found_count} resultados")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n" + "=" * 60)
    print("COMPARACIÓN DE BÚSQUEDAS POR RANGO")
    print("=" * 60)

    print("\n8. Range search por PRIMARY KEY")
    result = db_manager.range_search("sales", 100, 200)
    found_count = len(result.data) if result.data else 0
    print(f"   IDs 100-200: {found_count} resultados")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n9. Range search por ATRIBUTO NO INDEXADO (cantidad_vendida)")
    result = db_manager.range_search("sales", 5, 15, "cantidad_vendida")
    found_count = len(result.data) if result.data else 0
    print(f"   Cantidad 5-15: {found_count} resultados")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n10. Range search por ATRIBUTO NO INDEXADO (precio_unitario)")
    result = db_manager.range_search("sales", 50.0, 150.0, "precio_unitario")
    found_count = len(result.data) if result.data else 0
    print(f"   Precio 50-150: {found_count} resultados")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n" + "=" * 60)
    print("PRUEBAS DE MÚLTIPLES BÚSQUEDAS")
    print("=" * 60)

    print("\n11. 50 búsquedas aleatorias por PRIMARY KEY")
    primary_keys = random.sample(range(1, 1001), 50)
    start_time = time.time()
    total_accesses = 0
    found_count = 0

    for pk in primary_keys:
        result = db_manager.search("sales", pk)
        if result.data:
            found_count += 1
        total_accesses += result.total_disk_accesses

    elapsed_time = (time.time() - start_time) * 1000
    print(f"   Encontrados: {found_count}/50")
    print(f"   Tiempo total: {elapsed_time:.3f} ms")
    print(f"   Tiempo promedio: {elapsed_time/50:.3f} ms por búsqueda")
    print(f"   Accesos totales: {total_accesses}")
    print(f"   Accesos promedio: {total_accesses/50:.1f} por búsqueda")

    print("\n12. 20 búsquedas por SECONDARY INDEX")
    products = ["Laptop", "Mouse", "Monitor", "Smartphone", "Tablet"] * 4
    start_time = time.time()
    total_accesses = 0
    found_count = 0

    for product in products:
        result = db_manager.search("sales", product, "nombre_producto")
        if result.data and len(result.data) > 0:
            found_count += 1
        total_accesses += result.total_disk_accesses

    elapsed_time = (time.time() - start_time) * 1000
    print(f"   Búsquedas exitosas: {found_count}/20")
    print(f"   Tiempo total: {elapsed_time:.3f} ms")
    print(f"   Tiempo promedio: {elapsed_time/20:.3f} ms por búsqueda")
    print(f"   Accesos totales: {total_accesses}")
    print(f"   Accesos promedio: {total_accesses/20:.1f} por búsqueda")

    print("\n13. 20 búsquedas por ATRIBUTO NO INDEXADO")
    quantities = [1, 5, 10, 15, 20] * 4
    start_time = time.time()
    total_accesses = 0
    found_count = 0

    for qty in quantities:
        result = db_manager.search("sales", qty, "cantidad_vendida")
        if result.data and len(result.data) > 0:
            found_count += 1
        total_accesses += result.total_disk_accesses

    elapsed_time = (time.time() - start_time) * 1000
    print(f"   Búsquedas exitosas: {found_count}/20")
    print(f"   Tiempo total: {elapsed_time:.3f} ms")
    print(f"   Tiempo promedio: {elapsed_time/20:.3f} ms por búsqueda")
    print(f"   Accesos totales: {total_accesses}")
    print(f"   Accesos promedio: {total_accesses/20:.1f} por búsqueda")

    print("\n" + "=" * 60)
    print("COMPARACIÓN DE ELIMINACIONES")
    print("=" * 60)

    print("\n14. DELETE por PRIMARY KEY")
    delete_id = 999
    result = db_manager.delete("sales", delete_id)
    print(f"   DELETE ID {delete_id}: {'Exitoso' if result.data else 'Falló'}")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n15. DELETE por SECONDARY INDEX")
    delete_product = "Drone"
    result = db_manager.delete("sales", delete_product, "nombre_producto")
    print(f"   DELETE producto '{delete_product}': {result.data} registros eliminados")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n16. DELETE por ATRIBUTO NO INDEXADO")
    delete_quantity = 25
    result = db_manager.delete("sales", delete_quantity, "cantidad_vendida")
    print(f"   DELETE cantidad {delete_quantity}: {result.data} registros eliminados")
    print(f"   Tiempo: {result.execution_time_ms:.3f} ms")
    print(f"   Accesos a disco: {result.total_disk_accesses}")

    print("\n" + "=" * 80)
    print("RESUMEN COMPARATIVO")
    print("=" * 80)
    print("CONCLUSIONES:")
    print("1. Primary Key (indexado): Más eficiente - pocos accesos a disco")
    print("2. Secondary Index (indexado): Eficiente - accesos moderados")
    print("3. Atributos no indexados: Menos eficiente - muchos accesos (full scan)")
    print("4. La diferencia es más notable con datasets grandes")
    print("\n*** IMPORTANCIA DE LOS ÍNDICES DEMOSTRADA ***")

if __name__ == "__main__":
    performance_comparison_test()
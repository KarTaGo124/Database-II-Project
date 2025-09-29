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
    """Limpia las bases de datos de prueba anteriores"""
    # Get the project root directory
    project_root = os.path.join(os.path.dirname(__file__), '..')
    test_dbs = [
        os.path.join(project_root, "data", "databases", "test_sequential_index"),
        os.path.join(project_root, "data", "databases", "test_isam_index")
    ]

    for db_path in test_dbs:
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

def load_dataset_to_db(db_manager, table_name, csv_path, limit=1000):
    """Carga el dataset en una base de datos específica"""
    inserted_count = 0
    total_insert_time = 0
    total_disk_accesses = 0

    # Get the actual table from the database manager to use correct field structure
    table_info = db_manager.get_table_info(table_name)
    table = db_manager.tables[table_name]["table"]

    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')

        for i, row in enumerate(reader):
            if inserted_count >= limit:
                break

            try:
                record = Record(table.all_fields, table.key_field)

                id_key = next(k for k in row.keys() if 'ID de la venta' in k)

                sale_data = {
                    "id_venta": int(row[id_key]),
                    "nombre_producto": row['Nombre producto'][:50],
                    "cantidad_vendida": int(row['Cantidad vendida']),
                    "precio_unitario": float(row['Precio unitario'])
                }

                # Add active field if it exists in the table (for Sequential File)
                if any(field[0] == 'active' for field in table.all_fields):
                    sale_data["active"] = True

                record.set_values(**sale_data)
                result = db_manager.insert(table_name, record)

                total_insert_time += result.execution_time_ms
                total_disk_accesses += result.total_disk_accesses
                inserted_count += 1

                if inserted_count % 200 == 0:
                    print(f"   Insertados: {inserted_count} registros")

            except Exception as e:
                print(f"   Error insertando registro {i}: {e}")
                continue

    return inserted_count, total_insert_time, total_disk_accesses

def test_search_operations(db_manager, table_name, index_type):
    """Prueba diferentes operaciones de búsqueda y retorna métricas"""
    print(f"\n--- PRUEBAS DE BÚSQUEDA ({index_type}) ---")

    _ = db_manager.search(table_name, 1)

    results = {}

    print(f"1. Búsqueda por Primary Key (ID)")
    test_ids = [100, 500, 750, 999]
    pk_times = []
    pk_accesses = []

    for test_id in test_ids:
        result = db_manager.search(table_name, test_id)
        pk_times.append(result.execution_time_ms)
        pk_accesses.append(result.total_disk_accesses)
        print(f"   ID {test_id}: {result.execution_time_ms:.3f}ms, {result.total_disk_accesses} accesos")

    results['primary_key'] = {
        'avg_time': sum(pk_times) / len(pk_times),
        'avg_accesses': sum(pk_accesses) / len(pk_accesses)
    }

    # 2. Range search
    print(f"2. Range Search (IDs 200-300)")
    result = db_manager.range_search(table_name, 200, 300)
    found_count = len(result.data) if result.data else 0
    print(f"   Encontrados: {found_count}, Tiempo: {result.execution_time_ms:.3f}ms, Accesos: {result.total_disk_accesses}")

    results['range_search'] = {
        'time': result.execution_time_ms,
        'accesses': result.total_disk_accesses,
        'found': found_count
    }

    # 3. Múltiples búsquedas aleatorias
    print(f"3. 20 Búsquedas Aleatorias")
    random_ids = random.sample(range(1, 1001), 20)
    total_time = 0
    total_accesses = 0
    found_count = 0

    start_time = time.time()
    for rid in random_ids:
        result = db_manager.search(table_name, rid)
        if result.data:
            found_count += 1
        total_time += result.execution_time_ms
        total_accesses += result.total_disk_accesses

    batch_time = (time.time() - start_time) * 1000
    print(f"   Encontrados: {found_count}/20")
    print(f"   Tiempo total: {batch_time:.3f}ms")
    print(f"   Tiempo promedio: {total_time/20:.3f}ms por búsqueda")
    print(f"   Accesos promedio: {total_accesses/20:.1f} por búsqueda")

    results['batch_search'] = {
        'avg_time': total_time / 20,
        'avg_accesses': total_accesses / 20,
        'found_ratio': found_count / 20
    }

    return results

def test_insert_operations(db_manager, table_name, index_type, count=100):
    """Prueba inserciones adicionales y retorna métricas"""
    print(f"\n--- PRUEBAS DE INSERCIÓN ({index_type}) ---")

    # Get the actual table from the database manager to use correct field structure
    table = db_manager.tables[table_name]["table"]

    total_time = 0
    total_accesses = 0
    successful_inserts = 0

    # Generar IDs únicos que no existan (empezando desde 10000)
    start_time = time.time()
    for i in range(count):
        try:
            record = Record(table.all_fields, table.key_field)

            new_id = 10000 + i
            sale_data = {
                "id_venta": new_id,
                "nombre_producto": f"Test Product {i}",
                "cantidad_vendida": random.randint(1, 100),
                "precio_unitario": round(random.uniform(10.0, 2000.0), 2)
            }

            # Add active field if it exists in the table (for Sequential File)
            if any(field[0] == 'active' for field in table.all_fields):
                sale_data["active"] = True

            record.set_values(**sale_data)
            result = db_manager.insert(table_name, record)

            total_time += result.execution_time_ms
            total_accesses += result.total_disk_accesses
            successful_inserts += 1

        except Exception as e:
            print(f"   Error insertando {new_id}: {e}")

    batch_time = (time.time() - start_time) * 1000

    print(f"   Insertados: {successful_inserts}/{count}")
    print(f"   Tiempo total: {batch_time:.3f}ms")
    print(f"   Tiempo promedio: {total_time/successful_inserts:.3f}ms por inserción")
    print(f"   Accesos promedio: {total_accesses/successful_inserts:.1f} por inserción")

    return {
        'avg_time': total_time / successful_inserts if successful_inserts > 0 else 0,
        'avg_accesses': total_accesses / successful_inserts if successful_inserts > 0 else 0,
        'success_ratio': successful_inserts / count
    }

def test_delete_operations(db_manager, table_name, index_type, count=50):
    """Prueba eliminaciones y retorna métricas"""
    print(f"\n--- PRUEBAS DE ELIMINACIÓN ({index_type}) ---")

    # Eliminar algunos de los registros insertados anteriormente
    delete_ids = list(range(10000, 10000 + count))
    total_time = 0
    total_accesses = 0
    successful_deletes = 0

    start_time = time.time()
    for delete_id in delete_ids:
        try:
            result = db_manager.delete(table_name, delete_id)
            if result.data:
                successful_deletes += 1
            total_time += result.execution_time_ms
            total_accesses += result.total_disk_accesses

        except Exception as e:
            print(f"   Error eliminando {delete_id}: {e}")

    batch_time = (time.time() - start_time) * 1000

    print(f"   Eliminados: {successful_deletes}/{count}")
    print(f"   Tiempo total: {batch_time:.3f}ms")
    print(f"   Tiempo promedio: {total_time/count:.3f}ms por eliminación")
    print(f"   Accesos promedio: {total_accesses/count:.1f} por eliminación")

    return {
        'avg_time': total_time / count,
        'avg_accesses': total_accesses / count,
        'success_ratio': successful_deletes / count
    }

def primary_index_comparison_test():
    """Comparación completa entre Sequential File e ISAM como índices primarios"""
    print("=" * 80)
    print("TEST COMPARATIVO: SEQUENTIAL FILE vs ISAM (ÍNDICES PRIMARIOS)")
    print("=" * 80)

    clean_data_files()

    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'datasets', 'sales_dataset_unsorted.csv')

    # Tabla de definición - Sequential necesita extra_fields
    table_sequential = Table(
        table_name="sales",
        sql_fields=[
            ("id_venta", "INT", 4),
            ("nombre_producto", "CHAR", 50),
            ("cantidad_vendida", "INT", 4),
            ("precio_unitario", "FLOAT", 4)
        ],
        key_field="id_venta",
        extra_fields={"active": ("BOOL", 1)}
    )

    # Tabla de definición - ISAM no necesita extra_fields
    table_isam = Table(
        table_name="sales",
        sql_fields=[
            ("id_venta", "INT", 4),
            ("nombre_producto", "CHAR", 50),
            ("cantidad_vendida", "INT", 4),
            ("precio_unitario", "FLOAT", 4)
        ],
        key_field="id_venta"
    )

    results = {}

    # ================================
    # PRUEBA 1: SEQUENTIAL FILE
    # ================================
    print("\n" + "=" * 50)
    print("PRUEBA 1: SEQUENTIAL FILE COMO ÍNDICE PRIMARIO")
    print("=" * 50)

    db_sequential = DatabaseManager("test_sequential_index")
    db_sequential.create_table(table_sequential, primary_index_type="SEQUENTIAL")

    print("1. Cargando 1000 registros con Sequential File...")
    seq_count, seq_load_time, seq_load_accesses = load_dataset_to_db(db_sequential, "sales", csv_path, 1000)
    print(f"   Registros cargados: {seq_count}")
    print(f"   Tiempo de carga: {seq_load_time/1000:.2f}s")
    print(f"   Accesos de carga: {seq_load_accesses}")
    print(f"   Tiempo promedio por inserción: {seq_load_time/seq_count:.3f}ms")
    print(f"   Accesos promedio por inserción: {seq_load_accesses/seq_count:.1f}")

    results['sequential'] = {
        'load_time': seq_load_time,
        'load_accesses': seq_load_accesses,
        'load_count': seq_count
    }

    # Pruebas de operaciones
    results['sequential']['search'] = test_search_operations(db_sequential, "sales", "SEQUENTIAL")
    results['sequential']['insert'] = test_insert_operations(db_sequential, "sales", "SEQUENTIAL", 100)
    results['sequential']['delete'] = test_delete_operations(db_sequential, "sales", "SEQUENTIAL", 50)

    # ================================
    # PRUEBA 2: ISAM
    # ================================
    print("\n" + "=" * 50)
    print("PRUEBA 2: ISAM COMO ÍNDICE PRIMARIO")
    print("=" * 50)

    db_isam = DatabaseManager("test_isam_index")
    db_isam.create_table(table_isam, primary_index_type="ISAM")

    print("1. Cargando 1000 registros con ISAM...")
    isam_count, isam_load_time, isam_load_accesses = load_dataset_to_db(db_isam, "sales", csv_path, 1000)
    print(f"   Registros cargados: {isam_count}")
    print(f"   Tiempo de carga: {isam_load_time/1000:.2f}s")
    print(f"   Accesos de carga: {isam_load_accesses}")
    print(f"   Tiempo promedio por inserción: {isam_load_time/isam_count:.3f}ms")
    print(f"   Accesos promedio por inserción: {isam_load_accesses/isam_count:.1f}")

    results['isam'] = {
        'load_time': isam_load_time,
        'load_accesses': isam_load_accesses,
        'load_count': isam_count
    }

    # Pruebas de operaciones
    results['isam']['search'] = test_search_operations(db_isam, "sales", "ISAM")
    results['isam']['insert'] = test_insert_operations(db_isam, "sales", "ISAM", 100)
    results['isam']['delete'] = test_delete_operations(db_isam, "sales", "ISAM", 50)

    # ================================
    # ANÁLISIS COMPARATIVO
    # ================================
    print("\n" + "=" * 80)
    print("ANÁLISIS COMPARATIVO DE RENDIMIENTO")
    print("=" * 80)

    # Comparación de carga inicial
    print("\n1. CARGA INICIAL DE DATOS:")
    print(f"   Sequential - Tiempo: {results['sequential']['load_time']/1000:.2f}s, Accesos: {results['sequential']['load_accesses']}")
    print(f"   ISAM       - Tiempo: {results['isam']['load_time']/1000:.2f}s, Accesos: {results['isam']['load_accesses']}")

    load_time_diff = ((results['isam']['load_time'] / results['sequential']['load_time']) - 1) * 100
    load_access_diff = ((results['isam']['load_accesses'] / results['sequential']['load_accesses']) - 1) * 100
    print(f"   ISAM vs Sequential: {load_time_diff:+.1f}% tiempo, {load_access_diff:+.1f}% accesos")

    # Comparación de búsquedas
    print("\n2. BÚSQUEDAS POR PRIMARY KEY:")
    seq_pk = results['sequential']['search']['primary_key']
    isam_pk = results['isam']['search']['primary_key']
    print(f"   Sequential - Promedio: {seq_pk['avg_time']:.3f}ms, {seq_pk['avg_accesses']:.1f} accesos")
    print(f"   ISAM       - Promedio: {isam_pk['avg_time']:.3f}ms, {isam_pk['avg_accesses']:.1f} accesos")

    pk_time_diff = ((isam_pk['avg_time'] / seq_pk['avg_time']) - 1) * 100
    pk_access_diff = ((isam_pk['avg_accesses'] / seq_pk['avg_accesses']) - 1) * 100
    print(f"   ISAM vs Sequential: {pk_time_diff:+.1f}% tiempo, {pk_access_diff:+.1f}% accesos")

    # Comparación de range search
    print("\n3. RANGE SEARCH:")
    seq_range = results['sequential']['search']['range_search']
    isam_range = results['isam']['search']['range_search']
    print(f"   Sequential - {seq_range['time']:.3f}ms, {seq_range['accesses']} accesos, {seq_range['found']} encontrados")
    print(f"   ISAM       - {isam_range['time']:.3f}ms, {isam_range['accesses']} accesos, {isam_range['found']} encontrados")

    range_time_diff = ((isam_range['time'] / seq_range['time']) - 1) * 100
    range_access_diff = ((isam_range['accesses'] / seq_range['accesses']) - 1) * 100
    print(f"   ISAM vs Sequential: {range_time_diff:+.1f}% tiempo, {range_access_diff:+.1f}% accesos")

    # Comparación de inserciones
    print("\n4. INSERCIONES ADICIONALES:")
    seq_ins = results['sequential']['insert']
    isam_ins = results['isam']['insert']
    print(f"   Sequential - Promedio: {seq_ins['avg_time']:.3f}ms, {seq_ins['avg_accesses']:.1f} accesos")
    print(f"   ISAM       - Promedio: {isam_ins['avg_time']:.3f}ms, {isam_ins['avg_accesses']:.1f} accesos")

    ins_time_diff = ((isam_ins['avg_time'] / seq_ins['avg_time']) - 1) * 100
    ins_access_diff = ((isam_ins['avg_accesses'] / seq_ins['avg_accesses']) - 1) * 100
    print(f"   ISAM vs Sequential: {ins_time_diff:+.1f}% tiempo, {ins_access_diff:+.1f}% accesos")

    # Comparación de eliminaciones
    print("\n5. ELIMINACIONES:")
    seq_del = results['sequential']['delete']
    isam_del = results['isam']['delete']
    print(f"   Sequential - Promedio: {seq_del['avg_time']:.3f}ms, {seq_del['avg_accesses']:.1f} accesos")
    print(f"   ISAM       - Promedio: {isam_del['avg_time']:.3f}ms, {isam_del['avg_accesses']:.1f} accesos")

    del_time_diff = ((isam_del['avg_time'] / seq_del['avg_time']) - 1) * 100
    del_access_diff = ((isam_del['avg_accesses'] / seq_del['avg_accesses']) - 1) * 100
    print(f"   ISAM vs Sequential: {del_time_diff:+.1f}% tiempo, {del_access_diff:+.1f}% accesos")

    # Conclusiones
    print("\n" + "=" * 80)
    print("CONCLUSIONES")
    print("=" * 80)
    print("SEQUENTIAL FILE:")
    print("  + Más simple de implementar")
    print("  + Menor overhead en inserciones iniciales")
    print("  - Búsquedas O(n) - escalabilidad limitada")
    print("  - Range search menos eficiente en datasets grandes")
    print("")
    print("ISAM (Indexed Sequential Access Method):")
    print("  + Búsquedas O(log n) - mejor escalabilidad")
    print("  + Range search más eficiente")
    print("  + Estructura de índice multinivel")
    print("  - Mayor overhead en inserciones (mantenimiento de índices)")
    print("  - Más complejo de implementar")
    print("")
    print("RECOMENDACIÓN:")
    if isam_pk['avg_accesses'] < seq_pk['avg_accesses']:
        print("-> ISAM es más eficiente en accesos a disco para búsquedas")
    else:
        print("-> Sequential File es más eficiente para este tamaño de dataset")
    print("")
    print(f"Para datasets de 1000+ registros, ISAM muestra ventajas en:")
    print(f"- Búsquedas por clave primaria")
    print(f"- Operaciones de rango")
    print(f"- Escalabilidad a largo plazo")

if __name__ == "__main__":
    random.seed(42)  # Para resultados reproducibles
    primary_index_comparison_test()
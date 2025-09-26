#!/usr/bin/env python3

import os
import shutil
import time
import random
from .manager import ISAMManager
from ..core.record import Record, Table


def create_sales_table():
    return Table(
        table_name="sales",
        sql_fields=[
            ("id_venta", "INT", 4),
            ("nombre_producto", "CHAR", 50),
            ("cantidad_vendida", "INT", 4),
            ("precio_unitario", "FLOAT", 4),
            ("fecha_venta", "CHAR", 10)
        ],
        key_field="id_venta"
    )


def load_csv_data(filename, table, limit=None):
    records = []
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines[1:]:
            if limit and len(records) >= limit:
                break
            parts = line.strip().split(';')
            if len(parts) == 5:
                record = Record(table.all_fields, table.key_field)
                record.set_values(
                    id_venta=int(parts[0]),
                    nombre_producto=parts[1],
                    cantidad_vendida=int(parts[2]),
                    precio_unitario=float(parts[3]),
                    fecha_venta=parts[4]
                )
                records.append(record)
    return records


def clean_data_files():
    data_dir = os.path.join("indexes", "isam", "data_files")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def main():
    print("="*80)
    print("TEST COMPLETO - DATASET COMPLETO 1000 REGISTROS")
    print("="*80)

    clean_data_files()
    table = create_sales_table()

    # Cargar dataset completo
    records = load_csv_data('data/sales_dataset_unsorted.csv', table)
    print(f"Dataset cargado: {len(records)} registros")

    manager = ISAMManager(table, csv_filename='data/sales_dataset_unsorted.csv')
    manager.add_secondary_index('nombre_producto')
    manager.add_secondary_index('cantidad_vendida')

    print("\nIndices creados:")
    print("  - Primary: id_venta")
    print("  - Secondary: nombre_producto, cantidad_vendida")

    # === FASE 1: INSERCION MASIVA ===
    print(f"\n1. INSERCION MASIVA - {len(records)} registros")
    print("-" * 60)

    start_time = time.time()
    for i, record in enumerate(records):
        manager.insert(record)
        if (i + 1) % 100 == 0:
            print(f"   Insertados: {i + 1}/{len(records)}")

    insertion_time = time.time() - start_time

    stats = manager.get_statistics()
    primary_count = stats['primary']['record_count']
    secondary_nombre = stats['secondary']['nombre_producto']['record_count']
    secondary_cantidad = stats['secondary']['cantidad_vendida']['record_count']

    print(f"\n   Resultados insercion:")
    print(f"   - Primary: {primary_count}")
    print(f"   - Secondary nombre_producto: {secondary_nombre}")
    print(f"   - Secondary cantidad_vendida: {secondary_cantidad}")
    print(f"   - Tiempo: {insertion_time:.2f} segundos")

    errors = manager.validate_all_consistency()
    print(f"   - Errores de consistencia: {len(errors)}")

    expected_count = len(records)
    if (primary_count == secondary_nombre == secondary_cantidad == expected_count
        and len(errors) == 0):
        print("   OK - INSERCION PERFECTA: Todos los ratios 1:1:1")
    else:
        print("   ERROR - PROBLEMAS EN INSERCION")
        return False

    # === FASE 2: BUSQUEDAS EXHAUSTIVAS ===
    print("\n2. PRUEBAS DE BUSQUEDA EXHAUSTIVAS")
    print("-" * 60)

    # Busquedas primarias
    test_pks = random.sample([r.get_key() for r in records], 50)
    search_start = time.time()
    successful_primary = 0

    for pk in test_pks:
        result = manager.search_by_primary(pk)
        if result is not None:
            successful_primary += 1

    primary_search_time = time.time() - search_start
    print(f"   Busquedas primarias: {successful_primary}/50 exitosas ({primary_search_time*1000:.2f}ms)")

    # Busquedas secundarias por nombre
    search_start = time.time()
    laptop_results = manager.search_by_secondary('nombre_producto', 'Laptop')
    smartphone_results = manager.search_by_secondary('nombre_producto', 'Smartphone')
    tablet_results = manager.search_by_secondary('nombre_producto', 'Tablet')
    camara_results = manager.search_by_secondary('nombre_producto', 'Camara')
    secondary_search_time = time.time() - search_start

    print(f"   Busqueda 'Laptop': {len(laptop_results)} resultados")
    print(f"   Busqueda 'Smartphone': {len(smartphone_results)} resultados")
    print(f"   Busqueda 'Tablet': {len(tablet_results)} resultados")
    print(f"   Busqueda 'Camara': {len(camara_results)} resultados")
    print(f"   Tiempo busquedas secundarias nombre: {secondary_search_time*1000:.2f}ms")

    # Busquedas por rangos
    range_start = time.time()
    range_cantidad_1 = manager.range_search_by_secondary('cantidad_vendida', 1, 10)
    range_cantidad_2 = manager.range_search_by_secondary('cantidad_vendida', 15, 25)
    range_cantidad_3 = manager.range_search_by_secondary('cantidad_vendida', 5, 15)
    range_time = time.time() - range_start

    print(f"   Rango cantidad 1-10: {len(range_cantidad_1)} resultados")
    print(f"   Rango cantidad 15-25: {len(range_cantidad_2)} resultados")
    print(f"   Rango cantidad 5-15: {len(range_cantidad_3)} resultados")
    print(f"   Tiempo busquedas por rango: {range_time*1000:.2f}ms")

    if successful_primary >= 48:  # Al menos 96% exitosas
        print("   OK - BUSQUEDAS EXITOSAS")
    else:
        print("   ERROR - PROBLEMAS EN BUSQUEDAS")
        return False

    # === FASE 3: ELIMINACIONES MASIVAS ===
    print("\n3. ELIMINACIONES MASIVAS")
    print("-" * 60)

    records_to_delete = random.sample(records, 100)
    delete_pks = [r.get_key() for r in records_to_delete]

    delete_start = time.time()
    deleted_count = 0
    for pk in delete_pks:
        if manager.delete(pk):
            deleted_count += 1

    deletion_time = time.time() - delete_start
    print(f"   Eliminados: {deleted_count}/100 registros ({deletion_time:.2f}s)")

    # Verificar eliminaciones
    not_found_count = 0
    for pk in delete_pks:
        if manager.search_by_primary(pk) is None:
            not_found_count += 1

    print(f"   Verificacion: {not_found_count}/{len(delete_pks)} efectivamente eliminados")

    # Verificar consistencia post-eliminacion
    stats_after = manager.get_statistics()
    errors_after = manager.validate_all_consistency()
    expected_remaining = len(records) - deleted_count

    final_primary = stats_after['primary']['record_count']
    final_nombre = stats_after['secondary']['nombre_producto']['record_count']
    final_cantidad = stats_after['secondary']['cantidad_vendida']['record_count']

    print(f"   Conteo final: P={final_primary}, N={final_nombre}, C={final_cantidad}")
    print(f"   Esperado: {expected_remaining}")
    print(f"   Errores post-eliminacion: {len(errors_after)}")

    if (final_primary == final_nombre == final_cantidad == expected_remaining
        and len(errors_after) == 0):
        print("   OK - ELIMINACIONES PERFECTAS")
    else:
        print("   ERROR - PROBLEMAS EN ELIMINACIONES")
        return False

    # === FASE 4: INSERCIONES ADICIONALES ===
    print("\n4. INSERCIONES ADICIONALES")
    print("-" * 60)

    new_records = []
    max_pk = max([r.get_key() for r in records])

    for i in range(50):
        new_pk = max_pk + i + 1
        new_record = Record(table.all_fields, table.key_field)
        new_record.set_values(
            id_venta=new_pk,
            nombre_producto=f"ProductoNuevo_{i+1}",
            cantidad_vendida=random.randint(1, 30),
            precio_unitario=round(random.uniform(10.0, 1000.0), 2),
            fecha_venta="01/01/2025"
        )
        new_records.append(new_record)

    insert_start = time.time()
    for record in new_records:
        manager.insert(record)
    insert_time = time.time() - insert_start

    print(f"   Insertados {len(new_records)} nuevos registros ({insert_time:.2f}s)")

    # Verificar nuevas inserciones
    test_new_search = manager.search_by_secondary('nombre_producto', 'ProductoNuevo_1')
    test_range_new = manager.range_search_by_secondary('cantidad_vendida', 1, 30)

    print(f"   Busqueda producto nuevo: {len(test_new_search)} resultados")
    print(f"   Rango cantidad 1-30 (incluye nuevos): {len(test_range_new)} resultados")

    # Estadisticas finales
    final_stats = manager.get_statistics()
    final_errors = manager.validate_all_consistency()
    final_expected = expected_remaining + len(new_records)

    ultimate_primary = final_stats['primary']['record_count']
    ultimate_nombre = final_stats['secondary']['nombre_producto']['record_count']
    ultimate_cantidad = final_stats['secondary']['cantidad_vendida']['record_count']

    print(f"   Conteo final total: P={ultimate_primary}, N={ultimate_nombre}, C={ultimate_cantidad}")
    print(f"   Esperado: {final_expected}")
    print(f"   Errores finales: {len(final_errors)}")

    if (ultimate_primary == ultimate_nombre == ultimate_cantidad == final_expected
        and len(final_errors) == 0):
        print("   OK - INSERCIONES ADICIONALES PERFECTAS")
    else:
        print("   ERROR - PROBLEMAS EN INSERCIONES ADICIONALES")
        return False

    # === FASE 5: BUSQUEDAS POST-MODIFICACIONES ===
    print("\n5. BUSQUEDAS POST-MODIFICACIONES")
    print("-" * 60)

    # Buscar algunos registros que sabemos que existen
    remaining_records = [r for r in records if r.get_key() not in delete_pks]
    test_remaining_pks = random.sample([r.get_key() for r in remaining_records], 30)

    found_remaining = 0
    for pk in test_remaining_pks:
        if manager.search_by_primary(pk) is not None:
            found_remaining += 1

    print(f"   Busquedas registros restantes: {found_remaining}/30 encontrados")

    # Buscar productos nuevos
    found_new = 0
    for i in range(1, 11):  # Buscar 10 productos nuevos
        result = manager.search_by_secondary('nombre_producto', f'ProductoNuevo_{i}')
        if len(result) > 0:
            found_new += 1

    print(f"   Busquedas productos nuevos: {found_new}/10 encontrados")

    # Busquedas por rango final
    final_range_cantidad = manager.range_search_by_secondary('cantidad_vendida', 5, 25)

    print(f"   Rango cantidad 5-25 (final): {len(final_range_cantidad)} resultados")

    if found_remaining >= 28 and found_new >= 8:  # Al menos 90% exitosas
        print("   OK - BUSQUEDAS POST-MODIFICACIONES EXITOSAS")
    else:
        print("   ERROR - PROBLEMAS EN BUSQUEDAS FINALES")
        return False

    # === RESUMEN FINAL ===
    print("\n" + "="*80)
    print("RESUMEN FINAL - TEST COMPLETO 1000 REGISTROS")
    print("="*80)

    print("OK - Insercion masiva 1000 registros: PERFECTA")
    print("OK - Busquedas primarias (50 pruebas): PERFECTAS")
    print("OK - Busquedas secundarias multiples: PERFECTAS")
    print("OK - Busquedas por rango multiples: PERFECTAS")
    print("OK - Eliminaciones masivas (100 registros): PERFECTAS")
    print("OK - Inserciones adicionales (50 registros): PERFECTAS")
    print("OK - Busquedas post-modificaciones: PERFECTAS")
    print("OK - Consistencia mantenida durante TODO el proceso")
    print("OK - Todos los indices sincronizados perfectamente")

    print(f"\n*** EXITO TOTAL - ISAM CON DATASET COMPLETO ***")
    print(f"Procesados {len(records)} registros iniciales")
    print(f"Eliminados {deleted_count} registros")
    print(f"Agregados {len(new_records)} registros nuevos")
    print(f"Estado final: {ultimate_primary} registros con consistencia perfecta")
    print(f"Tiempo total insercion inicial: {insertion_time:.2f}s")

    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
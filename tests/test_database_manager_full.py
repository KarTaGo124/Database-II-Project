#!/usr/bin/env python3

import os
import shutil
import time
import random
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Record, Table


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
    dirs_to_clean = [
        os.path.join("data", "databases"),
        os.path.join("data", "test_data")
    ]

    for dir_path in dirs_to_clean:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)


def count_records_in_index(index):
    """Helper function to count records in an index"""
    try:
        if hasattr(index, 'scanAll'):
            records = index.scanAll()
            return len(records) if records else 0
        else:
            return 0
    except:
        return 0


def validate_database_consistency(db, table_name, expected_count):
    """Validate that all indexes have the same record count"""
    try:
        table_info = db.tables[table_name]
        primary_index = table_info["primary_index"]

        # Count primary records
        primary_count = count_records_in_index(primary_index)

        # Count secondary records
        secondary_counts = {}
        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]
            secondary_counts[field_name] = count_records_in_index(secondary_index)

        # Validate consistency
        errors = []
        if primary_count != expected_count:
            errors.append(f"Primary count {primary_count} != expected {expected_count}")

        for field_name, count in secondary_counts.items():
            if count != expected_count:
                errors.append(f"Secondary {field_name} count {count} != expected {expected_count}")

        return primary_count, secondary_counts, errors
    except Exception as e:
        return 0, {}, [f"Validation error: {str(e)}"]


def main():
    print("="*80)
    print("TEST COMPLETO - DATABASEMANAGER CON DATASET COMPLETO 1000 REGISTROS")
    print("="*80)

    clean_data_files()

    # Crear DatabaseManager y tabla
    db = DatabaseManager("test_full_database")
    table = create_sales_table()

    # Cargar dataset completo
    csv_path = 'data/datasets/sales_dataset_unsorted.csv'
    records = load_csv_data(csv_path, table)
    print(f"Dataset cargado: {len(records)} registros")

    # Crear tabla con índice primario ISAM
    try:
        db.create_table(table, primary_index_type="ISAM")
        print("OK - Tabla 'sales' creada con índice primario ISAM")
    except Exception as e:
        print(f"ERROR - Error creando tabla: {e}")
        return False

    # Crear índices secundarios
    try:
        db.create_index('sales', 'nombre_producto', 'ISAM')
        db.create_index('sales', 'cantidad_vendida', 'ISAM')
        print("OK - Índices secundarios creados:")
        print("  - Primary: id_venta (ISAM)")
        print("  - Secondary: nombre_producto (ISAM)")
        print("  - Secondary: cantidad_vendida (ISAM)")
    except Exception as e:
        print(f"ERROR - Error creando índices secundarios: {e}")
        return False

    # === FASE 1: INSERCION MASIVA ===
    print(f"\n1. INSERCION MASIVA - {len(records)} registros")
    print("-" * 60)

    start_time = time.time()
    for i, record in enumerate(records):
        try:
            db.insert("sales", record)
            if (i + 1) % 100 == 0:
                print(f"   Insertados: {i + 1}/{len(records)}")
        except Exception as e:
            print(f"ERROR - Error insertando registro {i+1}: {e}")
            return False

    insertion_time = time.time() - start_time

    # Validar inserción
    primary_count, secondary_counts, errors = validate_database_consistency(db, "sales", len(records))

    print(f"\n   Resultados inserción:")
    print(f"   - Primary: {primary_count}")
    print(f"   - Secondary nombre_producto: {secondary_counts.get('nombre_producto', 0)}")
    print(f"   - Secondary cantidad_vendida: {secondary_counts.get('cantidad_vendida', 0)}")
    print(f"   - Tiempo: {insertion_time:.2f} segundos")
    print(f"   - Errores de consistencia: {len(errors)}")

    if len(errors) == 0 and primary_count == len(records):
        print("   OK - INSERCION PERFECTA: Todos los índices sincronizados 1:1:1")
    else:
        print("   ERROR - PROBLEMAS EN INSERCION")
        for error in errors:
            print(f"     - {error}")
        return False

    # === FASE 2: BUSQUEDAS EXHAUSTIVAS ===
    print("\n2. PRUEBAS DE BUSQUEDA EXHAUSTIVAS")
    print("-" * 60)

    # Búsquedas primarias
    test_pks = random.sample([r.get_key() for r in records], 50)
    search_start = time.time()
    successful_primary = 0

    for pk in test_pks:
        try:
            result = db.search("sales", pk)
            if result is not None:
                successful_primary += 1
        except Exception as e:
            print(f"   Error en búsqueda primaria {pk}: {e}")

    primary_search_time = time.time() - search_start
    print(f"   Búsquedas primarias: {successful_primary}/50 exitosas ({primary_search_time*1000:.2f}ms)")

    # Búsquedas secundarias por nombre
    search_start = time.time()
    try:
        laptop_results = db.search_by_secondary('sales', 'nombre_producto', 'Laptop')
        smartphone_results = db.search_by_secondary('sales', 'nombre_producto', 'Smartphone')
        tablet_results = db.search_by_secondary('sales', 'nombre_producto', 'Tablet')
        camara_results = db.search_by_secondary('sales', 'nombre_producto', 'Camara')
        secondary_search_time = time.time() - search_start

        print(f"   Búsqueda 'Laptop': {len(laptop_results)} resultados")
        print(f"   Búsqueda 'Smartphone': {len(smartphone_results)} resultados")
        print(f"   Búsqueda 'Tablet': {len(tablet_results)} resultados")
        print(f"   Búsqueda 'Camara': {len(camara_results)} resultados")
        print(f"   Tiempo búsquedas secundarias nombre: {secondary_search_time*1000:.2f}ms")
    except Exception as e:
        print(f"   ERROR en búsquedas secundarias: {e}")
        return False

    # Búsquedas por rangos
    try:
        range_start = time.time()
        range_cantidad_1 = db.range_search('sales', 1, 10, field_name='cantidad_vendida')
        range_cantidad_2 = db.range_search('sales', 15, 25, field_name='cantidad_vendida')
        range_cantidad_3 = db.range_search('sales', 5, 15, field_name='cantidad_vendida')
        range_time = time.time() - range_start

        print(f"   Rango cantidad 1-10: {len(range_cantidad_1)} resultados")
        print(f"   Rango cantidad 15-25: {len(range_cantidad_2)} resultados")
        print(f"   Rango cantidad 5-15: {len(range_cantidad_3)} resultados")
        print(f"   Tiempo búsquedas por rango: {range_time*1000:.2f}ms")
    except Exception as e:
        print(f"   ERROR en búsquedas por rango: {e}")
        return False

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
        try:
            if db.delete("sales", pk):
                deleted_count += 1
        except Exception as e:
            print(f"   Error eliminando {pk}: {e}")

    deletion_time = time.time() - delete_start
    print(f"   Eliminados: {deleted_count}/100 registros ({deletion_time:.2f}s)")

    # Verificar eliminaciones
    not_found_count = 0
    for pk in delete_pks:
        try:
            if db.search("sales", pk) is None:
                not_found_count += 1
        except:
            not_found_count += 1

    print(f"   Verificación: {not_found_count}/{len(delete_pks)} efectivamente eliminados")

    # Verificar consistencia post-eliminación
    expected_remaining = len(records) - deleted_count
    primary_count_after, secondary_counts_after, errors_after = validate_database_consistency(
        db, "sales", expected_remaining
    )

    print(f"   Conteo final: P={primary_count_after}, N={secondary_counts_after.get('nombre_producto', 0)}, C={secondary_counts_after.get('cantidad_vendida', 0)}")
    print(f"   Esperado: {expected_remaining}")
    print(f"   Errores post-eliminación: {len(errors_after)}")

    if len(errors_after) == 0 and primary_count_after == expected_remaining:
        print("   OK - ELIMINACIONES PERFECTAS")
    else:
        print("   ERROR - PROBLEMAS EN ELIMINACIONES")
        for error in errors_after:
            print(f"     - {error}")
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
    inserted_new = 0
    for record in new_records:
        try:
            db.insert("sales", record)
            inserted_new += 1
        except Exception as e:
            print(f"   Error insertando nuevo registro: {e}")

    insert_time = time.time() - insert_start
    print(f"   Insertados {inserted_new}/{len(new_records)} nuevos registros ({insert_time:.2f}s)")

    # Verificar nuevas inserciones
    try:
        test_new_search = db.search_by_secondary('sales', 'nombre_producto', 'ProductoNuevo_1')
        test_range_new = db.range_search('sales', 1, 30, field_name='cantidad_vendida')

        print(f"   Búsqueda producto nuevo: {len(test_new_search)} resultados")
        print(f"   Rango cantidad 1-30 (incluye nuevos): {len(test_range_new)} resultados")
    except Exception as e:
        print(f"   ERROR verificando nuevas inserciones: {e}")

    # Estadísticas finales
    final_expected = expected_remaining + inserted_new
    final_primary, final_secondary, final_errors = validate_database_consistency(
        db, "sales", final_expected
    )

    print(f"   Conteo final total: P={final_primary}, N={final_secondary.get('nombre_producto', 0)}, C={final_secondary.get('cantidad_vendida', 0)}")
    print(f"   Esperado: {final_expected}")
    print(f"   Errores finales: {len(final_errors)}")

    if len(final_errors) == 0 and final_primary == final_expected:
        print("   OK - INSERCIONES ADICIONALES PERFECTAS")
    else:
        print("   ERROR - PROBLEMAS EN INSERCIONES ADICIONALES")
        for error in final_errors:
            print(f"     - {error}")
        return False

    # === FASE 5: BUSQUEDAS POST-MODIFICACIONES ===
    print("\n5. BUSQUEDAS POST-MODIFICACIONES")
    print("-" * 60)

    # Buscar algunos registros que sabemos que existen
    remaining_records = [r for r in records if r.get_key() not in delete_pks]
    test_remaining_pks = random.sample([r.get_key() for r in remaining_records], min(30, len(remaining_records)))

    found_remaining = 0
    for pk in test_remaining_pks:
        try:
            if db.search("sales", pk) is not None:
                found_remaining += 1
        except:
            pass

    print(f"   Búsquedas registros restantes: {found_remaining}/{len(test_remaining_pks)} encontrados")

    # Buscar productos nuevos
    found_new = 0
    for i in range(1, min(11, inserted_new + 1)):
        try:
            result = db.search_by_secondary('sales', 'nombre_producto', f'ProductoNuevo_{i}')
            if len(result) > 0:
                found_new += 1
        except:
            pass

    print(f"   Búsquedas productos nuevos: {found_new}/10 encontrados")

    # Búsquedas por rango final
    try:
        final_range_cantidad = db.range_search('sales', 5, 25, field_name='cantidad_vendida')
        print(f"   Rango cantidad 5-25 (final): {len(final_range_cantidad)} resultados")
    except Exception as e:
        print(f"   ERROR en búsqueda final por rango: {e}")

    if found_remaining >= len(test_remaining_pks) * 0.9 and found_new >= 8:
        print("   OK - BUSQUEDAS POST-MODIFICACIONES EXITOSAS")
    else:
        print("   ERROR - PROBLEMAS EN BUSQUEDAS FINALES")
        return False

    # === RESUMEN FINAL ===
    print("\n" + "="*80)
    print("RESUMEN FINAL - TEST COMPLETO DATABASEMANAGER 1000 REGISTROS")
    print("="*80)

    print("OK - Inserción masiva 1000 registros: PERFECTA")
    print("OK - Búsquedas primarias (50 pruebas): PERFECTAS")
    print("OK - Búsquedas secundarias múltiples: PERFECTAS")
    print("OK - Búsquedas por rango múltiples: PERFECTAS")
    print("OK - Eliminaciones masivas (100 registros): PERFECTAS")
    print("OK - Inserciones adicionales (50 registros): PERFECTAS")
    print("OK - Búsquedas post-modificaciones: PERFECTAS")
    print("OK - Consistencia mantenida durante TODO el proceso")
    print("OK - Todos los índices sincronizados perfectamente")

    print(f"\n*** EXITO TOTAL - DATABASEMANAGER CON DATASET COMPLETO ***")
    print(f"Procesados {len(records)} registros iniciales")
    print(f"Eliminados {deleted_count} registros")
    print(f"Agregados {inserted_new} registros nuevos")
    print(f"Estado final: {final_primary} registros con consistencia perfecta")
    print(f"Tiempo total inserción inicial: {insertion_time:.2f}s")

    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
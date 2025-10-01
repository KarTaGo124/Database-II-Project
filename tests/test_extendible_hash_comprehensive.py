#!/usr/bin/env python3

import os
import shutil
import sys
import time
import csv
sys.path.append('.')

from sql_parser.interface import execute_sql
from indexes.core.database_manager import DatabaseManager

def test_extendible_hash_comprehensive():
    print("=== Test Integral Extendible Hash ===")
    print("Sequential (primary) + Extendible Hash (secondary)")

    test_dir = "test_extendible_hash"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    data_path = os.path.join("data", "databases", test_dir)
    if os.path.exists(data_path):
        shutil.rmtree(data_path)

    try:
        db_manager = DatabaseManager(test_dir)

        print("\n1. CREACIÓN DE TABLA CON SEQUENTIAL COMO PRIMARY")
        result = execute_sql(db_manager, "CREATE TABLE productos (id INT KEY INDEX SEQUENTIAL, nombre VARCHAR[30], categoria VARCHAR[20], precio FLOAT, stock INT);")
        print(f"Resultado: {result}")

        print("\n2. CARGA DATOS DESDE CSV")
        csv_path = "data/datasets/sales_dataset_unsorted.csv"
        loaded_records = 0
        failed_records = 0

        with open(csv_path, 'r', encoding='utf-8-sig') as file:
            csv_reader = csv.reader(file, delimiter=';')
            next(csv_reader)

            for i, row in enumerate(csv_reader):
                if len(row) >= 5 and loaded_records < 200:
                    try:
                        id_producto = int(row[0])
                        nombre = row[1][:30]
                        categoria = "Electronica" if i % 3 == 0 else "Hogar" if i % 3 == 1 else "Deportes"
                        precio = float(row[3])
                        stock = int(row[2]) if int(row[2]) > 0 else 1

                        sql = f'INSERT INTO productos VALUES ({id_producto}, "{nombre}", "{categoria}", {precio}, {stock});'
                        result = execute_sql(db_manager, sql)

                        if "OK" in str(result):
                            loaded_records += 1
                        else:
                            failed_records += 1

                        if (loaded_records + failed_records) % 50 == 0:
                            print(f"Procesados: {loaded_records + failed_records} registros")

                    except Exception as e:
                        failed_records += 1
                        if failed_records <= 3:
                            print(f"Error procesando fila {i+1}: {e}")

        print(f"Carga completada: {loaded_records} registros cargados, {failed_records} fallidos")

        print("\n3. VERIFICACIÓN DE DATOS CARGADOS")
        result = execute_sql(db_manager, "SELECT * FROM productos WHERE id = 25;")
        print(f"Búsqueda por primary key (25): {len(result) if isinstance(result, list) else result}")

        print("\n4. CREACIÓN DE ÍNDICES EXTENDIBLE HASH")

        print("4.1 Índice HASH en categoria...")
        result = execute_sql(db_manager, "CREATE INDEX ON productos (categoria) USING HASH;")
        print(f"Resultado: {result}")

        print("4.2 Índice HASH en stock...")
        result = execute_sql(db_manager, "CREATE INDEX ON productos (stock) USING HASH;")
        print(f"Resultado: {result}")

        print("\n5. BÚSQUEDAS CON ÍNDICES HASH")

        print("5.1 Búsqueda por categoria (HASH)...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE categoria = "Electronica";')
        print(f"Electronica: {len(result) if isinstance(result, list) else result} resultados")
        if isinstance(result, list) and len(result) > 0:
            first_result = result[0]
            if hasattr(first_result, 'nombre'):
                print(f"Primer resultado: {first_result.nombre}, stock={first_result.stock}")
            else:
                print(f"Primer resultado: {first_result}")

        print("5.2 Búsqueda por stock (HASH)...")
        result = execute_sql(db_manager, "SELECT * FROM productos WHERE stock = 1;")
        print(f"Stock=1: {len(result) if isinstance(result, list) else result} resultados")

        result = execute_sql(db_manager, "SELECT * FROM productos WHERE stock = 2;")
        print(f"Stock=2: {len(result) if isinstance(result, list) else result} resultados")

        print("\n6. INSERCIONES ADICIONALES PARA FORZAR SPLITS")

        print("6.1 Insertando registros concentrados en misma categoria...")
        for i in range(50):
            new_id = 9000 + i
            sql = f'INSERT INTO productos VALUES ({new_id}, "Producto{i}", "Electronica", {100.0 + i}, {i % 5 + 1});'
            result = execute_sql(db_manager, sql)
            if "OK" not in str(result):
                print(f"Error insertando {new_id}: {result}")

        print("6.2 Verificando distribución después de inserciones...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE categoria = "Electronica";')
        print(f"Total Electronica después de inserciones: {len(result) if isinstance(result, list) else result}")

        print("\n7. ELIMINACIONES MASIVAS PARA PROBAR MERGE")

        print("7.1 Eliminando registros con stock = 1...")
        result = execute_sql(db_manager, "DELETE FROM productos WHERE stock = 1;")
        print(f"DELETE stock=1 resultado: {result}")

        print("7.2 Verificando después de eliminación...")
        result = execute_sql(db_manager, "SELECT * FROM productos WHERE stock = 1;")
        print(f"Verificación stock=1: {len(result) if isinstance(result, list) else result}")

        print("7.3 Eliminando más registros para forzar merge...")
        result = execute_sql(db_manager, "DELETE FROM productos WHERE stock = 2;")
        print(f"DELETE stock=2 resultado: {result}")

        result = execute_sql(db_manager, "DELETE FROM productos WHERE stock = 3;")
        print(f"DELETE stock=3 resultado: {result}")

        print("\n8. ELIMINACIONES POR CATEGORIA")

        print("8.1 Eliminando categoría Hogar...")
        result = execute_sql(db_manager, 'DELETE FROM productos WHERE categoria = "Hogar";')
        print(f"DELETE Hogar resultado: {result}")

        print("8.2 Verificando eliminación...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE categoria = "Hogar";')
        print(f"Verificación Hogar: {len(result) if isinstance(result, list) else result}")

        print("\n9. CONTEO FINAL Y DISTRIBUCIÓN")

        result = execute_sql(db_manager, "SELECT * FROM productos;")
        total_records = len(result) if isinstance(result, list) else 0
        print(f"Total registros restantes: {total_records}")

        categorias = ["Electronica", "Deportes"]
        for cat in categorias:
            result = execute_sql(db_manager, f'SELECT * FROM productos WHERE categoria = "{cat}";')
            count = len(result) if isinstance(result, list) else 0
            print(f"Categoria {cat}: {count} registros")

        print("\n10. PRUEBAS DE RENDIMIENTO")

        start_time = time.time()
        for i in range(20):
            result = execute_sql(db_manager, 'SELECT * FROM productos WHERE categoria = "Electronica";')
        end_time = time.time()
        print(f"20 búsquedas por categoria (HASH): {(end_time - start_time)*1000:.2f}ms")

        start_time = time.time()
        for i in range(20):
            result = execute_sql(db_manager, f"SELECT * FROM productos WHERE stock = {(i % 3) + 4};")
        end_time = time.time()
        print(f"20 búsquedas por stock (HASH): {(end_time - start_time)*1000:.2f}ms")

        print("\n11. VERIFICACIÓN FINAL DE ARCHIVOS")
        full_path = os.path.join("data", "databases", test_dir)
        files = []
        if os.path.exists(full_path):
            for root, dirs, filenames in os.walk(full_path):
                for filename in filenames:
                    if filename.endswith(('.dat', '.dir', '.bkt')):
                        rel_path = os.path.relpath(os.path.join(root, filename), full_path)
                        files.append(rel_path)
        files.sort()
        print(f"Archivos generados: {files}")

        print("\n12. ELIMINACIÓN DE ÍNDICES")

        print("12.1 Eliminando índice categoria...")
        result = execute_sql(db_manager, "DROP INDEX categoria;")
        print(f"DROP INDEX categoria: {result}")

        print("12.2 Verificando búsqueda sin índice...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE categoria = "Electronica";')
        print(f"Búsqueda sin HASH: {len(result) if isinstance(result, list) else result}")

        print(f"\n13. VERIFICACIÓN POST-DROP INDEX")
        print("Verificando que archivos fueron eliminados por drop_index...")
        full_path = os.path.join("data", "databases", test_dir)
        remaining_files = []
        if os.path.exists(full_path):
            for root, dirs, filenames in os.walk(full_path):
                for filename in filenames:
                    if filename.endswith(('.dat', '.dir', '.bkt')):
                        rel_path = os.path.relpath(os.path.join(root, filename), full_path)
                        remaining_files.append(rel_path)
        remaining_files.sort()
        print(f"Archivos restantes después de DROP INDEX: {remaining_files}")

        expected_files = ['primary\\aux.dat', 'primary\\main.dat', 'secondary\\productos_stock_stock.bkt', 'secondary\\productos_stock_stock.dir']
        if set(remaining_files) == set(expected_files):
            print("[OK] DROP INDEX funciono correctamente - solo quedan archivos del indice stock")
        else:
            print("[ERROR] DROP INDEX no funciono como esperado")

        print("\n[OK] Test integral de Extendible Hash completado exitosamente!")
        print("NOTA: Archivos del test quedan para verificación manual del DROP INDEX")

    except Exception as e:
        print(f"\n[ERROR] Error en el test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_extendible_hash_comprehensive()
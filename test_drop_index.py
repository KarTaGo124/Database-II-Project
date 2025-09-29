#!/usr/bin/env python3

import os
import shutil
import sys
sys.path.append('.')

from sql_parser.interface import execute_sql
from indexes.core.database_manager import DatabaseManager

def test_drop_index_files():
    print("=== Test de limpieza de archivos DROP INDEX ===")

    test_dir = "test_drop_files"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    try:
        db_manager = DatabaseManager(test_dir)

        print("\n1. CREATE TABLE...")
        result = execute_sql(db_manager, "CREATE TABLE test (id INT KEY INDEX ISAM, precio FLOAT);")
        print(f"Resultado: {result}")

        print("\n2. Insertando datos...")
        result = execute_sql(db_manager, 'INSERT INTO test VALUES (1, 100.0);')
        print(f"Resultado: {result}")

        print("\n3. Creando indice FLOAT...")
        result = execute_sql(db_manager, "CREATE INDEX ON test (precio) USING ISAM;")
        print(f"Resultado: {result}")

        print("\n4. Verificando archivos creados...")
        import time
        time.sleep(0.1)  # Esperar a que se escriban los archivos

        files_before = []
        full_path = os.path.join("data", "databases", test_dir)
        print(f"Buscando en: {full_path}")

        if os.path.exists(full_path):
            for root, dirs, filenames in os.walk(full_path):
                for filename in filenames:
                    if filename.endswith(('.dat')):
                        rel_path = os.path.relpath(os.path.join(root, filename), full_path)
                        files_before.append(rel_path)
        files_before.sort()
        print(f"Archivos antes de DROP: {files_before}")

        print("\n5. DROP INDEX...")
        result = execute_sql(db_manager, "DROP INDEX precio;")
        print(f"Resultado: {result}")

        print("\n6. Verificando archivos despues de DROP...")
        time.sleep(0.1)  # Esperar a que se eliminen los archivos

        files_after = []
        if os.path.exists(full_path):
            for root, dirs, filenames in os.walk(full_path):
                for filename in filenames:
                    if filename.endswith(('.dat')):
                        rel_path = os.path.relpath(os.path.join(root, filename), full_path)
                        files_after.append(rel_path)
        files_after.sort()
        print(f"Archivos despues de DROP: {files_after}")

        print("\n7. Verificando eliminación de archivos del índice secundario:")
        secondary_files_before = [f for f in files_before if 'secondary' in f]
        secondary_files_after = [f for f in files_after if 'secondary' in f]

        print(f"Archivos secundarios antes: {secondary_files_before}")
        print(f"Archivos secundarios después: {secondary_files_after}")

        secondary_not_removed = [f for f in secondary_files_before if f in secondary_files_after]

        if secondary_not_removed:
            print(f"ERROR: {len(secondary_not_removed)} archivos del índice secundario no se eliminaron: {secondary_not_removed}")
        else:
            print("OK: Todos los archivos del índice secundario se eliminaron correctamente")

        primary_files = [f for f in files_after if 'primary' in f]
        print(f"Archivos primarios que permanecen (correcto): {primary_files}")

        print("\n[OK] Test completado!")

    except Exception as e:
        print(f"\n[ERROR] Error en el test: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_drop_index_files()
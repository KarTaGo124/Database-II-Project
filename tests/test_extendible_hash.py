#!/usr/bin/env python3

import os
import shutil
import sys
sys.path.append('.')

from sql_parser.interface import execute_sql
from indexes.core.database_manager import DatabaseManager

def test_extendible_hash():
    print("=== Test de Extendible Hash ===")

    test_dir = "test_extendible_hash"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    # Limpiar cualquier directorio de datos residual
    data_path = os.path.join("data", "databases", test_dir)
    if os.path.exists(data_path):
        shutil.rmtree(data_path)

    try:
        db_manager = DatabaseManager(test_dir)

        print("\n1. CREATE TABLE...")
        result = execute_sql(db_manager, "CREATE TABLE productos (id INT KEY INDEX ISAM, nombre VARCHAR[20]);")
        print(f"Resultado: {result}")

        print("\n2. INSERT datos iniciales...")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (1, "Laptop");')
        print(f"Resultado: {result}")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (2, "Mouse");')
        print(f"Resultado: {result}")

        print("\n3. CREATE INDEX HASH...")
        result = execute_sql(db_manager, "CREATE INDEX ON productos (nombre) USING HASH;")
        print(f"Resultado: {result}")

        print("\n4. SELECT por HASH...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Mouse";')
        print(f"Resultado: {result}")

        print("\n5. INSERT después del índice...")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (3, "Monitor");')
        print(f"Resultado: {result}")

        print("\n6. SELECT datos nuevos...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Monitor";')
        print(f"Resultado: {result}")

        print("\n7. DROP INDEX HASH...")
        result = execute_sql(db_manager, "DROP INDEX nombre;")
        print(f"Resultado: {result}")

        print("\n8. Verificando archivos generados...")
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

        print("\n[OK] Test de Extendible Hash completado exitosamente!")

    except Exception as e:
        print(f"\n[ERROR] Error en el test: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_extendible_hash()
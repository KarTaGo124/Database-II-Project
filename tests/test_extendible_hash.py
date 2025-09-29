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

    try:
        db_manager = DatabaseManager(test_dir)

        print("\n1. CREATE TABLE simple...")
        result = execute_sql(db_manager, "CREATE TABLE test (id INT KEY INDEX ISAM, nombre VARCHAR[50]);")
        print(f"Resultado: {result}")

        print("\n2. Insertando datos...")
        result = execute_sql(db_manager, 'INSERT INTO test VALUES (1, "Laptop");')
        print(f"Resultado: {result}")
        result = execute_sql(db_manager, 'INSERT INTO test VALUES (2, "Mouse");')
        print(f"Resultado: {result}")

        print("\n3. Creando indice HASH en VARCHAR...")
        result = execute_sql(db_manager, "CREATE INDEX idx_nombre ON test (nombre) USING HASH;")
        print(f"Resultado: {result}")

        print("\n4. Busqueda por indice HASH...")
        result = execute_sql(db_manager, 'SELECT * FROM test WHERE nombre = "Mouse";')
        print(f"Resultado: {result}")

        print("\n5. Insertando mas datos...")
        result = execute_sql(db_manager, 'INSERT INTO test VALUES (3, "Monitor");')
        print(f"Resultado: {result}")

        print("\n6. Busqueda en datos nuevos...")
        result = execute_sql(db_manager, 'SELECT * FROM test WHERE nombre = "Monitor";')
        print(f"Resultado: {result}")

        print("\n7. DROP INDEX HASH...")
        result = execute_sql(db_manager, "DROP INDEX idx_nombre;")
        print(f"Resultado: {result}")

        print("\n8. Verificando archivos generados...")
        files = []
        for root, dirs, filenames in os.walk(test_dir):
            for filename in filenames:
                if filename.endswith(('.dat', '.dir', '.bkt')):
                    rel_path = os.path.relpath(os.path.join(root, filename), test_dir)
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
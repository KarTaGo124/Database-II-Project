#!/usr/bin/env python3

import os
import shutil
import sys
sys.path.append('.')

from sql_parser.interface import execute_sql
from indexes.core.database_manager import DatabaseManager

def test_index_operations():
    print("=== Test de Operaciones de Indices ===")

    test_dir = "test_index_ops"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    try:
        db_manager = DatabaseManager(test_dir)

        print("\\n1. CREATE TABLE sin indices secundarios...")
        result = execute_sql(db_manager, "CREATE TABLE productos (id INT KEY INDEX ISAM, nombre VARCHAR[50], precio FLOAT);")
        print(f"Resultado: {result}")

        print("\\n2. Insertando datos iniciales...")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (1, "Laptop", 999.99);')
        print(f"Resultado: {result}")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (2, "Mouse", 25.50);')
        print(f"Resultado: {result}")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (3, "Teclado", 75.00);')
        print(f"Resultado: {result}")

        print("\\n3. Verificando datos insertados...")
        result = execute_sql(db_manager, "SELECT * FROM productos;")
        print(f"Resultado: {len(result)} registros encontrados")

        print("\\n4. Creando indice secundario en tabla con datos...")
        result = execute_sql(db_manager, "CREATE INDEX idx_nombre ON productos (nombre) USING ISAM;")
        print(f"Resultado: {result}")

        print("\\n5. Verificando busqueda por indice secundario...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Mouse";')
        print(f"Resultado: {result}")

        print("\\n6. Insertando mas datos despues de crear indice...")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (4, "Monitor", 300.00);')
        print(f"Resultado: {result}")

        print("\\n7. Verificando busqueda en datos nuevos...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Monitor";')
        print(f"Resultado: {result}")

        print("\\n8. Busqueda por nombre...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Monitor";')
        print(f"Resultado: {result}")

        print("\\n9. Verificando archivos generados...")
        files = []
        for root, dirs, filenames in os.walk(test_dir):
            for filename in filenames:
                if filename.endswith('.dat'):
                    rel_path = os.path.relpath(os.path.join(root, filename), test_dir)
                    files.append(rel_path)
        files.sort()
        print(f"Archivos .dat generados: {files}")

        print("\\n10. DROP INDEX por nombre...")
        result = execute_sql(db_manager, "DROP INDEX idx_nombre;")
        print(f"Resultado: {result}")

        print("\\n11. Verificando archivos despues de DROP INDEX...")
        files_after_drop = []
        for root, dirs, filenames in os.walk(test_dir):
            for filename in filenames:
                if filename.endswith('.dat'):
                    rel_path = os.path.relpath(os.path.join(root, filename), test_dir)
                    files_after_drop.append(rel_path)
        files_after_drop.sort()
        print(f"Archivos .dat restantes: {files_after_drop}")

        print("\\n12. Intentando usar indice eliminado...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Mouse";')
        print(f"Resultado (deberia usar scan completo): {len(result) if isinstance(result, list) else result}")

        print("\\n13. Verificando busqueda por primary key...")
        result = execute_sql(db_manager, "SELECT * FROM productos WHERE id = 1;")
        print(f"Resultado: {result}")

        print("\\n14. DROP TABLE completa...")
        result = execute_sql(db_manager, "DROP TABLE productos;")
        print(f"Resultado: {result}")

        print("\\n15. Verificando limpieza total...")
        remaining_files = []
        for root, dirs, filenames in os.walk(test_dir):
            for filename in filenames:
                remaining_files.append(os.path.relpath(os.path.join(root, filename), test_dir))
        print(f"Archivos restantes: {remaining_files}")

        print("\\n16. Recreando tabla con indices en CREATE TABLE...")
        result = execute_sql(db_manager, "CREATE TABLE productos (id INT KEY INDEX ISAM, nombre VARCHAR[50] INDEX ISAM, categoria VARCHAR[20]);")
        print(f"Resultado: {result}")

        print("\\n17. Insertando datos en tabla con indices predefinidos...")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (1, "Laptop", "Electronica");')
        print(f"Resultado: {result}")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (2, "Mouse", "Accesorios");')
        print(f"Resultado: {result}")

        print("\\n18. Verificando funcionamiento de indices predefinidos...")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Mouse";')
        print(f"Resultado: {result}")

        result = execute_sql(db_manager, "SELECT * FROM productos WHERE id = 1;")
        print(f"Resultado: {result}")

        print("\\n[OK] Test de operaciones de indices completado exitosamente!")

    except Exception as e:
        print(f"\\n[ERROR] Error en el test: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_index_operations()
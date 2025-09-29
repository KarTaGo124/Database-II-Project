#!/usr/bin/env python3

import os
import shutil
from sql_parser.interface import execute_sql
from indexes.core.database_manager import DatabaseManager

def test_sql_integration():
    print("=== Prueba de Integración SQL Parser + DatabaseManager ===")

    # Limpiar directorio de prueba
    test_dir = "test_sql_db"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    try:
        # Inicializar motor SQL
        db_manager = DatabaseManager(test_dir)

        print("\n1. Creando tabla con ISAM...")
        result = execute_sql(db_manager, "CREATE TABLE productos (id INT KEY INDEX ISAM, nombre VARCHAR[50] INDEX ISAM, precio FLOAT);")
        print(f"Resultado: {result}")

        print("\n2. Insertando datos...")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (1, "Laptop", 999.99);')
        print(f"Resultado: {result}")

        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (2, "Mouse", 25.50);')
        print(f"Resultado: {result}")

        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (3, "Teclado", 75.00);')
        print(f"Resultado: {result}")

        print("\n3. Consultando datos...")
        result = execute_sql(db_manager, "SELECT * FROM productos;")
        print(f"Resultado: {result}")

        print("\n4. Búsqueda por clave primaria...")
        result = execute_sql(db_manager, "SELECT * FROM productos WHERE id = 2;")
        print(f"Resultado: {result}")

        print("\nsss. Búsqueda por clave primaria...")
        result = execute_sql(db_manager, "SELECT * FROM productos WHERE nombre = 'Laptop';")
        print(f"Resultado: {result}")

        print("\n5. Búsqueda por rango...")
        result = execute_sql(db_manager, "SELECT nombre, precio FROM productos WHERE precio BETWEEN 20.0 AND 100.0;")
        print(f"Resultado: {result}")

        
        print("\n7. Eliminando datos...")
        result = execute_sql(db_manager, "DELETE FROM productos WHERE id = 3;")
        print(f"Resultado: {result}")

        print("\n8. Verificando eliminación...")
        result = execute_sql(db_manager, "SELECT * FROM productos;")
        print(f"Resultado: {result}")



        print("\n[OK] Prueba de integración completada exitosamente!")

    except Exception as e:
        print(f"\n[ERROR] Error en la prueba: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Limpiar archivos de prueba
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_sql_integration()
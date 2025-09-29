#!/usr/bin/env python3

import os
import shutil
from sql_parser.parser_sql import parse
from sql_parser.executor import Executor
from indexes.core.database_manager import DatabaseManager

class Engine:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.executor = Executor(db_manager)

    def execute(self, sql: str):
        try:
            plans = parse(sql)
            results = []
            for plan in plans:
                result = self.executor.execute(plan)
                results.append(result)
            return results[0] if len(results) == 1 else results
        except Exception as e:
            return f"ERROR: {e}"

def test_sql_integration():
    print("=== Prueba de Integración SQL Parser + DatabaseManager ===")

    # Limpiar directorio de prueba
    test_dir = "test_sql_db"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    try:
        # Inicializar motor SQL
        db_manager = DatabaseManager(test_dir)
        engine = Engine(db_manager)

        print("\n1. Creando tabla con ISAM...")
        result = engine.execute("CREATE TABLE productos (id INT KEY INDEX ISAM, nombre VARCHAR[50], precio FLOAT);")
        print(f"Resultado: {result}")

        print("\n2. Insertando datos...")
        result = engine.execute('INSERT INTO productos VALUES (1, "Laptop", 999.99);')
        print(f"Resultado: {result}")

        result = engine.execute('INSERT INTO productos VALUES (2, "Mouse", 25.50);')
        print(f"Resultado: {result}")

        result = engine.execute('INSERT INTO productos VALUES (3, "Teclado", 75.00);')
        print(f"Resultado: {result}")

        print("\n3. Consultando datos...")
        result = engine.execute("SELECT * FROM productos;")
        print(f"Resultado: {result}")

        print("\n4. Búsqueda por clave primaria...")
        result = engine.execute("SELECT * FROM productos WHERE id = 2;")
        print(f"Resultado: {result}")

        print("\n5. Búsqueda por rango...")
        result = engine.execute("SELECT nombre, precio FROM productos WHERE precio BETWEEN 20.0 AND 100.0;")
        print(f"Resultado: {result}")

        print("\n6. Creando índice secundario...")
        result = engine.execute("CREATE INDEX idx_precio ON productos (precio) USING ISAM;")
        print(f"Resultado: {result}")

        """
        print("\n7. Explicando plan de ejecución...")
        result = engine.execute("EXPLAIN SELECT * FROM productos WHERE id = 1;")
        print(f"Resultado: {result}")
        """
        
        print("\n8. Eliminando datos...")
        result = engine.execute("DELETE FROM productos WHERE id = 3;")
        print(f"Resultado: {result}")

        print("\n9. Verificando eliminación...")
        result = engine.execute("SELECT * FROM productos;")
        print(f"Resultado: {result}")

        print("\n10. Eliminando índice...")
        result = engine.execute("DROP INDEX idx_precio;")
        print(f"Resultado: {result}")

        print("\n11. Eliminando tabla...")
        result = engine.execute("DROP TABLE productos;")
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
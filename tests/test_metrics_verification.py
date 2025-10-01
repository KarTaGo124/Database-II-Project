#!/usr/bin/env python3

import os
import shutil
import sys
sys.path.append('.')

from sql_parser.interface import execute_sql
from indexes.core.database_manager import DatabaseManager

def test_exact_metrics():
    print("=== Verificación EXACTA de Métricas ===")

    test_dir = "test_metrics"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    data_path = os.path.join("data", "databases", test_dir)
    if os.path.exists(data_path):
        shutil.rmtree(data_path)

    try:
        db_manager = DatabaseManager(test_dir)

        print("\n1. CREAR TABLA Y HASH INDEX")
        execute_sql(db_manager, "CREATE TABLE test (id INT KEY INDEX SEQUENTIAL, categoria VARCHAR[10]);")

        print("\n2. INSERTAR ALGUNOS REGISTROS")
        for i in range(5):
            execute_sql(db_manager, f'INSERT INTO test VALUES ({i}, "cat{i % 2}");')

        print("\n3. CREAR ÍNDICE HASH")
        result = execute_sql(db_manager, "CREATE INDEX ON test (categoria) USING HASH;")
        print(f"Resultado: {result}")

        print("\n4. BÚSQUEDA CON MÉTRICAS DETALLADAS")

        # Obtener referencia al índice para revisar métricas
        table_info = db_manager.tables["test"]
        hash_index = table_info["secondary_indexes"]["categoria"]["index"]

        print("4.1 Búsqueda simple...")
        result = hash_index.search("cat0")
        print(f"Resultado búsqueda: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("\n4.2 Segunda búsqueda...")
        result = hash_index.search("cat1")
        print(f"Resultado búsqueda: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("\n4.3 Búsqueda de clave inexistente...")
        result = hash_index.search("noexiste")
        print(f"Resultado búsqueda: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("\n5. INSERCIÓN CON MÉTRICAS")
        from indexes.core.record import Record

        # Crear registro manualmente para ver métricas exactas
        test_record = Record(
            list_of_types=[("id", "INT", 4), ("categoria", "VARCHAR", 10), ("active", "BOOL", 1)],
            key_field="id"
        )
        test_record.set_values(id=99, categoria="new_cat", active=True)

        print("5.1 Inserción en hash...")
        result = hash_index.insert(test_record)
        print(f"Resultado inserción: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("\n6. ELIMINACIÓN CON MÉTRICAS")
        result = hash_index.delete("new_cat")
        print(f"Resultado eliminación: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("\n[OK] Verificación de métricas completada")

    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_exact_metrics()
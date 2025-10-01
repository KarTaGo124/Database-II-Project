#!/usr/bin/env python3

import os
import shutil
import sys
sys.path.append('..')

from indexes.core.record import Record, Table
from indexes.sequential_file.sequential_file import SequentialFile

def test_sequential_metrics():
    print("=== Verificación EXACTA de Métricas - Sequential File ===")

    test_dir = "test_sequential_metrics"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    try:
        # Crear tabla y sequential file
        table = Table(
            table_name="test",
            sql_fields=[("id", "INT", 4), ("name", "CHAR", 20)],
            key_field="id",
            extra_fields={"active": ("BOOL", 1)}
        )

        main_file = os.path.join(test_dir, "main.dat")
        aux_file = os.path.join(test_dir, "aux.dat")
        seq_file = SequentialFile(main_file, aux_file, table, k_rec=3)

        print("\n1. INSERCIÓN CON MÉTRICAS DETALLADAS")

        # Insertar algunos registros
        for i in range(5):
            record = Record(table.all_fields, table.key_field)
            record.set_values(id=i, name=f"user_{i}", active=True)

            print(f"1.{i+1} Insertando registro {i}...")
            result = seq_file.insert(record)
            print(f"Resultado: {result.data}")
            print(f"READS: {result.disk_reads}")
            print(f"WRITES: {result.disk_writes}")
            print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print(f"\nArchivos después de inserciones:")
        print(f"Main file size: {seq_file.get_file_size(main_file)} records")
        print(f"Aux file size: {seq_file.get_file_size(aux_file)} records")

        print("\n2. BÚSQUEDA CON MÉTRICAS DETALLADAS")

        print("2.1 Búsqueda de clave existente...")
        result = seq_file.search(2)
        print(f"Resultado: {result.data.name if result.data else None}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("2.2 Búsqueda de clave inexistente...")
        result = seq_file.search(99)
        print(f"Resultado: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("\n3. ELIMINACIÓN CON MÉTRICAS DETALLADAS")

        print("3.1 Eliminando registro existente...")
        result = seq_file.delete(1)
        print(f"Resultado: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("3.2 Verificando eliminación...")
        result = seq_file.search(1)
        print(f"Búsqueda del eliminado: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")

        print("\n4. REBUILD CON MÉTRICAS DETALLADAS")

        print("4.1 Estado antes de rebuild...")
        print(f"Main file size: {seq_file.get_file_size(main_file)} records")
        print(f"Aux file size: {seq_file.get_file_size(aux_file)} records")

        print("4.2 Ejecutando rebuild manual (sin tracking)...")
        seq_file.rebuild()
        print("Rebuild manual completado")

        print("4.3 Estado después de rebuild manual...")
        print(f"Main file size: {seq_file.get_file_size(main_file)} records")
        print(f"Aux file size: {seq_file.get_file_size(aux_file)} records")

        print("4.4 Verificando datos después de rebuild...")
        result = seq_file.search(2)
        print(f"Búsqueda después de rebuild: {result.data.name if result.data else None}")
        print(f"READS: {result.disk_reads}")

        print("4.5 Provocando rebuild automático con tracking...")
        for i in range(10, 15):
            record = Record(table.all_fields, table.key_field)
            record.set_values(id=i, name=f"user_{i}", active=True)
            result = seq_file.insert(record)
            if result.disk_reads > 5:
                print(f"Insert {i} activó rebuild automático:")
                print(f"READS: {result.disk_reads}, WRITES: {result.disk_writes}")
                break

        print("\n[OK] Verificación de métricas Sequential File completada")

    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_sequential_metrics()
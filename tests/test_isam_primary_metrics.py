#!/usr/bin/env python3

import os
import shutil
import sys
sys.path.append('..')

from indexes.core.record import Record, Table
from indexes.isam.primary import ISAMPrimaryIndex

def test_isam_primary_metrics():
    print("=== Verificación EXACTA de Métricas - ISAM Primary ===")

    test_dir = "test_isam_primary_metrics"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    try:
        table = Table(
            table_name="test",
            sql_fields=[("id", "INT", 4), ("name", "CHAR", 20)],
            key_field="id",
            extra_fields={"active": ("BOOL", 1)}
        )

        filename = os.path.join(test_dir, "data.dat")
        isam = ISAMPrimaryIndex(table, filename)

        print("\n1. INSERCIÓN CON MÉTRICAS DETALLADAS")

        for i in range(10):
            record = Record(table.all_fields, table.key_field)
            record.set_values(id=i, name=f"user_{i}", active=True)

            print(f"1.{i+1} Insertando registro {i}...")
            result = isam.insert(record)
            print(f"Resultado: {result.data}")
            print(f"READS: {result.disk_reads}")
            print(f"WRITES: {result.disk_writes}")
            print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("\n2. BÚSQUEDA CON MÉTRICAS DETALLADAS")

        print("2.1 Búsqueda de clave existente (inicio)...")
        result = isam.search(0)
        print(f"Resultado: {result.data.name if result.data else None}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("2.2 Búsqueda de clave existente (medio)...")
        result = isam.search(5)
        print(f"Resultado: {result.data.name if result.data else None}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("2.3 Búsqueda de clave existente (final)...")
        result = isam.search(9)
        print(f"Resultado: {result.data.name if result.data else None}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("2.4 Búsqueda de clave inexistente...")
        result = isam.search(99)
        print(f"Resultado: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("\n3. ELIMINACIÓN CON MÉTRICAS DETALLADAS")

        print("3.1 Eliminando registro existente...")
        result = isam.delete(5)
        print(f"Resultado: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("3.2 Verificando eliminación...")
        result = isam.search(5)
        print(f"Búsqueda del eliminado: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")

        print("3.3 Eliminando clave inexistente...")
        result = isam.delete(99)
        print(f"Resultado: {result.data}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")

        print("\n4. RANGE SEARCH CON MÉTRICAS DETALLADAS")

        print("4.1 Range search (2-7)...")
        result = isam.range_search(2, 7)
        print(f"Resultado: {len(result.data)} registros encontrados")
        if result.data:
            print(f"Primer resultado: {result.data[0].name if hasattr(result.data[0], 'name') else result.data[0]}")
            print(f"Último resultado: {result.data[-1].name if hasattr(result.data[-1], 'name') else result.data[-1]}")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")
        print(f"TIEMPO: {result.execution_time_ms:.2f}ms")

        print("4.2 Range search vacío (50-60)...")
        result = isam.range_search(50, 60)
        print(f"Resultado: {len(result.data)} registros encontrados")
        print(f"READS: {result.disk_reads}")
        print(f"WRITES: {result.disk_writes}")

        print("\n5. REBUILD VERIFICATION")
        print("5.1 Estado antes de rebuild...")
        result = isam.search(0)
        print(f"Búsqueda antes de rebuild - READS: {result.disk_reads}, WRITES: {result.disk_writes}")

        print("5.2 Ejecutando rebuild...")
        isam.rebuild()
        print("Rebuild completado")

        print("5.3 Verificando después de rebuild...")
        result = isam.search(0)
        print(f"Búsqueda después de rebuild - READS: {result.disk_reads}, WRITES: {result.disk_writes}")

        result = isam.search(9)
        print(f"Otra búsqueda después de rebuild - READS: {result.disk_reads}, WRITES: {result.disk_writes}")

        print("\n[OK] Verificación de métricas ISAM Primary completada")

    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_isam_primary_metrics()
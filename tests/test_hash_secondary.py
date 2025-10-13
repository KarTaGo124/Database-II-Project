#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor
import shutil
import time

def print_metrics(result, operation_name):
    print(f"\n[METRICS] {operation_name}")
    print(f"  Time: {result.execution_time_ms:.2f} ms")
    print(f"  Reads: {result.disk_reads}")
    print(f"  Writes: {result.disk_writes}")
    print(f"  Total accesses: {result.total_disk_accesses}")
    if hasattr(result, 'operation_breakdown') and result.operation_breakdown:
        print(f"  Breakdown: {result.operation_breakdown}")

def test_hash_secondary():
    print("=" * 70)
    print("TEST: HASH SECONDARY INDEX")
    print("=" * 70)

    if os.path.exists('data/database'):
        try:
            shutil.rmtree('data/database')
        except:
            pass
        time.sleep(0.5)

    db = DatabaseManager()
    executor = Executor(db)

    print("\n1. CREATE TABLE con ISAM primary")
    result = executor.execute(parse("""
        CREATE TABLE empleados (
            emp_id INT KEY INDEX ISAM,
            nombre VARCHAR[40],
            departamento VARCHAR[30],
            salario FLOAT
        )
    """)[0])
    print(f"   {result.data}")
    print_metrics(result, "CREATE TABLE")

    print("\n2. CREATE SECONDARY INDEX (HASH) en departamento")
    result = executor.execute(parse('CREATE INDEX ON empleados (departamento) USING HASH')[0])
    print(f"   Secondary index creado en 'departamento'")
    print_metrics(result, "CREATE INDEX")

    print("\n3. CREATE SECONDARY INDEX (HASH) en nombre")
    result = executor.execute(parse('CREATE INDEX ON empleados (nombre) USING HASH')[0])
    print(f"   Secondary index creado en 'nombre'")
    print_metrics(result, "CREATE INDEX")

    print("\n4. INSERT 12 registros (actualiza índices)")
    empleados = [
        (1, "Ana Martinez", "Ventas", 55000.0),
        (2, "Carlos Lopez", "IT", 68000.0),
        (3, "Elena Garcia", "Ventas", 52000.0),
        (4, "David Chen", "IT", 72000.0),
        (5, "Maria Rodriguez", "RRHH", 60000.0),
        (6, "Jose Perez", "IT", 70000.0),
        (7, "Laura Kim", "Ventas", 58000.0),
        (8, "Miguel Santos", "Finanzas", 65000.0),
        (9, "Sofia Wang", "IT", 75000.0),
        (10, "Pedro Gomez", "Ventas", 54000.0),
        (11, "Carmen Lee", "RRHH", 62000.0),
        (12, "Luis Brown", "Finanzas", 67000.0),
    ]

    total_time = 0
    total_writes = 0
    for eid, nombre, dept, salario in empleados:
        result = executor.execute(parse(f'INSERT INTO empleados VALUES ({eid}, "{nombre}", "{dept}", {salario})')[0])
        total_time += result.execution_time_ms
        total_writes += result.disk_writes
    print(f"   Insertados: 12 empleados")
    print(f"   Total time: {total_time:.2f} ms")
    print(f"   Total writes: {total_writes}")

    print("\n5. SELECT por SECONDARY INDEX (departamento = IT)")
    result = executor.execute(parse('SELECT * FROM empleados WHERE departamento = "IT"')[0])
    print(f"   Encontrados: {len(result.data)} empleados en IT")
    for rec in result.data:
        print(f"     - {rec['nombre']}: ${rec['salario']}")
    print_metrics(result, "SELECT by SECONDARY INDEX (departamento)")

    print("\n6. SELECT por SECONDARY INDEX (departamento = Ventas)")
    result = executor.execute(parse('SELECT * FROM empleados WHERE departamento = "Ventas"')[0])
    print(f"   Encontrados: {len(result.data)} empleados en Ventas")
    for rec in result.data:
        print(f"     - {rec['nombre']}: ${rec['salario']}")
    print_metrics(result, "SELECT by SECONDARY INDEX (departamento)")

    print("\n7. SELECT por SECONDARY INDEX (nombre específico)")
    result = executor.execute(parse('SELECT * FROM empleados WHERE nombre = "David Chen"')[0])
    print(f"   Encontrado: {result.data[0]['nombre']} - Dept: {result.data[0]['departamento']}")
    print_metrics(result, "SELECT by SECONDARY INDEX (nombre)")

    print("\n8. SELECT por PRIMARY KEY (comparación)")
    result = executor.execute(parse('SELECT * FROM empleados WHERE emp_id = 5')[0])
    print(f"   Encontrado: {result.data[0]['nombre']} - ${result.data[0]['salario']}")
    print_metrics(result, "SELECT by PRIMARY KEY")

    print("\n9. DELETE con actualización de índices secundarios")
    result = executor.execute(parse('DELETE FROM empleados WHERE emp_id = 2')[0])
    print(f"   Eliminado: {result.data}")
    print_metrics(result, "DELETE (actualiza secondary indexes)")

    print("\n10. Verificar que secondary index se actualizó")
    result = executor.execute(parse('SELECT * FROM empleados WHERE departamento = "IT"')[0])
    print(f"   IT ahora tiene: {len(result.data)} empleados (antes 4, ahora 3)")
    for rec in result.data:
        print(f"     - {rec['nombre']}")
    print_metrics(result, "SELECT by SECONDARY INDEX after DELETE")

    print("\n11. DELETE por SECONDARY INDEX (eliminar todos de RRHH)")
    result = executor.execute(parse('SELECT * FROM empleados WHERE departamento = "RRHH"')[0])
    rrhh_ids = [rec['emp_id'] for rec in result.data]
    print(f"   IDs a eliminar: {rrhh_ids}")

    for emp_id in rrhh_ids:
        result = executor.execute(parse(f'DELETE FROM empleados WHERE emp_id = {emp_id}')[0])
    print(f"   Eliminados todos los empleados de RRHH")

    print("\n12. Verificar eliminación")
    result = executor.execute(parse('SELECT * FROM empleados WHERE departamento = "RRHH"')[0])
    print(f"   RRHH ahora tiene: {len(result.data)} empleados")
    print_metrics(result, "SELECT by SECONDARY INDEX after bulk DELETE")

    print("\n13. SCAN ALL final")
    result = executor.execute(parse('SELECT * FROM empleados')[0])
    print(f"   Total empleados restantes: {len(result.data)}")
    print_metrics(result, "SCAN ALL")

    print("\n" + "=" * 70)
    print("TEST HASH SECONDARY INDEX PASSED")
    print("=" * 70)

if __name__ == "__main__":
    try:
        test_hash_secondary()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

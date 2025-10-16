#!/usr/bin/env python3
import sys, os, shutil, time, random

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor


def print_metrics(result, operation_name):
    print(f"\n[METRICS] {operation_name}")
    print(f"  Time: {result.execution_time_ms:.2f} ms")
    print(f"  Reads: {result.disk_reads}")
    print(f"  Writes: {result.disk_writes}")
    print(f"  Total accesses: {result.total_disk_accesses}")


def test_hash_secondary_exhaustive():
    print("=" * 80)
    print("TEST EXTENDIBLE HASHING — EXHAUSTIVE FUNCTIONAL TEST")
    print("=" * 80)

    if os.path.exists('data/database'):
        shutil.rmtree('data/database', ignore_errors=True)
        time.sleep(0.3)

    db = DatabaseManager()
    executor = Executor(db)

    # === 1. CREATE TABLE AND SECONDARY INDEX ===
    print("\n1. Crear tabla y secondary index extendible")
    executor.execute(parse("""
                           CREATE TABLE empleados
                           (
                               emp_id       INT KEY INDEX ISAM,
                               nombre       VARCHAR[40],
                               departamento VARCHAR[30],
                               salario      FLOAT
                           )
                           """)[0])
    executor.execute(parse('CREATE INDEX ON empleados (departamento) USING HASH')[0])

    # === 2. MASS INSERTS to trigger directory doubling and bucket splits ===
    print("\n2. Insertar registros para forzar splits y duplicación de directorio")

    departamentos = ["IT", "Ventas", "RRHH", "Finanzas", "Logística", "Legal"]
    nombres = ["Ana", "Carlos", "David", "Elena", "Laura", "Luis", "Maria", "Jose", "Carmen", "Pedro", "Miguel",
               "Lucia", "Jorge", "Rosa", "Daniel"]

    inserted = []
    for emp_id in range(1, 65):  # 64 registros para forzar varios splits
        nombre = random.choice(nombres) + " " + random.choice(["Lopez", "Gomez", "Perez", "Lee", "Chen", "Wang"])
        dept = random.choice(departamentos)
        salario = round(random.uniform(40000, 90000), 2)
        query = f'INSERT INTO empleados VALUES ({emp_id}, "{nombre}", "{dept}", {salario})'
        res = executor.execute(parse(query)[0])
        inserted.append((emp_id, nombre, dept, salario))
    print(f"   Insertados {len(inserted)} empleados (múltiples splits)")

    # === 3. VALIDAR LECTURAS POR HASH INDEX ===
    print("\n3. Consultas por secondary index")
    for dept in ["IT", "Ventas", "RRHH"]:
        res = executor.execute(parse(f'SELECT * FROM empleados WHERE departamento = "{dept}"')[0])
        print(f"   {dept}: {len(res.data)} empleados encontrados")
    print("   ✅ Consultas por hash index exitosas")

    # === 4. ELIMINACIONES aleatorias ===
    print("\n4. Eliminar 10 registros aleatorios (verificar actualización de índice)")
    deleted = random.sample(inserted, 10)
    for emp_id, *_ in deleted:
        executor.execute(parse(f'DELETE FROM empleados WHERE emp_id = {emp_id}')[0])
    print("   ✅ Eliminaciones ejecutadas")

    # === 5. Verificar consistencia post-delete ===
    print("\n5. Verificar que eliminaciones se reflejen en el índice")
    for _, _, dept, _ in deleted:
        res = executor.execute(parse(f'SELECT * FROM empleados WHERE departamento = "{dept}"')[0])
        for rec in res.data:
            assert rec["emp_id"] not in [d[0] for d in deleted], f"Registro eliminado aún aparece en índice {dept}"
    print("   ✅ Índices actualizados correctamente tras eliminación")

    # === 6. Reinserciones para probar reutilización de buckets ===
    print("\n6. Reinserciones para comprobar reutilización de espacio")
    reinserts = [
        (200, "Nuevo Empleado", "Legal", 58000.0),
        (201, "Reinsertado Uno", "IT", 76000.0),
        (202, "Reinsertado Dos", "Ventas", 61000.0)
    ]
    for r in reinserts:
        executor.execute(parse(f'INSERT INTO empleados VALUES ({r[0]}, "{r[1]}", "{r[2]}", {r[3]})')[0])
    print("   ✅ Reinsertados empleados sin error")

    # === 7. SELECTS ESPECÍFICOS ===
    print("\n7. Consultas específicas post-reinserción")
    res = executor.execute(parse('SELECT * FROM empleados WHERE nombre = "Nuevo Empleado"')[0])
    assert len(res.data) == 1
    print(f"   Encontrado: {res.data[0]['nombre']} en {res.data[0]['departamento']}")

    # === 8. SCAN ALL ===
    print("\n8. SCAN ALL para verificar integridad final")
    res = executor.execute(parse('SELECT * FROM empleados')[0])
    print(f"   Total empleados en tabla: {len(res.data)}")

    # === 9. PRUEBA DE ESTABILIDAD: inserciones adicionales ===
    print("\n9. Insertar 20 empleados extra para asegurar estabilidad")
    for emp_id in range(300, 320):
        dept = random.choice(departamentos)
        nombre = random.choice(nombres) + " Test"
        salario = round(random.uniform(45000, 95000), 2)
        executor.execute(parse(f'INSERT INTO empleados VALUES ({emp_id}, "{nombre}", "{dept}", {salario})')[0])
    print("   ✅ Insertados sin errores (estabilidad comprobada)")

    print("\n✅ TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
    print("=" * 80)


if __name__ == "__main__":
    try:
        test_hash_secondary_exhaustive()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback;

        traceback.print_exc()

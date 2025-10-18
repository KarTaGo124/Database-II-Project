#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Suite de pruebas end-to-end por SQL para verificar funcionalidad:
- Índice HASH secundario (Extendible Hashing)
- Índice RTREE secundario (radio / knn)
- Primario SEQUENTIAL (borrados + rangos)

Todas las sentencias pasan por parse() + Executor, sin atajos.
"""

import os, sys, shutil, time, random
from pathlib import Path

# Colocar cwd en raíz del repo
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))
os.chdir(str(ROOT))

from sql_parser.parser import parse
from sql_parser.executor import Executor
from indexes.core.database_manager import DatabaseManager


# ---------------------- Utilidades de ejecución/chequeo ----------------------

class TestFail(AssertionError): pass

def _pp(obj, limit=3):
    if not isinstance(obj, list):
        return str(obj)
    n = len(obj)
    head = obj[:limit]
    body = ", ".join([str(r) for r in head])
    return f"[{body}]{'…' if n>limit else ''} (n={n})"

def exec_one(executor, sql: str):
    plans = parse(sql)
    assert len(plans) == 1, f"Se esperaba 1 plan: {sql}"
    return executor.execute(plans[0])

def expect_rows(executor, sql: str, n: int):
    out = exec_one(executor, sql)
    data = out.data if hasattr(out, "data") else out
    if not isinstance(data, list):
        raise TestFail(f"Esperaba lista de filas en: {sql}, got={type(data)}")
    if len(data) != n:
        raise TestFail(f"Esperaba {n} filas en: {sql}, got={len(data)}; data={_pp(data)}")
    return data

def expect_at_least_rows(executor, sql: str, n: int):
    out = exec_one(executor, sql)
    data = out.data if hasattr(out, "data") else out
    if not isinstance(data, list) or len(data) < n:
        raise TestFail(f"Esperaba >= {n} filas en: {sql}, got={len(data) if isinstance(data, list) else '¿?'}; data={_pp(data)}")
    return data

def expect_not_exists(executor, sql: str, **equals):
    out = exec_one(executor, sql)
    data = out.data if hasattr(out, "data") else out
    if not isinstance(data, list):
        raise TestFail(f"Esperaba lista de filas en: {sql}")
    for r in data:
        if all(r.get(k) == v for k, v in equals.items()):
            raise TestFail(f"Encontré fila NO esperada {equals} en: {sql}; data={_pp(data, 5)}")

def expect_error(executor, sql: str, exc_type=Exception):
    try:
        exec_one(executor, sql)
    except exc_type as e:
        return e
    except Exception as e:
        raise TestFail(f"Se esperaba {exc_type.__name__} pero lanzó {type(e).__name__}: {e}")
    raise TestFail(f"Se esperaba excepción {exc_type.__name__} para: {sql}")

def reset_db_folder():
    if os.path.exists('data/database'):
        shutil.rmtree('data/database', ignore_errors=True)
        time.sleep(0.2)

def fresh_engine(seed: int = 1337):
    reset_db_folder()
    random.seed(seed)
    db = DatabaseManager(database_name="test_db")
    ex = Executor(db)
    return db, ex


# ---------------------- Suite HASH (Extendible Hashing) ----------------------

def suite_hash():
    print("\n=== SUITE: HASH (Extendible Hashing) ===")
    _, ex = fresh_engine()

    # Tabla + índice HASH
    exec_one(ex, """
        CREATE TABLE empleados (
          emp_id       INT KEY INDEX ISAM,
          nombre       VARCHAR[40],
          departamento VARCHAR[30],
          salario      FLOAT
        );
    """)
    exec_one(ex, 'CREATE INDEX ON empleados (departamento) USING HASH;')

    # Insertar muchos para forzar splits
    departamentos = ["IT", "Ventas", "RRHH", "Finanzas", "Logistica", "Legal"]
    nombres = ["Ana","Carlos","David","Elena","Laura","Luis","Maria","Jose",
               "Carmen","Pedro","Miguel","Lucia","Jorge","Rosa","Daniel"]
    apellidos = ["Lopez","Gomez","Perez","Lee","Chen","Wang"]

    N = 300
    inserted = []
    for emp_id in range(1, N+1):
        nombre = f'{random.choice(nombres)} {random.choice(apellidos)}'
        dept = random.choice(departamentos)
        salario = round(random.uniform(40000, 90000), 2)
        exec_one(ex, f'INSERT INTO empleados VALUES ({emp_id}, "{nombre}", "{dept}", {salario});')
        inserted.append((emp_id, nombre, dept, salario))

    # SELECT EQ por secundario
    it_rows = expect_at_least_rows(ex, 'SELECT * FROM empleados WHERE departamento = "IT";', 1)

    # DELETE por PK y verificar que no aparezca en índice secundario
    victim_id, _, victim_dept, _ = inserted[10]
    exec_one(ex, f'DELETE FROM empleados WHERE emp_id = {victim_id};')
    expect_not_exists(ex, f'SELECT * FROM empleados WHERE departamento = "{victim_dept}";', emp_id=victim_id)

    # DELETE "masivo" por secundario: seleccionar PKs y borrar por PK (solo = y BETWEEN están soportados)
    ventas_rows = exec_one(ex, 'SELECT * FROM empleados WHERE departamento = "Ventas";').data
    for r in ventas_rows:
        exec_one(ex, f'DELETE FROM empleados WHERE emp_id = {r["emp_id"]};')
    expect_rows(ex, 'SELECT * FROM empleados WHERE departamento = "Ventas";', 0)

    # Reinserciones en mismo secundario y ver que reaparezcan
    exec_one(ex, 'INSERT INTO empleados VALUES (9001, "Nuevo Vendedor 1", "Ventas", 60000.0);')
    exec_one(ex, 'INSERT INTO empleados VALUES (9002, "Nuevo Vendedor 2", "Ventas", 65000.0);')
    expect_rows(ex, 'SELECT * FROM empleados WHERE departamento = "Ventas";', 2)

    # Negativo: BETWEEN sobre HASH no soportado
    expect_error(ex, 'SELECT * FROM empleados WHERE departamento BETWEEN "A" AND "Z";', exc_type=Exception)

    print("✅ SUITE HASH: OK")


# ---------------------- Suite RTREE (spatial) ----------------------

def suite_rtree():
    print("\n=== SUITE: RTREE (espacial) ===")
    _, ex = fresh_engine()

    # ARRAY[FLOAT,2] como coordenadas + índice RTREE (secundario)
    exec_one(ex, """
        CREATE TABLE puntos (
          id   INT KEY INDEX ISAM,
          name VARCHAR[40],
          pos  ARRAY[FLOAT,2] INDEX RTREE
        );
    """)

    # Insertar puntos en clusters
    pts = []
    def add_point(pid, name, x, y):
        exec_one(ex, f'INSERT INTO puntos VALUES ({pid}, "{name}", ({x},{y}));')
        pts.append((pid, name, (x,y)))

    # cluster A cerca de (0,0)
    k=1
    for i in range(8):
        add_point(k, f"A{i}", random.uniform(-1.0,1.0), random.uniform(-1.0,1.0)); k+=1
    # cluster B cerca de (10,10)
    for i in range(8):
        add_point(k, f"B{i}", 10+random.uniform(-1.0,1.0), 10+random.uniform(-1.0,1.0)); k+=1
    # outliers
    add_point(k, "O1", 50, 50); k+=1
    add_point(k, "O2", -30, 20); k+=1

    # IN (punto, radio)
    resA = exec_one(ex, 'SELECT * FROM puntos WHERE pos IN ((0,0), 2);')
    if not (6 <= len(resA.data) <= 10):
        raise TestFail(f"Esperaba ~8 vecinos alrededor (0,0) con r=2; got={len(resA.data)} data={_pp(resA.data,5)}")

    # NEAREST (punto, k)
    resK = exec_one(ex, 'SELECT * FROM puntos WHERE pos NEAREST ((10,10), 3);')
    if len(resK.data) != 3:
        raise TestFail(f"NEAREST k=3 debe devolver 3 filas; got={len(resK.data)}")
    for r in resK.data:
        x,y = r["pos"]
        if (x-10)**2 + (y-10)**2 > 25:
            raise TestFail(f"NEAREST devolvió un punto lejano a (10,10): {r}")

    # DELETE por radio (compatibilidad con DELETE actual):
    # 1) seleccionar ids dentro del radio
    vecinos = exec_one(ex, 'SELECT * FROM puntos WHERE pos IN ((0,0), 2);').data
    # 2) borrar uno por uno por PK (DELETE soporta '=')
    for r in vecinos:
        exec_one(ex, f'DELETE FROM puntos WHERE id = {r["id"]};')
    # 3) verificar que no quede ninguno en ese radio
    res_after = exec_one(ex, 'SELECT * FROM puntos WHERE pos IN ((0,0), 2);').data
    if len(res_after) != 0:
        raise TestFail("Delete por radio no vació la vecindad de (0,0) como se esperaba.")

    # Confirmar que el resto sigue presente
    rest = exec_one(ex, 'SELECT * FROM puntos;').data
    if len(rest) == 0:
        raise TestFail("Tras borrar por RTREE quedó vacía la tabla — sospechoso.")

    print("✅ SUITE RTREE: OK")


# ---------------------- Suite SEQUENTIAL (primario) ----------------------

def suite_sequential():
    print("\n=== SUITE: SEQUENTIAL (primario) ===")
    _, ex = fresh_engine()

    exec_one(ex, """
        CREATE TABLE ventas_seq (
          id       INT KEY INDEX SEQUENTIAL,
          nombre   VARCHAR[40],
          cantidad INT,
          precio   FLOAT,
          fecha    DATE
        );
    """)

    datos = [
        (10, "A", 1, 100.0, "2024-01-01"),
        (5,  "B", 2,  50.0, "2024-01-02"),
        (20, "C", 3, 200.0, "2024-01-03"),
        (15, "D", 4, 150.0, "2024-01-04"),
        (1,  "E", 5,  10.0, "2024-01-05"),
        (30, "F", 6, 300.0, "2024-01-06"),
    ]
    for r in datos:
        exec_one(ex, f'INSERT INTO ventas_seq VALUES ({r[0]},"{r[1]}",{r[2]},{r[3]},"{r[4]}");')

    # Búsqueda por PK existente e inexistente
    expect_rows(ex, 'SELECT * FROM ventas_seq WHERE id = 10;', 1)
    expect_rows(ex, 'SELECT * FROM ventas_seq WHERE id = 999;', 0)

    # Range por PK
    r = exec_one(ex, 'SELECT * FROM ventas_seq WHERE id BETWEEN 5 AND 20;').data
    ids = sorted([row["id"] for row in r])
    if ids != [5,10,15,20]:
        raise TestFail(f"Rango [5,20] incorrecto: {ids}")

    # DELETE por PK + verificación
    exec_one(ex, 'DELETE FROM ventas_seq WHERE id = 15;')
    expect_rows(ex, 'SELECT * FROM ventas_seq WHERE id = 15;', 0)

    # DELETE por rango + verificación
    exec_one(ex, 'DELETE FROM ventas_seq WHERE id BETWEEN 5 AND 10;')
    expect_rows(ex, 'SELECT * FROM ventas_seq WHERE id = 5;', 0)
    expect_rows(ex, 'SELECT * FROM ventas_seq WHERE id = 10;', 0)

    # Contenido final
    left = exec_one(ex, 'SELECT * FROM ventas_seq;').data
    left_ids = sorted([row["id"] for row in left])
    if left_ids != [1,20,30]:
        raise TestFail(f"Contenido final inesperado en SEQUENTIAL: {left_ids}")

    print("✅ SUITE SEQUENTIAL: OK")


# ---------------------- Main ----------------------

def main():
    fails = []
    try:
        suite_hash()
    except Exception as e:
        print(f"❌ FALLÓ SUITE HASH: {e}")
        fails.append(("HASH", e))

    try:
        suite_rtree()
    except Exception as e:
        print(f"❌ FALLÓ SUITE RTREE: {e}")
        fails.append(("RTREE", e))

    try:
        suite_sequential()
    except Exception as e:
        print(f"❌ FALLÓ SUITE SEQUENTIAL: {e}")
        fails.append(("SEQUENTIAL", e))

    if fails:
        print("\n====== RESUMEN: FALLAS ======")
        for name, err in fails:
            print(f"- {name}: {err}")
        sys.exit(1)

    print("\n🎉 TODAS LAS SUITES PASARON")
    sys.exit(0)


if __name__ == "__main__":
    main()

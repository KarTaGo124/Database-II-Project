import argparse
import time
from pathlib import Path
from typing import Any, Dict, List, Iterable

from .parser import parse
from .executor import Executor
from indexes.core.database_manager import DatabaseManager

def build_statements(table: str, csv_path: str) -> List[str]:
    return [
        f'CREATE TABLE {table} (  id INT KEY INDEX ISAM,  nombre VARCHAR[50] INDEX BTREE,  cantidad INT,  precio FLOAT INDEX EXTENDIBLE,  fecha DATE INDEX BTREE);',
        f'CREATE TABLE {table} FROM FILE "{csv_path}" USING INDEX ISAM(id);',

        f'SELECT * FROM {table} WHERE id = 403;',
        f'SELECT * FROM {table} WHERE id = 56;',

        f'SELECT * FROM {table} WHERE nombre = "Laptop";',

        f'SELECT * FROM {table} WHERE nombre BETWEEN "C" AND "N";',

        f'SELECT * FROM {table} WHERE precio = 813.52;',

        f'SELECT * FROM {table} WHERE precio BETWEEN 700 AND 900;',

        f'SELECT * FROM {table} WHERE fecha = "2024-07-30";',

        f'DELETE FROM {table} WHERE nombre = "Laptop";',
        f'SELECT * FROM {table} WHERE nombre = "Laptop";',

        f'SELECT * FROM {table} WHERE id = 403;',
        f'DELETE FROM {table} WHERE id = 403;',
        f'SELECT * FROM {table} WHERE id = 403;',
    ]

def main():
    parser = argparse.ArgumentParser(description="SQL demo engine (CSV real, físico).")
    parser.add_argument("--csv", default=str(Path("data/datasets/sales_dataset_unsorted.csv")),
                        help="Ruta al CSV (por defecto: data/datasets/sales_dataset_unsorted.csv)")
    parser.add_argument("--table", default="Ventas", help="Nombre de tabla física (por defecto: Ventas)")
    parser.add_argument("--limit", type=int, default=25, help="Límite de filas a imprimir por SELECT (por defecto: 25)")
    args = parser.parse_args()

    db = DatabaseManager(database_name="demo_db")
    execu = Executor(db)

    stmts = build_statements(args.table, args.csv)
    run_block(f"BLOQUE: {args.table} (CSV real)", stmts, execu, row_print_limit=args.limit)

def banner(title: str) -> None:
    print(f"\n== {title} ==\n")

def format_ms(ms: float) -> str:
    return f"[{ms:.1f} ms]"

def print_rows(rows: List[Dict[str, Any]], limit: int = 25) -> None:
    n = len(rows)
    if n == 0:
        print("Output: []")
        return
    show = rows[:limit]
    print(f"Output ({min(n, limit)} de {n} filas):")
    for i, r in enumerate(show, 1):
        print(f"  {i:>3}: {r}")
    if n > limit:
        print(f"... ({n - limit} más)")

class Stopwatch:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.ms = (time.perf_counter() - self.t0) * 1000.0

def run_block(title: str, stmts: Iterable[str], execu: Executor, row_print_limit: int = 25) -> None:
    banner(title)

    for sql in stmts:
        print(f"SQL: {sql}")
        try:
            plans = parse(sql)
        except Exception as e:
            print(f"Parse error: {e}\n")
            continue

        for plan in plans:
            try:
                with Stopwatch() as sw:
                    out = execu.execute(plan)
                    print(f"Plan: {type(plan).__name__}")
                    if out is None:
                        pass
                    elif isinstance(out, list) and (not out or isinstance(out[0], dict)):
                        print_rows(out, limit=row_print_limit)
                        print()
                    else:
                        print(f"Output: {out}\n")
                print(format_ms(sw.ms), "\n")
            except Exception as e:
                print(f"Execution error: {e}\n")

if __name__ == "__main__":
    main()
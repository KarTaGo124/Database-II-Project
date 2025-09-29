# TESTS QUE FUNCIONAN CON EL CSV
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, List, Iterable

from .parser_sql import parse
from .executor import Executor
from indexes.core.database_manager import DatabaseManager
from .planes import ExplainPlan, PredicateEq, PredicateBetween


# ========= util: impresión y tiempos =========

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
    """Context manager simple para medir ms."""
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.ms = (time.perf_counter() - self.t0) * 1000.0


# ========= EXPLAIN físico =========

def _pk_name(db: DatabaseManager, table: str) -> str:
    return db.tables[table]["table"].key_field


def _primary_type(db: DatabaseManager, table: str) -> str:
    info = db.get_table_info(table) or {}
    return str(info.get("primary_type", "ISAM")).upper()


def _has_secondary(db: DatabaseManager, table: str, col: str) -> bool:
    sec = db.tables.get(table, {}).get("secondary_indexes", {}) or {}
    return col in sec and sec[col] is not None


def physical_explain(db: DatabaseManager, sel_plan) -> str:
    table = sel_plan.table
    where = sel_plan.where
    pk = _pk_name(db, table)
    pk_typ = _primary_type(db, table)

    if where is None:
        primary = db.tables[table]["primary_index"]
        if hasattr(primary, "scanAll"):
            return f"Index Full Scan using {pk_typ}\n  -> full scan (scanAll)"
        return "Seq Scan\n  -> full table"

    if isinstance(where, PredicateEq):
        col = where.column or pk
        if col == pk:
            return f"Index Scan using {pk_typ}\n  -> index_col={pk}, op=== "
        if _has_secondary(db, table, col):
            return f"Index Scan using {col}\n  -> index_col={col}, op=== "
        return "Seq Scan\n  -> filter: equality"

    if isinstance(where, PredicateBetween):
        col = where.column or pk
        if col == pk:
            return f"Index Range Scan using {pk_typ}\n  -> index_col={pk}, op=BETWEEN"
        if _has_secondary(db, table, col):
            return f"Index Range Scan using {col}\n  -> index_col={col}, op=BETWEEN"
        return "Seq Scan\n  -> filter: BETWEEN"

    return "Seq Scan\n  -> filter: (predicado no soportado físicamente)"


# ========= Runner =========

def run_block(title: str, stmts: Iterable[str], execu: Executor, row_print_limit: int = 25) -> None:
    banner(title)

    for sql in stmts:
        print(f"SQL: {sql}")
        # Parse
        try:
            plans = parse(sql)
        except Exception as e:
            print(f"Parse error: {e}\n")
            continue

        for plan in plans:
            try:
                with Stopwatch() as sw:
                    if isinstance(plan, ExplainPlan):
                        phys = physical_explain(execu.db, plan.inner)
                        print(phys, "\n")
                    else:
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


# ========= Statements de demo (CSV real) =========

def build_statements(table: str, csv_path: str) -> List[str]:
    return [
        # Definición física e ingestión desde CSV
        f'CREATE TABLE {table} (  id INT KEY INDEX ISAM,  nombre VARCHAR[50] INDEX BTREE,  cantidad INT,  precio FLOAT INDEX EXTENDIBLE,  fecha DATE INDEX BTREE);',
        f'CREATE TABLE {table} FROM FILE "{csv_path}" USING INDEX ISAM(id);',

        # Consultas varias (todas reales)
        f'EXPLAIN SELECT * FROM {table} WHERE id = 403;',
        f'SELECT * FROM {table} WHERE id = 403;',
        f'SELECT * FROM {table} WHERE id = 56;',

        f'EXPLAIN SELECT * FROM {table} WHERE nombre = "Laptop";',
        f'SELECT * FROM {table} WHERE nombre = "Laptop";',

        f'EXPLAIN SELECT * FROM {table} WHERE nombre BETWEEN "C" AND "N";',
        f'SELECT * FROM {table} WHERE nombre BETWEEN "C" AND "N";',

        f'EXPLAIN SELECT * FROM {table} WHERE precio = 813.52;',
        f'SELECT * FROM {table} WHERE precio = 813.52;',

        f'EXPLAIN SELECT * FROM {table} WHERE precio BETWEEN 700 AND 900;',
        f'SELECT * FROM {table} WHERE precio BETWEEN 700 AND 900;',

        f'EXPLAIN SELECT * FROM {table} WHERE fecha = "2024-07-30";',
        f'SELECT * FROM {table} WHERE fecha = "2024-07-30";',

        f'DELETE FROM {table} WHERE nombre = "Laptop";',
        f'SELECT * FROM {table} WHERE nombre = "Laptop";',

        f'SELECT * FROM {table} WHERE id = 403;',
        f'DELETE FROM {table} WHERE id = 403;',
        f'SELECT * FROM {table} WHERE id = 403;',
    ]


# ========= main / CLI =========

def main() -> None:
    parser = argparse.ArgumentParser(description="SQL demo engine (CSV real, físico).")
    parser.add_argument("--csv", default=str(Path("data/datasets/sales_dataset_unsorted.csv")),
                        help="Ruta al CSV (por defecto: data/datasets/sales_dataset_unsorted.csv)")
    parser.add_argument("--table", default="Ventas", help="Nombre de tabla física (por defecto: Ventas)")
    parser.add_argument("--limit", type=int, default=25, help="Límite de filas a imprimir por SELECT (por defecto: 25)")
    args = parser.parse_args()

    # Motor físico real
    db = DatabaseManager(database_name="demo_db")
    execu = Executor(db)

    # Statements y ejecución
    stmts = build_statements(args.table, args.csv)
    run_block(f"BLOQUE: {args.table} (CSV real)", stmts, execu, row_print_limit=args.limit)


if __name__ == "__main__":
    main()

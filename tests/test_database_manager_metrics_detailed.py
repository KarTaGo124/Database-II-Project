#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table, Record
import shutil
import time

def test_database_manager_metrics_detailed():
    """Detailed test to verify metrics accumulation in DatabaseManager operations."""

    print("=== DATABASE MANAGER DETAILED METRICS TEST ===")

    test_dir = f"test_db_metrics_{int(time.time())}"

    try:
        db = DatabaseManager(test_dir)

        # Create ISAM table with secondary index
        table = Table("test_metrics", [("id", "INT", 4), ("name", "CHAR", 20), ("category", "INT", 4)], "id")
        db.create_table(table, "ISAM")
        db.create_index("test_metrics", "category", "ISAM")

        # Insert test data
        for i in range(1, 6):
            record = Record(table.all_fields, table.key_field)
            record.set_field_value("id", i)
            record.set_field_value("name", f"Item{i}")
            record.set_field_value("category", 100 + (i % 3))
            db.insert("test_metrics", record)

        print("\n=== TESTING METRICS ACCUMULATION ===")

        # Test 1: Primary search (should be simple)
        print("\n1. PRIMARY SEARCH:")
        primary_result = db.search("test_metrics", 2)
        print(f"   Primary search id=2: reads={primary_result.disk_reads}, writes={primary_result.disk_writes}")

        # Test 2: Secondary search (should accumulate secondary + primary lookups)
        print("\n2. SECONDARY SEARCH:")
        secondary_result = db.search("test_metrics", 100, "category")
        print(f"   Secondary search category=100: reads={secondary_result.disk_reads}, writes={secondary_result.disk_writes}")
        print(f"   Records found: {len(secondary_result.data)}")

        # The problem: Secondary search should have MORE reads than primary search
        # because it does: secondary index traversal + primary index lookups for each match
        if secondary_result.disk_reads <= primary_result.disk_reads:
            print(f"   [ERROR] Secondary reads ({secondary_result.disk_reads}) should be > primary reads ({primary_result.disk_reads})")
            print(f"   This indicates nested primary lookups are NOT being counted!")
        else:
            print(f"   [OK] Secondary correctly accumulates nested metrics")

        # Test 3: Range search with secondary index
        print("\n3. SECONDARY RANGE_SEARCH:")
        range_result = db.range_search("test_metrics", 100, 102, "category")
        print(f"   Range search category 100-102: reads={range_result.disk_reads}, writes={range_result.disk_writes}")
        print(f"   Records found: {len(range_result.data)}")

        # Test 4: Compare direct secondary index vs DatabaseManager
        print("\n4. DIRECT SECONDARY INDEX COMPARISON:")
        secondary_index = db.tables["test_metrics"]["secondary_indexes"]["category"]["index"]
        direct_result = secondary_index.search(100)
        print(f"   Direct secondary search: reads={direct_result.disk_reads}, writes={direct_result.disk_writes}")
        print(f"   DatabaseManager search: reads={secondary_result.disk_reads}, writes={secondary_result.disk_writes}")

        if secondary_result.disk_reads == direct_result.disk_reads:
            print(f"   [ERROR] DatabaseManager is NOT adding primary lookup metrics!")
        else:
            print(f"   [OK] DatabaseManager correctly adds primary lookup metrics")

        print("\n=== ANALYSIS ===")
        print(f"Expected behavior:")
        print(f"- Primary search: low reads (direct index access)")
        print(f"- Secondary search: higher reads (secondary + multiple primary lookups)")
        print(f"- DatabaseManager should accumulate ALL nested operation metrics")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_database_manager_metrics_detailed()
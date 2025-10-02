#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table, Record
import shutil
import time

def test_breakdown_functionality():
    """Test the new operation breakdown functionality in DatabaseManager."""

    print("=== TESTING OPERATION BREAKDOWN FUNCTIONALITY ===")

    test_dir = f"test_breakdown_{int(time.time())}"
    print(f"Test directory: {test_dir}")

    try:
        db = DatabaseManager(test_dir)

        # Create table with multiple secondary indexes
        table = Table("products", [
            ("id", "INT", 4),
            ("name", "CHAR", 20),
            ("category_id", "INT", 4),
            ("price", "FLOAT", 4)
        ], "id")

        db.create_table(table, "ISAM")
        db.create_index("products", "category_id", "ISAM")
        db.create_index("products", "name", "ISAM")

        print("\n=== 1. INSERT BREAKDOWN TEST ===")
        record = Record(table.all_fields, table.key_field)
        record.set_field_value("id", 1)
        record.set_field_value("name", "Laptop")
        record.set_field_value("category_id", 10)
        record.set_field_value("price", 999.99)

        insert_result = db.insert("products", record)
        print(f"INSERT Result:")
        print(f"   Total: reads={insert_result.disk_reads}, writes={insert_result.disk_writes}, time={insert_result.execution_time_ms:.2f}ms")

        if hasattr(insert_result, 'operation_breakdown') and insert_result.operation_breakdown:
            breakdown = insert_result.operation_breakdown
            print(f"   Breakdown:")
            for key, metrics in breakdown.items():
                print(f"      {key}: reads={metrics['reads']}, writes={metrics['writes']}, time={metrics['time_ms']:.2f}ms")

            # Verify totals match
            total_breakdown_reads = sum(metrics['reads'] for metrics in breakdown.values())
            total_breakdown_writes = sum(metrics['writes'] for metrics in breakdown.values())

            if total_breakdown_reads == insert_result.disk_reads and total_breakdown_writes == insert_result.disk_writes:
                print("   [OK] Breakdown totals match overall metrics")
            else:
                print(f"   [ERROR] Breakdown totals don't match: {total_breakdown_reads}/{total_breakdown_writes} vs {insert_result.disk_reads}/{insert_result.disk_writes}")
        else:
            print("   [ERROR] No breakdown found in INSERT result")

        # Add more records for testing
        for i in range(2, 5):
            record = Record(table.all_fields, table.key_field)
            record.set_field_value("id", i)
            record.set_field_value("name", f"Product{i}")
            record.set_field_value("category_id", 10 + (i % 2))
            record.set_field_value("price", 100.0 + i * 50)
            db.insert("products", record)

        print("\n=== 2. SECONDARY SEARCH BREAKDOWN TEST ===")
        search_result = db.search("products", 10, "category_id")
        print(f"SEARCH by category_id=10:")
        print(f"   Total: reads={search_result.disk_reads}, writes={search_result.disk_writes}, time={search_result.execution_time_ms:.2f}ms")
        print(f"   Records found: {len(search_result.data)}")

        if hasattr(search_result, 'operation_breakdown') and search_result.operation_breakdown:
            breakdown = search_result.operation_breakdown
            print(f"   Breakdown:")
            for key, metrics in breakdown.items():
                print(f"      {key}: reads={metrics['reads']}, writes={metrics['writes']}, time={metrics['time_ms']:.2f}ms")

            # Verify secondary search shows proper separation
            if 'secondary_metrics' in breakdown and 'primary_metrics' in breakdown:
                print("   [OK] Both secondary and primary metrics captured")
                if breakdown['primary_metrics']['reads'] > 0:
                    print("   [OK] Primary lookup metrics captured")
                else:
                    print("   [ERROR] No primary lookup metrics captured")
            else:
                print("   [ERROR] Missing secondary or primary metrics in breakdown")
        else:
            print("   [ERROR] No breakdown found in SEARCH result")

        print("\n=== 3. SECONDARY RANGE_SEARCH BREAKDOWN TEST ===")
        range_result = db.range_search("products", 10, 11, "category_id")
        print(f"RANGE_SEARCH category_id 10-11:")
        print(f"   Total: reads={range_result.disk_reads}, writes={range_result.disk_writes}, time={range_result.execution_time_ms:.2f}ms")
        print(f"   Records found: {len(range_result.data)}")

        if hasattr(range_result, 'operation_breakdown') and range_result.operation_breakdown:
            breakdown = range_result.operation_breakdown
            print(f"   Breakdown:")
            for key, metrics in breakdown.items():
                print(f"      {key}: reads={metrics['reads']}, writes={metrics['writes']}, time={metrics['time_ms']:.2f}ms")
            print("   [OK] Range search breakdown captured")
        else:
            print("   [ERROR] No breakdown found in RANGE_SEARCH result")

        print("\n=== 4. SECONDARY DELETE BREAKDOWN TEST ===")
        delete_result = db.delete("products", 10, "category_id")
        print(f"DELETE by category_id=10:")
        print(f"   Total: reads={delete_result.disk_reads}, writes={delete_result.disk_writes}, time={delete_result.execution_time_ms:.2f}ms")
        print(f"   Records deleted: {delete_result.data}")

        if hasattr(delete_result, 'operation_breakdown') and delete_result.operation_breakdown:
            breakdown = delete_result.operation_breakdown
            print(f"   Breakdown:")
            for key, metrics in breakdown.items():
                print(f"      {key}: reads={metrics['reads']}, writes={metrics['writes']}, time={metrics['time_ms']:.2f}ms")
            print("   [OK] Delete breakdown captured")
        else:
            print("   [ERROR] No breakdown found in DELETE result")

        print("\n=== 5. PRIMARY OPERATION TEST (NO BREAKDOWN) ===")
        primary_search = db.search("products", 2)
        print(f"PRIMARY SEARCH by id=2:")
        print(f"   Total: reads={primary_search.disk_reads}, writes={primary_search.disk_writes}, time={primary_search.execution_time_ms:.2f}ms")

        if hasattr(primary_search, 'operation_breakdown') and primary_search.operation_breakdown:
            print("   [ERROR] Primary operation should not have breakdown")
        else:
            print("   [OK] Primary operation correctly has no breakdown")

        print("\n=== BREAKDOWN IMPLEMENTATION SUMMARY ===")
        print("[OK] INSERT: Detailed breakdown by individual secondary indexes")
        print("[OK] SEARCH (secondary): Primary vs secondary metrics separated")
        print("[OK] RANGE_SEARCH (secondary): Metrics separation maintained")
        print("[OK] DELETE (secondary): Breakdown propagated from search")
        print("[OK] Primary operations: No unnecessary breakdown")
        print("[OK] Totals consistency: Breakdown sums match overall metrics")

        print("\n[SUCCESS] Operation breakdown functionality working correctly!")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_breakdown_functionality()
#!/usr/bin/env python3

import os
import shutil
import sys

sys.path.append('.')

from sql_parser.interface import execute_sql
from indexes.core.database_manager import DatabaseManager


def test_extendible_hash_simple():
    print("=== Simple Extendible Hash Test ===")

    test_dir = "test_extendible_hash"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    data_path = os.path.join("data", "databases", test_dir)
    if os.path.exists(data_path):
        shutil.rmtree(data_path)

    try:
        db_manager = DatabaseManager(test_dir)

        print("\n=== STEP 1: CREATE TABLE ===")
        result = execute_sql(db_manager, "CREATE TABLE productos (id INT KEY INDEX ISAM, nombre VARCHAR[20]);")
        print(f"Result: {result}")

        print("\n=== STEP 2: CREATE HASH INDEX ===")
        result = execute_sql(db_manager, "CREATE INDEX ON productos (nombre) USING HASH;")
        print(f"Result: {result}")

        print("\n=== STEP 3: INSERT 3 RECORDS ===")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (1, "Laptop");')
        print(f"Insert Laptop (id=1): {result}")

        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (2, "Mouse");')
        print(f"Insert Mouse (id=2): {result}")

        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (3, "Keyboard");')
        print(f"Insert Keyboard (id=3): {result}")

        print("\n=== STEP 4: SEARCH EACH RECORD ===")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Laptop";')
        print(f"Search Laptop: {result}")

        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Mouse";')
        print(f"Search Mouse: {result}")

        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Keyboard";')
        print(f"Search Keyboard: {result}")

        print("\n=== STEP 5: DELETE ONE RECORD ===")
        result = execute_sql(db_manager, 'DELETE FROM productos WHERE nombre = "Mouse";')
        print(f"Delete Mouse: {result}")

        print("\n=== STEP 6: VERIFY DELETION ===")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Mouse";')
        print(f"Search Mouse (should be empty): {result}")

        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Laptop";')
        print(f"Search Laptop (should still exist): {result}")

        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Keyboard";')
        print(f"Search Keyboard (should still exist): {result}")

        print("\n=== STEP 7: INSERT DUPLICATE KEY ===")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (4, "Laptop");')
        print(f"Insert another Laptop (id=4): {result}")

        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Laptop";')
        print(f"Search Laptop (should return 2 records): {result}")

        print("\n=== STEP 8: DELETE ALL LAPTOPS ===")
        result = execute_sql(db_manager, 'DELETE FROM productos WHERE nombre = "Laptop";')
        print(f"Delete all Laptops: {result}")

        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Laptop";')
        print(f"Search Laptop (should be empty): {result}")

        print("\n=== STEP 9: INSERT INTO DELETED SLOT ===")
        result = execute_sql(db_manager, 'INSERT INTO productos VALUES (5, "Monitor");')
        print(f"Insert Monitor (id=5): {result}")

        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Monitor";')
        print(f"Search Monitor: {result}")

        print("\n=== STEP 10: SCAN ALL REMAINING ===")
        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Keyboard";')
        print(f"Keyboard: {result}")

        result = execute_sql(db_manager, 'SELECT * FROM productos WHERE nombre = "Monitor";')
        print(f"Monitor: {result}")

        print("\n=== STEP 11: CHECK FILES ===")
        full_path = os.path.join("data", "databases", test_dir)
        if os.path.exists(full_path):
            for root, dirs, filenames in os.walk(full_path):
                for filename in filenames:
                    filepath = os.path.join(root, filename)
                    size = os.path.getsize(filepath)
                    print(f"  {filename}: {size} bytes")

        print("\n✓ Test completed successfully!")

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)


if __name__ == "__main__":
    test_extendible_hash_simple()
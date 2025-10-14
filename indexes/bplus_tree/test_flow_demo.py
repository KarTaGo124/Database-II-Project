#!/usr/bin/env python3

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from indexes.core.record import Record, Table
from indexes.core.database_manager import DatabaseManager

def test_unclustered_flow():
    print("=== TESTING UNCLUSTERED INDEX FLOW: PRIMARY -> SECONDARY ===")
    print("Demonstrating that unclustered index stores primary keys, not records")
    print()
    
    # Clean up any existing test files
    import shutil
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_dir = os.path.join(current_dir, "test_flow")
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir, exist_ok=True)
    
    # Define simple table schema
    sql_fields = [
        ("id", "INT", 4),
        ("product", "CHAR", 30), 
        ("price", "FLOAT", 4)
    ]
    
    table = Table("products", sql_fields, "id")
    
    # Create DatabaseManager
    db_manager = DatabaseManager("flow_test")
    db_manager.base_dir = test_dir
    
    # Step 1: Create table with primary index
    print("STEP 1: Creating table with PRIMARY BTREE index")
    result = db_manager.create_table(table, "BTREE")
    print(f"   Primary index created: {result}")
    
    # Step 2: Create secondary index on product name
    print("\nSTEP 2: Creating SECONDARY index on 'product' field")
    result = db_manager.create_index("products", "product", "BTREE")
    print(f"   Secondary index created: {result}")
    print(f"   Secondary index will store: product_name -> primary_key mappings")
    
    # Step 3: Insert records into PRIMARY index
    print("\nSTEP 3: INSERTING records (Primary index first, then updates secondary)")
    
    test_products = [
        (101, "Laptop", 999.99),
        (102, "Mouse", 25.50),
        (103, "Keyboard", 75.00),
        (104, "Monitor", 299.99),
        (105, "Laptop", 1299.99),  # Duplicate product name with different primary key
    ]
    
    for product_id, product_name, price in test_products:
        record = Record(sql_fields, "id")
        record.set_values(id=product_id, product=product_name, price=price)
        
        print(f"\n   Inserting: ID={product_id}, Product='{product_name}', Price=${price}")
        
        start_time = time.time()
        result = db_manager.insert("products", record)
        insert_time = time.time() - start_time
        
        print(f"   -> Primary index: stores complete record at key {product_id}")
        print(f"   -> Secondary index: stores mapping '{product_name}' -> PrimaryKey({product_id})")
        print(f"   -> Disk I/O: {result.disk_reads} reads, {result.disk_writes} writes ({insert_time*1000:.1f}ms)")
    
    print(f"\nINSERTION COMPLETE: 5 records in primary, 5 mappings in secondary")
    
    # Step 4: Demonstrate search by primary key
    print("\nSTEP 4: SEARCH BY PRIMARY KEY (Direct lookup)")
    search_id = 103
    start_time = time.time()
    result = db_manager.search("products", search_id)
    search_time = time.time() - start_time
    
    if result.data:
        record = result.data[0]
        print(f"   Searching for ID {search_id}:")
        print(f"   -> Found: {record.product} - ${record.price}")
        print(f"   -> Disk I/O: {result.disk_reads} reads, {result.disk_writes} writes ({search_time*1000:.1f}ms)")
        print(f"   -> Process: PRIMARY index[{search_id}] -> Record directly")
    
    # Step 5: Demonstrate search by secondary key (unclustered)
    print("\nSTEP 5: SEARCH BY SECONDARY KEY (Two-step lookup)")
    search_product = "Laptop"
    start_time = time.time()
    result = db_manager.search("products", search_product, "product")
    search_time = time.time() - start_time
    
    print(f"   Searching for product '{search_product}':")
    print(f"   -> STEP 5a: Secondary index['{search_product}'] -> [PrimaryKey(101), PrimaryKey(105)]")
    print(f"   -> STEP 5b: Primary index[101] -> Record(101, 'Laptop', 999.99)")
    print(f"   -> STEP 5c: Primary index[105] -> Record(105, 'Laptop', 1299.99)")
    print(f"   -> Found {len(result.data)} laptops:")
    
    for record in result.data:
        print(f"      * ID {record.id}: {record.product} - ${record.price}")
    
    print(f"   -> Total Disk I/O: {result.disk_reads} reads, {result.disk_writes} writes ({search_time*1000:.1f}ms)")
    print(f"   -> This demonstrates the unclustered two-step lookup!")
    
    # Step 6: Demonstrate range search on secondary index
    print("\nSTEP 6: RANGE SEARCH BY SECONDARY KEY")
    start_time = time.time()
    result = db_manager.range_search("products", "K", "M", "product")  # Keyboard to Monitor
    range_time = time.time() - start_time
    
    print(f"   Range search: products between 'K' and 'M':")
    print(f"   -> STEP 6a: Secondary index range['K', 'M'] -> [PrimaryKey(103), PrimaryKey(105), ...]")
    print(f"   -> STEP 6b: For each primary key, lookup in primary index")
    print(f"   -> Found {len(result.data)} products:")
    
    for record in result.data:
        print(f"      * ID {record.id}: {record.product} - ${record.price}")
    
    print(f"   -> Total Disk I/O: {result.disk_reads} reads, {result.disk_writes} writes ({range_time*1000:.1f}ms)")
    
    # Step 7: Show storage analysis
    print("\nSTEP 7: STORAGE ANALYSIS")
    
    primary_dir = os.path.join(test_dir, "primary")
    secondary_dir = os.path.join(test_dir, "secondary")
    
    if os.path.exists(primary_dir):
        primary_files = [f for f in os.listdir(primary_dir) if f.endswith('.dat')]
        print(f"   Primary index files: {primary_files}")
        for file in primary_files:
            file_path = os.path.join(primary_dir, file)
            file_size = os.path.getsize(file_path)
            print(f"   -> {file}: {file_size} bytes (stores complete records)")
    
    if os.path.exists(secondary_dir):
        secondary_files = [f for f in os.listdir(secondary_dir) if f.endswith('.dat')]
        print(f"   Secondary index files: {secondary_files}")
        for file in secondary_files:
            file_path = os.path.join(secondary_dir, file)
            file_size = os.path.getsize(file_path)
            print(f"   -> {file}: {file_size} bytes (stores product_name -> primary_key mappings)")
    

    print("=== UNCLUSTERED INDEX FLOW TEST COMPLETED ===")

if __name__ == "__main__":
    test_unclustered_flow()

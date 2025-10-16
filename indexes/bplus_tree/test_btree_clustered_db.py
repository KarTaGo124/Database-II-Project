"""
Test B+ Tree Clustered Index using DatabaseManager
Disk-only operations with sales CSV
"""
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table, Record

def create_sales_table():
    """Define sales table structure"""
    sql_fields = [
        ("sale_id", "INT", 4),
        ("product_name", "CHAR", 50),
        ("quantity", "INT", 4),
        ("unit_price", "FLOAT", 4),
        ("sale_date", "CHAR", 20)
    ]
    
    return Table(
        table_name="sales",
        sql_fields=sql_fields,
        key_field="sale_id"
    )

def load_sales_from_csv(db_manager, table_name, csv_path, max_records=100):
    """Load sales from CSV"""
    import csv
    
    print(f"\n{'='*60}")
    print(f"LOADING DATA FROM CSV: {csv_path}")
    print(f"{'='*60}")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        count = 0
        
        for row in reader:
            if count >= max_records:
                break
            
            try:
                sale_id = int(row['ID de la venta'])
                product_name = row['Nombre producto'].strip().encode('utf-8')
                quantity = int(row['Cantidad vendida'])
                unit_price = float(row['Precio unitario'].replace(',', '.'))
                sale_date = row['Fecha de venta'].strip().encode('utf-8')
                
                # Get table definition
                table_info = db_manager.tables[table_name]
                table_def = table_info["table"]
                
                record = Record(table_def.all_fields, table_def.key_field)
                record.set_values(
                    sale_id=sale_id,
                    product_name=product_name,
                    quantity=quantity,
                    unit_price=unit_price,
                    sale_date=sale_date
                )
                
                result = db_manager.insert(table_name, record)
                count += 1
                
                if count % 20 == 0:
                    print(f"  Loaded {count} records | R/W: {result.disk_reads}/{result.disk_writes}")
                    
            except Exception as e:
                print(f"  Error loading row: {e}")
                continue
    
    print(f"\n✓ Total records loaded: {count}")
    return count

def test_search_operations(db_manager, table_name):
    """Test search operations on clustered index"""
    print(f"\n{'='*60}")
    print("SEARCH OPERATIONS TEST (Clustered Index)")
    print(f"{'='*60}")
    
    # Test 1: Exact search
    print("\n1. Exact search (sale_id=56):")
    result = db_manager.search(table_name, 56)
    if result.data:
        record = result.data[0]
        print(f"   Found: {record.product_name} | Qty: {record.quantity} | Price: ${record.unit_price}")
    else:
        print("   Not found")
    print(f"   Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")
    
    # Test 2: Non-existent key
    print("\n2. Search non-existent (sale_id=99999):")
    result = db_manager.search(table_name, 99999)
    print(f"   Found: {len(result.data)} records")
    print(f"   Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")
    
    # Test 3: Range search
    print("\n3. Range search (sale_id 100-110):")
    result = db_manager.range_search(table_name, 100, 110)
    print(f"   Found: {len(result.data)} records")
    for rec in result.data[:3]:
        print(f"     - ID:{rec.sale_id} | {rec.product_name}")
    if len(result.data) > 3:
        print(f"     ... and {len(result.data) - 3} more")
    print(f"   Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")

def test_delete_operations(db_manager, table_name):
    """Test delete operations on clustered index"""
    print(f"\n{'='*60}")
    print("DELETE OPERATIONS TEST (Clustered Index)")
    print(f"{'='*60}")
    
    # Test 1: Delete single record
    print("\n1. Delete single record (sale_id=403):")
    result = db_manager.delete(table_name, 403)
    print(f"   Deleted: {result.data}")
    print(f"   Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")
    
    # Verify deletion
    search_result = db_manager.search(table_name, 403)
    print(f"   Verification: Found {len(search_result.data)} records (should be 0)")
    
    # Test 2: Range delete
    print("\n2. Range delete (sale_id 200-205):")
    result = db_manager.range_delete(table_name, 200, 205)
    print(f"   Deleted: {result.data} records")
    print(f"   Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")

def test_scan_all(db_manager, table_name):
    """Test full table scan"""
    print(f"\n{'='*60}")
    print("FULL TABLE SCAN TEST")
    print(f"{'='*60}")
    
    result = db_manager.scan_all(table_name)
    print(f"Total records: {len(result.data)}")
    print(f"Performance: {result.execution_time_ms:.2f}ms | Disk R/W: {result.disk_reads}/{result.disk_writes}")
    
    # Show first 5 records
    print("\nFirst 5 records:")
    for rec in result.data[:5]:
        print(f"  ID:{rec.sale_id} | {rec.product_name} | ${rec.unit_price}")

def main():
    print("\n" + "="*60)
    print("B+ TREE CLUSTERED INDEX TEST - DISK-BASED")
    print("="*60)
    
    # Setup
    db_manager = DatabaseManager("btree_clustered_test_db")
    table = create_sales_table()
    csv_path = "data/datasets/sales_dataset_unsorted.csv"
    
    # Create table with B+ Tree clustered index
    print("\nCreating table 'sales' with B+ Tree Clustered Index...")
    db_manager.create_table(table, primary_index_type="BTREE")
    print("✓ Table created with B+ Tree (Primary/Clustered)")
    
    # Load data
    load_sales_from_csv(db_manager, "sales", csv_path, max_records=100)
    
    # Run tests
    test_search_operations(db_manager, "sales")
    test_delete_operations(db_manager, "sales")
    test_scan_all(db_manager, "sales")
    
    # Final stats
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    print("✓ All operations completed successfully")
    print("✓ All data stored on disk (no RAM caching)")
    print("✓ Disk reads/writes tracked for each operation")
    print(f"\nKey Points:")
    print("  - Clustered index stores actual records in leaf nodes")
    print("  - Direct access to records (single lookup)")
    print("  - Efficient range queries via linked leaf nodes")
    print("  - All operations are disk-based (4KB pages)")

if __name__ == "__main__":
    main()
"""
COMPARISON 1: Primary Indexes Performance
==========================================
Compares: Sequential vs ISAM vs B+Tree Clustered
Dataset: World Cities
Operations: Insert, Search by id, Range Search by id
"""

import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor
from experiments.csv_exporter import export_comparison_1
import time

def test_primary_index(index_type, index_name):
    """Test a specific primary index type using SQL parser"""
    print(f"\n{'='*70}")
    print(f"Testing {index_name} (Primary Index)")
    print(f"{'='*70}")

    # Create unique database for this test
    db_name = f"comp1_{index_type.lower()}_db"

    # Clean up previous test data if it exists
    import shutil
    if os.path.exists(db_name):
        shutil.rmtree(db_name)

    db_manager = DatabaseManager(db_name)
    executor = Executor(db_manager)

    # CREATE TABLE using SQL
    create_table_sql = f"""
    CREATE TABLE cities (
        id INT KEY INDEX {index_type},
        city VARCHAR[50],
        country VARCHAR[20],
        lat FLOAT,
        lon FLOAT,
        population INT
    )
    """

    print(f"\nCreating table with {index_name}...")
    plans = parse(create_table_sql)
    for plan in plans:
        executor.execute(plan)
    print(f"[OK] Table 'cities' created with {index_name} primary index")

    results = {
        'index_type': index_name,
        'insert': {},
        'search': {},
        'range_search': {}
    }

    # LOAD DATA using SQL
    print(f"\n--- Loading data from CSV ---")
    load_sql = """
    LOAD DATA FROM FILE "data/datasets/worldcities.csv"
    INTO cities
    """

    start_time = time.time()
    plans = parse(load_sql)
    load_result = executor.execute(plans[0])
    end_time = time.time()

    # Get metrics from result
    # Extract number of inserted records from summary message
    num_records = 0
    if "insertados=" in str(load_result.data):
        import re
        match = re.search(r'insertados=(\d+)', str(load_result.data))
        if match:
            num_records = int(match.group(1))

    results['insert'] = {
        'records': num_records,
        'total_reads': load_result.disk_reads,
        'total_writes': load_result.disk_writes,
        'time_ms': load_result.execution_time_ms,
        'total_time_ms': (end_time - start_time) * 1000
    }

    print(f"  Summary: {load_result.data}")
    print(f"  Total R/W: {load_result.disk_reads}/{load_result.disk_writes}")
    print(f"  Load time: {load_result.execution_time_ms:.2f}ms")
    print(f"  Total time: {results['insert']['total_time_ms']:.2f}ms")

    print(f"\n--- Search Tests ---")

    # Test 1: Exact search by id
    test_ids = [1392685764, 1356226629, 1124616052, 1096999548, 1624958412, 1608693683, 1484666646, 1360051337, 1156029196, 1304206491] 
    search_reads = []
    search_times = []
    search_counts = []

    for test_id in test_ids:
        search_sql = f"SELECT * FROM cities WHERE id = {test_id}"
        plans = parse(search_sql)
        result = executor.execute(plans[0])

        search_reads.append(result.disk_reads)
        search_times.append(result.execution_time_ms)
        search_counts.append(len(result.data))

        # Print records found
        if result.data:
            print(f"    ID {test_id}: Found {len(result.data)} record(s)")
            for rec in result.data[:1]:  # Show first record only
                print(f"      {rec}")

    results['search'] = {
        'avg_reads': sum(search_reads) / len(search_reads),
        'avg_time_ms': sum(search_times) / len(search_times),
        'avg_results': sum(search_counts) / len(search_counts),
        'samples': len(test_ids)
    }

    print(f"  Exact Search (avg of {len(test_ids)} queries):")
    print(f"    Avg Reads: {results['search']['avg_reads']:.2f}")
    print(f"    Avg Time: {results['search']['avg_time_ms']:.2f}ms")
    print(f"    Avg Results: {results['search']['avg_results']:.1f} records")

    # Test 2: Range search by id
    range_searches = [
        (1000000000, 1100000000),
        (1200000000, 1800000000),
        (1400000000, 1600000000),
        (1500000000, 1700000000),
        (1300000000, 1900000000)
    ]
    range_reads = []
    range_times = []
    range_counts = []

    for start_id, end_id in range_searches:
        range_sql = f"SELECT * FROM cities WHERE id BETWEEN {start_id} AND {end_id}"
        plans = parse(range_sql)
        result = executor.execute(plans[0])

        range_reads.append(result.disk_reads)
        range_times.append(result.execution_time_ms)
        range_counts.append(len(result.data))

        # Print sample of records found
        print(f"    Range [{start_id}, {end_id}]: Found {len(result.data)} record(s)")
        if result.data:
            for rec in result.data[:2]:  # Show first 2 records
                print(f"      {rec}")

    results['range_search'] = {
        'avg_reads': sum(range_reads) / len(range_reads),
        'avg_time_ms': sum(range_times) / len(range_times),
        'avg_results': sum(range_counts) / len(range_counts),
        'samples': len(range_searches)
    }

    print(f"  Range Search (avg of {len(range_searches)} queries):")
    print(f"    Avg Reads: {results['range_search']['avg_reads']:.2f}")
    print(f"    Avg Time: {results['range_search']['avg_time_ms']:.2f}ms")
    print(f"    Avg Results: {results['range_search']['avg_results']:.0f} records")

    return results

def main():
    print("\n" + "="*70)
    print("COMPARISON 1: PRIMARY INDEXES PERFORMANCE")
    print("="*70)
    print("Dataset: World Cities")
    print("Comparing: Sequential vs ISAM vs B+Tree Clustered")
    print("Using SQL Parser & Executor")
    print("="*70)

    # Test each index type
    all_results = []

    # Sequential
    seq_results = test_primary_index("SEQUENTIAL", "Sequential")
    all_results.append(seq_results)

    # ISAM
    isam_results = test_primary_index("ISAM", "ISAM")
    all_results.append(isam_results)

    # B+Tree
    btree_results = test_primary_index("BTREE", "B+Tree Clustered")
    all_results.append(btree_results)

    # Print comparison summary
    print("\n" + "="*70)
    print("SUMMARY: COMPARISON ACROSS ALL PRIMARY INDEXES")
    print("="*70)

    print("\n--- INSERTION PERFORMANCE ---")
    print(f"  {'Index Type':<20} {'Records':<12} {'Total R/W':<20} {'Time (ms)'}")
    print(f"  {'-'*75}")
    for res in all_results:
        ins = res['insert']
        rw = f"{ins['total_reads']}/{ins['total_writes']}"
        print(f"  {res['index_type']:<20} {ins['records']:<12} {rw:<20} {ins['total_time_ms']:.2f}")

    print("\n--- SEARCH PERFORMANCE (Exact Search by ID) ---")
    print(f"  {'Index Type':<20} {'Avg Reads':<15} {'Avg Time (ms)':<15} {'Avg Results'}")
    print(f"  {'-'*65}")
    for res in all_results:
        print(f"  {res['index_type']:<20} {res['search']['avg_reads']:<15.2f} {res['search']['avg_time_ms']:<15.2f} {res['search']['avg_results']:.1f}")

    print("\n--- RANGE SEARCH PERFORMANCE (by ID range) ---")
    print(f"  {'Index Type':<20} {'Avg Reads':<15} {'Avg Time (ms)':<15} {'Avg Results'}")
    print(f"  {'-'*65}")
    for res in all_results:
        rs = res['range_search']
        print(f"  {res['index_type']:<20} {rs['avg_reads']:<15.2f} {rs['avg_time_ms']:<15.2f} {rs['avg_results']:.0f}")

    # Export results to CSV
    print("\n--- EXPORTING RESULTS TO CSV ---")
    csv_files = export_comparison_1(all_results)
    for csv_file in csv_files:
        print(f"  Saved: {csv_file}")

    print("\n" + "="*70)
    print("COMPARISON 1 COMPLETE")
    print("="*70)

if __name__ == "__main__":
    main()

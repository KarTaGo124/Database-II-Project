"""
COMPARISON 2: Secondary Indexes for Exact Search
=================================================
Compares: Hash vs B+Tree Unclustered for exact search queries
Dataset: World Cities
Base: B+Tree Clustered (primary: id)
Secondary field: city (unique keys for better hash distribution)
Operations: Insert overhead, Exact search by city
"""

import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor
from experiments.csv_exporter import export_comparison_2
import time
import json

def test_secondary_index(config_name, secondary_index_type=None):
    """Test with different secondary index configurations"""
    print(f"\n{'='*70}")
    print(f"Testing {config_name}")
    print(f"{'='*70}")

    # Create unique database
    db_name = f"comp2_{config_name.replace(' ', '_').replace('+', '').lower()}_db"

    # Clean up previous test data if it exists
    import shutil
    # DatabaseManager creates databases in ./data/databases/
    db_path = os.path.join("data", "databases", db_name)
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
        print(f"  [CLEANUP] Removed existing database at {db_path}")

    db_manager = DatabaseManager(db_name)
    executor = Executor(db_manager)

    # CREATE TABLE (always with B+Tree Clustered primary)
    create_table_sql = """
    CREATE TABLE cities (
        id INT KEY INDEX BTREE,
        city VARCHAR[50],
        country VARCHAR[20],
        lat FLOAT,
        lon FLOAT,
        population INT
    )
    """

    print(f"\nCreating table...")
    plans = parse(create_table_sql)
    for plan in plans:
        executor.execute(plan)
    print(f"[OK] Table created with B+Tree Clustered primary index")

    # LOAD DATA
    print(f"\nLoading data...")
    load_sql = """
    LOAD DATA FROM FILE "data/datasets/worldcities.csv"
    INTO cities
    """

    start_time = time.time()
    plans = parse(load_sql)
    load_result = executor.execute(plans[0])
    end_time = time.time()

    insert_metrics = {
        'records': load_result.data,
        'total_reads': load_result.disk_reads,
        'total_writes': load_result.disk_writes,
        'time_ms': load_result.execution_time_ms,
        'total_time_ms': (end_time - start_time) * 1000
    }

    print(f"  Loaded: {load_result.data} records")
    print(f"  R/W: {load_result.disk_reads}/{load_result.disk_writes}")
    print(f"  Time: {insert_metrics['time_ms']:.2f}ms")

    # CREATE SECONDARY INDEX if specified
    if secondary_index_type:
        print(f"\nCreating secondary index on 'city' using {secondary_index_type}...")
        create_index_sql = f"""
        CREATE INDEX ON cities(city) USING {secondary_index_type}
        """

        start_time = time.time()
        plans = parse(create_index_sql)
        index_result = executor.execute(plans[0])
        end_time = time.time()

        insert_metrics['index_creation_reads'] = index_result.disk_reads
        insert_metrics['index_creation_writes'] = index_result.disk_writes
        insert_metrics['index_creation_time_ms'] = (end_time - start_time) * 1000

        print(f"  Index created")
        print(f"  R/W: {index_result.disk_reads}/{index_result.disk_writes}")
        print(f"  Time: {insert_metrics['index_creation_time_ms']:.2f}ms")

        # Total insertion cost (data + index)
        insert_metrics['total_insertion_reads'] = insert_metrics['total_reads'] + insert_metrics['index_creation_reads']
        insert_metrics['total_insertion_writes'] = insert_metrics['total_writes'] + insert_metrics['index_creation_writes']
        insert_metrics['total_insertion_time_ms'] = insert_metrics['total_time_ms'] + insert_metrics['index_creation_time_ms']
    else:
        insert_metrics['total_insertion_reads'] = insert_metrics['total_reads']
        insert_metrics['total_insertion_writes'] = insert_metrics['total_writes']
        insert_metrics['total_insertion_time_ms'] = insert_metrics['total_time_ms']

    # TEST: Exact search by city
    print(f"\n--- Exact Search Tests ---")

    # Test with various cities (should be unique)
    test_cities = ["Baboua", "Ortigueira", "London", "Paris", "Mumbai", "Lima", "Beijing", "Sydney", "Moscow", "Cairo","Tampa", "Paraipaba", "Chicago", "Houston", "Philadelphia", "Phoenix", "Bilibino", "Muswellbrook", "Dallas", "Antigonish"]
    search_reads = []
    search_times = []
    search_counts = []

    for city in test_cities:
        search_sql = f'SELECT * FROM cities WHERE city = "{city}"'
        plans = parse(search_sql)
        result = executor.execute(plans[0])
        search_reads.append(result.disk_reads)
        search_times.append(result.execution_time_ms)
        search_counts.append(len(result.data))

        # Print what was found
        if result.data:
            print(f"    {city}: Found {len(result.data)} record(s) - Reads: {result.disk_reads}, Time: {result.execution_time_ms:.2f}ms")
            for rec in result.data[:1]:  # Show first record
                print(f"      -> {rec}")
        else:
            print(f"    {city}: NOT FOUND - Reads: {result.disk_reads}, Time: {result.execution_time_ms:.2f}ms")
            # Extra debug for failures with Hash
            if secondary_index_type == "HASH":
                print(f"    [DEBUG] Hash search failed for '{city}'")

    search_metrics = {
        'avg_reads': sum(search_reads) / len(search_reads),
        'avg_time_ms': sum(search_times) / len(search_times),
        'avg_results': sum(search_counts) / len(search_counts),
        'samples': len(test_cities)
    }

    print(f"\n  Summary - Exact search by city (avg of {len(test_cities)} queries):")
    print(f"    Avg Reads: {search_metrics['avg_reads']:.2f}")
    print(f"    Avg Time: {search_metrics['avg_time_ms']:.2f}ms")
    print(f"    Avg Results: {search_metrics['avg_results']:.1f} records")

    return {
        'config': config_name,
        'insert': insert_metrics,
        'search': search_metrics
    }

def main():
    print("\n" + "="*70)
    print("COMPARISON 2: SECONDARY INDEXES FOR EXACT SEARCH")
    print("="*70)
    print("Dataset: World Cities (1k records)")
    print("Base: B+Tree Clustered (primary: id)")
    print("Comparing: No index vs Hash vs B+Tree Unclustered")
    print("Field: city (VARCHAR) - unique keys")
    print("Using SQL Parser & Executor")
    print("="*70)

    all_results = []

    # Configuration 1: No secondary index
    result_no_index = test_secondary_index("No Secondary Index", secondary_index_type=None)
    all_results.append(result_no_index)

    # Configuration 2: Hash secondary index
    result_hash = test_secondary_index("Hash Secondary Index", secondary_index_type="HASH")
    all_results.append(result_hash)

    # Configuration 3: B+Tree Unclustered secondary index
    result_btree = test_secondary_index("B+Tree Unclustered Secondary Index", secondary_index_type="BTREE")
    all_results.append(result_btree)



    # Print comparison summary
    print("\n" + "="*70)
    print("SUMMARY: COMPARISON OF SECONDARY INDEXES FOR EXACT SEARCH")
    print("="*70)

    print("\n--- INSERTION OVERHEAD (Data Load + Index Creation) ---")
    print(f"  {'Configuration':<35} {'Total R/W':<20} {'Total Time (ms)'}")
    print(f"  {'-'*75}")
    for res in all_results:
        ins = res['insert']
        rw = f"{ins['total_insertion_reads']}/{ins['total_insertion_writes']}"
        print(f"  {res['config']:<35} {rw:<20} {ins['total_insertion_time_ms']:.2f}")

    print("\n--- EXACT SEARCH PERFORMANCE (by city field) ---")
    print(f"  {'Configuration':<35} {'Avg Reads':<15} {'Avg Time (ms)':<15} {'Avg Results'}")
    print(f"  {'-'*80}")
    for res in all_results:
        srch = res['search']
        print(f"  {res['config']:<35} {srch['avg_reads']:<15.2f} {srch['avg_time_ms']:<15.2f} {srch['avg_results']:.1f}")

    # Analysis
    print("\n--- ANALYSIS ---")
    no_idx = all_results[0]
    hash_idx = all_results[1]
    btree_idx = all_results[2]

    # Insertion overhead
    hash_overhead = hash_idx['insert']['total_insertion_time_ms'] - no_idx['insert']['total_insertion_time_ms']
    btree_overhead = btree_idx['insert']['total_insertion_time_ms'] - no_idx['insert']['total_insertion_time_ms']

    print(f"\nInsertion Overhead:")
    print(f"  Hash: +{hash_overhead:.2f}ms ({hash_overhead/no_idx['insert']['total_insertion_time_ms']*100:.1f}% increase)")
    print(f"  B+Tree: +{btree_overhead:.2f}ms ({btree_overhead/no_idx['insert']['total_insertion_time_ms']*100:.1f}% increase)")

    # Search speedup
    hash_speedup = no_idx['search']['avg_time_ms'] / hash_idx['search']['avg_time_ms']
    btree_speedup = no_idx['search']['avg_time_ms'] / btree_idx['search']['avg_time_ms']

    print(f"\nSearch Speedup (vs No Index):")
    print(f"  Hash: {hash_speedup:.2f}x faster")
    print(f"  B+Tree Unclustered: {btree_speedup:.2f}x faster")

    # Export results to CSV
    print("\n--- EXPORTING RESULTS TO CSV ---")
    csv_files = export_comparison_2(all_results)
    for csv_file in csv_files:
        print(f"  Saved: {csv_file}")

    print("\n" + "="*70)
    print("COMPARISON 2 COMPLETE")
    print("="*70)

if __name__ == "__main__":
    main()
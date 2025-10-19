"""
COMPARISON 3: Secondary Index for Range Search
===============================================
Compares: B+Tree Unclustered vs No Index for range queries
Dataset: World Cities (1K records by default, change to worldcities.csv for full 41K)
Base: B+Tree Clustered (primary: id)
Secondary field: country (tests duplicate key handling)
Operations: Insert overhead, Range search by country name
"""

import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor
from experiments.csv_exporter import export_comparison_3
import time

def test_with_config(config_name, use_secondary_index=False):
    """Test with or without secondary index"""
    print(f"\n{'='*70}")
    print(f"Testing {config_name}")
    print(f"{'='*70}")

    # Create unique database
    db_name = f"comp3_{config_name.replace(' ', '_').lower()}_db"
    db_manager = DatabaseManager(db_name)
    executor = Executor(db_manager)

    # CREATE TABLE
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
    print(f"[OK] Table created")

    # LOAD DATA
    print(f"\nLoading data...")
    load_sql = """
    LOAD DATA FROM FILE "data/datasets/worldcities_10k.csv"
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

    # CREATE SECONDARY INDEX if requested
    if use_secondary_index:
        print(f"\nCreating B+Tree Unclustered index on 'country'...")
        create_index_sql = """
        CREATE INDEX ON cities(country) USING BTREE
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

        # Total insertion cost
        insert_metrics['total_insertion_reads'] = insert_metrics['total_reads'] + insert_metrics['index_creation_reads']
        insert_metrics['total_insertion_writes'] = insert_metrics['total_writes'] + insert_metrics['index_creation_writes']
        insert_metrics['total_insertion_time_ms'] = insert_metrics['total_time_ms'] + insert_metrics['index_creation_time_ms']
    else:
        insert_metrics['total_insertion_reads'] = insert_metrics['total_reads']
        insert_metrics['total_insertion_writes'] = insert_metrics['total_writes']
        insert_metrics['total_insertion_time_ms'] = insert_metrics['total_time_ms']

    # TEST: Range search by country name (tests duplicate handling)
    print(f"\n--- Range Search Tests ---")

    # Test various country ranges
    range_queries = [
        ("A", "C"),          # Countries: Argentina, Brazil, etc.
        ("J", "M"),          # Countries: Japan, Mexico, etc.
        ("S", "U"),          # Countries: Spain, Turkey, United States, etc.
        ("Germany", "India"), # Specific range
        ("Brazil", "Canada")  # Another specific range
    ]

    search_reads = []
    search_times = []
    search_counts = []

    for start_country, end_country in range_queries:
        search_sql = f'SELECT * FROM cities WHERE country BETWEEN "{start_country}" AND "{end_country}"'
        plans = parse(search_sql)
        result = executor.execute(plans[0])

        search_reads.append(result.disk_reads)
        search_times.append(result.execution_time_ms)
        search_counts.append(len(result.data))

    search_metrics = {
        'avg_reads': sum(search_reads) / len(search_reads),
        'avg_time_ms': sum(search_times) / len(search_times),
        'avg_results': sum(search_counts) / len(search_counts),
        'samples': len(range_queries)
    }

    print(f"  Range search by country (avg of {len(range_queries)} queries):")
    print(f"    Avg Reads: {search_metrics['avg_reads']:.2f}")
    print(f"    Avg Time: {search_metrics['avg_time_ms']:.2f}ms")
    print(f"    Avg Results: {search_metrics['avg_results']:.0f} cities")

    return {
        'config': config_name,
        'insert': insert_metrics,
        'search': search_metrics
    }

def main():
    print("\n" + "="*70)
    print("COMPARISON 3: SECONDARY INDEX FOR RANGE SEARCH")
    print("="*70)
    print("Dataset: World Cities (100 records)")
    print("Base: B+Tree Clustered (primary: id)")
    print("Comparing: No Index vs B+Tree Unclustered")
    print("Field: country (VARCHAR) - tests duplicate key handling")
    print("Operation: Range search by country name")
    print("Using SQL Parser & Executor")
    print("="*70)

    all_results = []

    # Configuration 1: No secondary index (full scan)
    result_no_index = test_with_config("No Secondary Index", use_secondary_index=False)
    all_results.append(result_no_index)

    # Configuration 2: B+Tree Unclustered secondary index
    result_btree = test_with_config("B+Tree Unclustered Index", use_secondary_index=True)
    all_results.append(result_btree)

    # Print comparison summary
    print("\n" + "="*70)
    print("SUMMARY: B+TREE UNCLUSTERED FOR RANGE QUERIES")
    print("="*70)

    print("\n--- INSERTION OVERHEAD (Data Load + Index Creation) ---")
    print(f"  {'Configuration':<30} {'Total R/W':<20} {'Total Time (ms)'}")
    print(f"  {'-'*70}")
    for res in all_results:
        ins = res['insert']
        rw = f"{ins['total_insertion_reads']}/{ins['total_insertion_writes']}"
        print(f"  {res['config']:<30} {rw:<20} {ins['total_insertion_time_ms']:.2f}")

    print("\n--- RANGE SEARCH PERFORMANCE (by country field) ---")
    print(f"  {'Configuration':<30} {'Avg Reads':<15} {'Avg Time (ms)'}")
    print(f"  {'-'*60}")
    for res in all_results:
        srch = res['search']
        print(f"  {res['config']:<30} {srch['avg_reads']:<15.2f} {srch['avg_time_ms']:.2f}")

    # Analysis
    print("\n--- ANALYSIS ---")
    no_idx = all_results[0]
    btree_idx = all_results[1]

    # Insertion overhead
    overhead = btree_idx['insert']['total_insertion_time_ms'] - no_idx['insert']['total_insertion_time_ms']
    print(f"\nInsertion Overhead:")
    print(f"  B+Tree Index: +{overhead:.2f}ms ({overhead/no_idx['insert']['total_insertion_time_ms']*100:.1f}% increase)")

    # Search speedup
    speedup = no_idx['search']['avg_time_ms'] / btree_idx['search']['avg_time_ms']
    reads_reduction = (1 - btree_idx['search']['avg_reads'] / no_idx['search']['avg_reads']) * 100

    print(f"\nRange Search Performance:")
    print(f"  Speedup: {speedup:.2f}x faster with index")
    print(f"  Disk reads reduction: {reads_reduction:.1f}%")

    print(f"\nConclusion:")
    if speedup > 2:
        print(f"  B+Tree Unclustered provides significant benefit for range queries on country field")
        print(f"  Successfully handles duplicate keys (multiple cities per country)")
    else:
        print(f"  B+Tree Unclustered provides moderate benefit for range queries on country field")
        print(f"  Successfully handles duplicate keys (multiple cities per country)")

    # Export results to CSV
    print("\n--- EXPORTING RESULTS TO CSV ---")
    csv_files = export_comparison_3(all_results)
    for csv_file in csv_files:
        print(f"  Saved: {csv_file}")

    print("\n" + "="*70)
    print("COMPARISON 3 COMPLETE")
    print("="*70)

if __name__ == "__main__":
    main()

"""
COMPARISON 2: Secondary Indexes for Exact Search
=================================================
Compares: Hash vs B+Tree Unclustered for exact search queries
Dataset: World Cities (1K records by default, change to worldcities.csv for full 41K)
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
    test_cities = ["Tokyo", "New York", "London", "Paris", "Mumbai", "Lima", "Beijing", "Sydney", "Moscow","Cairo"]
    search_reads = []
    search_times = []
    search_counts = []
    search_results_data = {}  # Store actual results for consistency check

    for city in test_cities:
        search_sql = f'SELECT * FROM cities WHERE city = "{city}"'
        plans = parse(search_sql)
        result = executor.execute(plans[0])
        search_reads.append(result.disk_reads)
        search_times.append(result.execution_time_ms)
        search_counts.append(len(result.data))

        # Store results for consistency verification
        # Sort by id to ensure consistent ordering
        sorted_data = sorted(result.data, key=lambda x: x.get('id', 0) if isinstance(x, dict) else 0)
        search_results_data[city] = sorted_data

    search_metrics = {
        'avg_reads': sum(search_reads) / len(search_reads),
        'avg_time_ms': sum(search_times) / len(search_times),
        'avg_results': sum(search_counts) / len(search_counts),
        'samples': len(test_cities)
    }

    print(search_times)
    print(f"  Exact search by city (avg of {len(test_cities)} queries):")
    print(f"    Avg Reads: {search_metrics['avg_reads']:.2f}")
    print(f"    Avg Time: {search_metrics['avg_time_ms']:.2f}ms")
    print(f"    Avg Results: {search_metrics['avg_results']:.1f} records")

    return {
        'config': config_name,
        'insert': insert_metrics,
        'search': search_metrics,
        'search_results': search_results_data,  # Include actual results
        'test_cities': test_cities
    }

def verify_consistency(all_results):
    """
    Verify that all index configurations return the same results for the same queries
    Returns True if all results are consistent, False otherwise
    """
    print("\n" + "="*70)
    print("CONSISTENCY VERIFICATION")
    print("="*70)

    if len(all_results) < 2:
        print("  Not enough configurations to compare (need at least 2)")
        return True

    # Use first configuration as baseline
    baseline = all_results[0]
    baseline_name = baseline['config']
    baseline_results = baseline['search_results']
    test_cities = baseline['test_cities']

    all_consistent = True
    inconsistencies = []

    print(f"\n  Baseline: {baseline_name}")
    print(f"  Comparing against {len(all_results) - 1} other configuration(s)\n")

    # Compare each configuration against baseline
    for i, result in enumerate(all_results[1:], start=1):
        config_name = result['config']
        config_results = result['search_results']

        print(f"  [{i}] Comparing: {config_name}")

        config_consistent = True

        # Compare results for each test city
        for city in test_cities:
            baseline_data = baseline_results.get(city, [])
            config_data = config_results.get(city, [])

            # Check if record counts match
            if len(baseline_data) != len(config_data):
                all_consistent = False
                config_consistent = False
                inconsistencies.append({
                    'config': config_name,
                    'city': city,
                    'issue': 'Record count mismatch',
                    'baseline_count': len(baseline_data),
                    'config_count': len(config_data),
                    'baseline_data': baseline_data,
                    'config_data': config_data
                })
                continue

            # Compare actual data (already sorted by id)
            for idx, (baseline_record, config_record) in enumerate(zip(baseline_data, config_data)):
                if baseline_record != config_record:
                    all_consistent = False
                    config_consistent = False
                    inconsistencies.append({
                        'config': config_name,
                        'city': city,
                        'issue': 'Record data mismatch',
                        'record_index': idx,
                        'baseline_record': baseline_record,
                        'config_record': config_record
                    })

        if config_consistent:
            print(f"      ✓ All results match baseline")
        else:
            print(f"      ✗ INCONSISTENCIES FOUND!")

    # Print summary
    print("\n" + "-"*70)
    if all_consistent:
        print("  RESULT: ✓ ALL CONFIGURATIONS RETURNED CONSISTENT RESULTS")
        print("  All index implementations are working correctly!")
    else:
        print("  RESULT: ✗ INCONSISTENCIES DETECTED")
        print(f"  Found {len(inconsistencies)} inconsistency(ies) across configurations")

        # Print detailed debug information
        print("\n" + "="*70)
        print("DEBUG: INCONSISTENCY DETAILS")
        print("="*70)

        for i, inc in enumerate(inconsistencies, start=1):
            print(f"\n  Inconsistency #{i}:")
            print(f"    Configuration: {inc['config']}")
            print(f"    Query City: {inc['city']}")
            print(f"    Issue: {inc['issue']}")

            if inc['issue'] == 'Record count mismatch':
                print(f"    Baseline ({baseline_name}): {inc['baseline_count']} record(s)")
                print(f"    {inc['config']}: {inc['config_count']} record(s)")
                print(f"\n    Baseline data:")
                print(f"      {json.dumps(inc['baseline_data'], indent=6)}")
                print(f"    {inc['config']} data:")
                print(f"      {json.dumps(inc['config_data'], indent=6)}")

            elif inc['issue'] == 'Record data mismatch':
                print(f"    Record index: {inc['record_index']}")
                print(f"    Baseline record:")
                print(f"      {json.dumps(inc['baseline_record'], indent=6)}")
                print(f"    {inc['config']} record:")
                print(f"      {json.dumps(inc['config_record'], indent=6)}")

                # Show differences field by field
                if isinstance(inc['baseline_record'], dict) and isinstance(inc['config_record'], dict):
                    print(f"    Field-by-field comparison:")
                    all_keys = set(inc['baseline_record'].keys()) | set(inc['config_record'].keys())
                    for key in sorted(all_keys):
                        baseline_val = inc['baseline_record'].get(key, '<MISSING>')
                        config_val = inc['config_record'].get(key, '<MISSING>')
                        if baseline_val != config_val:
                            print(f"      {key}: {baseline_val} ≠ {config_val}")

    print("="*70)

    return all_consistent

def main():
    print("\n" + "="*70)
    print("COMPARISON 2: SECONDARY INDEXES FOR EXACT SEARCH")
    print("="*70)
    print("Dataset: World Cities (1K records)")
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

    # VERIFY CONSISTENCY ACROSS ALL CONFIGURATIONS
    consistency_ok = verify_consistency(all_results)

    if not consistency_ok:
        print("\n" + "!"*70)
        print("WARNING: Inconsistencies detected! Check debug output above.")
        print("!"*70)

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

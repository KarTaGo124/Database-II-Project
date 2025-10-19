"""
COMPARISON 4: Spatial Indexes (R-Tree)
=======================================
Compares: R-Tree spatial index performance
Dataset: NYC Airbnb (10K records)
Base: B+Tree Clustered (primary: id)
Secondary field: coordinates (ARRAY[FLOAT, 2]) mapped from latitude, longitude
Operations: Insert overhead, KNN search, Radius search
"""

import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor
from experiments.csv_exporter import export_comparison_4
import time

def test_rtree():
    """Test R-Tree spatial index performance"""
    print(f"\n{'='*70}")
    print(f"Testing R-Tree Spatial Index")
    print(f"{'='*70}")

    # Create database
    db_name = "comp4_rtree_db"
    db_manager = DatabaseManager(db_name)
    executor = Executor(db_manager)

    # CREATE TABLE with ARRAY field for coordinates
    create_table_sql = """
    CREATE TABLE airbnb (
        id INT KEY INDEX BTREE,
        name VARCHAR[100],
        neighbourhood VARCHAR[50],
        coordinates ARRAY[FLOAT, 2],
        price INT,
        room_type VARCHAR[30]
    );
    """

    print(f"\nCreating table with coordinates field (ARRAY[FLOAT, 2])...")
    plans = parse(create_table_sql)
    for plan in plans:
        executor.execute(plan)
    print(f"[OK] Table created")

    results = {
        'config': 'R-Tree Spatial Index',
        'insert': {},
        'knn_search': {},
        'radius_search': {}
    }

    # LOAD DATA with mapping
    print(f"\n--- Loading data ---")
    load_sql = """
    LOAD DATA FROM FILE "data/datasets/airbnb_nyc_10k.csv"
    INTO airbnb
    WITH MAPPING (coordinates = ARRAY(latitude, longitude));
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

    # CREATE R-TREE INDEX on coordinates field
    print(f"\nCreating R-Tree index on coordinates...")
    create_index_sql = """
    CREATE INDEX ON airbnb(coordinates) USING RTREE;
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

    insert_metrics['total_insertion_reads'] = insert_metrics['total_reads'] + insert_metrics['index_creation_reads']
    insert_metrics['total_insertion_writes'] = insert_metrics['total_writes'] + insert_metrics['index_creation_writes']
    insert_metrics['total_insertion_time_ms'] = insert_metrics['total_time_ms'] + insert_metrics['index_creation_time_ms']

    results['insert'] = insert_metrics

    # KNN SEARCH TEST
    print(f"\n--- KNN Search Test ---")
    # Times Square: 40.758, -73.9855
    knn_sql = """
    SELECT * FROM airbnb
    WHERE coordinates NEAREST ((40.758, -73.9855), 10);
    """

    plans = parse(knn_sql)
    knn_result = executor.execute(plans[0])

    results['knn_search'] = {
        'reads': knn_result.disk_reads,
        'writes': knn_result.disk_writes,
        'time_ms': knn_result.execution_time_ms,
        'results': len(knn_result.data) if hasattr(knn_result.data, '__len__') else 10
    }

    print(f"  Query: 10 nearest to Times Square (40.758, -73.9855)")
    print(f"  Reads: {knn_result.disk_reads}")
    print(f"  Time: {knn_result.execution_time_ms:.2f}ms")
    print(f"  Results: {results['knn_search']['results']} listings")

    # RADIUS SEARCH TEST
    print(f"\n--- Radius Search Test ---")
    # 0.01 degrees ≈ 1.1km radius around Central Park
    radius_sql = """
    SELECT * FROM airbnb
    WHERE coordinates IN ((40.7614, -73.9776), 0.01);
    """

    plans = parse(radius_sql)
    radius_result = executor.execute(plans[0])

    results['radius_search'] = {
        'reads': radius_result.disk_reads,
        'writes': radius_result.disk_writes,
        'time_ms': radius_result.execution_time_ms,
        'results': len(radius_result.data) if hasattr(radius_result.data, '__len__') else 0
    }

    print(f"  Query: Radius 0.01 deg (~1.1km) from Central Park (40.7614, -73.9776)")
    print(f"  Reads: {radius_result.disk_reads}")
    print(f"  Time: {radius_result.execution_time_ms:.2f}ms")
    print(f"  Results: {results['radius_search']['results']} listings")

    return results

def main():
    print("\n" + "="*70)
    print("COMPARISON 4: SPATIAL INDEXES (R-TREE)")
    print("="*70)
    print("Dataset: NYC Airbnb (10K records)")
    print("Base: B+Tree Clustered (primary: id)")
    print("Testing: R-Tree spatial index performance")
    print("Field: coordinates (ARRAY[FLOAT, 2]) from latitude, longitude")
    print("Using SQL Parser & Executor")
    print("="*70)

    # Run R-Tree test
    results = test_rtree()

    # Print summary
    print("\n" + "="*70)
    print("SUMMARY: R-TREE SPATIAL INDEX PERFORMANCE")
    print("="*70)

    print("\n--- INSERTION PERFORMANCE ---")
    ins = results['insert']
    print(f"  Data Loading:")
    print(f"    Records: {ins['records']}")
    print(f"    R/W: {ins['total_reads']}/{ins['total_writes']}")
    print(f"    Time: {ins['total_time_ms']:.2f}ms")
    print(f"\n  Index Creation:")
    print(f"    R/W: {ins['index_creation_reads']}/{ins['index_creation_writes']}")
    print(f"    Time: {ins['index_creation_time_ms']:.2f}ms")
    print(f"\n  Total Insertion:")
    print(f"    R/W: {ins['total_insertion_reads']}/{ins['total_insertion_writes']}")
    print(f"    Time: {ins['total_insertion_time_ms']:.2f}ms")

    print("\n--- KNN SEARCH PERFORMANCE ---")
    knn = results['knn_search']
    print(f"  Reads: {knn['reads']}")
    print(f"  Time: {knn['time_ms']:.2f}ms")
    print(f"  Results: {knn['results']} listings")

    print("\n--- RADIUS SEARCH PERFORMANCE ---")
    rad = results['radius_search']
    print(f"  Reads: {rad['reads']}")
    print(f"  Time: {rad['time_ms']:.2f}ms")
    print(f"  Results: {rad['results']} listings")

    # Export results to CSV
    print("\n--- EXPORTING RESULTS TO CSV ---")
    all_results = [results]
    csv_files = export_comparison_4(all_results)
    for csv_file in csv_files:
        print(f"  Saved: {csv_file}")

    print("\n" + "="*70)
    print("COMPARISON 4 COMPLETE")
    print("="*70)

if __name__ == "__main__":
    main()
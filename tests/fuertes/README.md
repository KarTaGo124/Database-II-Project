# Comprehensive Index Performance Tests

This test suite provides exhaustive performance comparisons for all database index structures across 5 different datasets.

## Overview

The test suite evaluates the following index structures:

### Primary Indices
- **ISAM** (Indexed Sequential Access Method)
- **BTREE** (B+ Tree Clustered)
- **SEQUENTIAL** (Sequential File)

### Secondary Indices
- **BTREE** (B+ Tree Unclustered)
- **HASH** (Extendible Hashing)
- **RTREE** (R-Tree for spatial data)

## Datasets

### 1. U.S. Airbnb Open Data (226K+ records)
**File**: `U.S. Airbnb Open Data/AB_US_2023.csv`

**Recommended Index Combinations**:
- Primary: ISAM, BTREE, SEQUENTIAL
- Secondary BTREE: price, number_of_reviews, availability_365
- Secondary HASH: city, room_type, state
- Secondary RTREE: (latitude, longitude) for spatial queries

**Test Operations**:
- Exact search on city, room_type
- Range search on price, number_of_reviews
- Spatial queries on coordinates

---

### 2. NYC Airbnb Open Data (48,895 records)
**File**: `NYC Airbnb Open Data/AB_NYC_2019.csv`

**Recommended Index Combinations**:
- Primary: ISAM, BTREE
- Secondary BTREE: price, minimum_nights, number_of_reviews
- Secondary HASH: neighbourhood_group, neighbourhood, room_type
- Secondary RTREE: (latitude, longitude) - dense spatial data

**Test Operations**:
- Exact search on neighbourhood_group, room_type
- Range search on price, minimum_nights
- Spatial queries in NYC area (dense point distribution)

---

### 3. World Cities Database (41,000 records)
**File**: `World Cities Database/worldcities.csv`

**Recommended Index Combinations**:
- Primary: ISAM, BTREE
- Secondary BTREE: population, city (alphabetical)
- Secondary HASH: country, iso2, iso3
- Secondary RTREE: (lat, lon) - global spatial distribution

**Test Operations**:
- Exact search on country, iso2
- Range search on population
- Global spatial queries (sparse point distribution)

---

### 4. Bank Marketing Dataset (41,188 records)
**File**: `Bank Marketing Dataset/bank-additional/bank-additional-full.csv`

**Recommended Index Combinations**:
- Primary: ISAM, BTREE, SEQUENTIAL
- Secondary BTREE: age, balance, duration, campaign
- Secondary HASH: job, marital, education, month
- **NO RTREE** (no spatial data)

**Test Operations**:
- Exact search on job, marital, education
- Range search on age, balance, duration
- Perfect for comparing traditional indices without spatial component

---

### 5. Starbucks Locations Worldwide (25-30K records)
**File**: `Starbucks Locations Worldwide/startbucks.csv`

**Recommended Index Combinations**:
- Primary: ISAM, BTREE
- Secondary HASH: Store ID, Country, Ownership Type
- Secondary RTREE: (Latitude, Longitude) - store locator use case

**Test Operations**:
- Exact search on Store ID, Country
- k-NN queries for "find nearest Starbucks"
- Spatial range queries

---

## Installation

### Prerequisites

```bash
pip install pandas matplotlib seaborn numpy
```

### Project Structure

```
tests/fuertes/
├── tests.py                          # Main test suite
├── README.md                         # This file
├── U.S. Airbnb Open Data/
│   └── AB_US_2023.csv
├── NYC Airbnb Open Data/
│   └── AB_NYC_2019.csv
├── World Cities Database/
│   └── worldcities.csv
├── Bank Marketing Dataset/
│   └── bank-additional/
│       └── bank-additional-full.csv
└── Starbucks Locations Worldwide/
    └── startbucks.csv
```

## Running the Tests

### Quick Start (Recommended for initial testing)

```bash
cd tests/fuertes
python tests.py
```

This will run tests with a sample size of **500 records** per dataset for faster execution.

### Full Test Run

To test with larger samples, edit the `main()` function in `tests.py`:

```python
# Change sample_size to desired value
tester.run_all_tests(sample_size=5000)  # 5000 records per dataset
```

### Custom Test Configuration

You can modify test configurations in the dataset config functions:

```python
def create_airbnb_us_config(base_path: str) -> DatasetTestConfig:
    return DatasetTestConfig(
        name="US Airbnb",
        csv_path=os.path.join(base_path, "U.S. Airbnb Open Data/AB_US_2023.csv"),
        primary_key="id",
        fields={...},
        primary_indices=["ISAM", "BTREE", "SEQUENTIAL"],  # Modify this
        secondary_indices={...},  # Modify this
        test_operations={...}  # Modify this
    )
```

## Test Operations Performed

### For Each Primary Index Type
1. **Table Creation** - Measure time to create table with index
2. **Bulk Insert** - Insert all sample records, measure:
   - Average insertion time per record
   - Total disk reads
   - Total disk writes
3. **Point Search** - Search 50 random primary keys, measure:
   - Average search time
   - Average disk reads
4. **Range Search** (BTREE/ISAM only) - Search range of keys, measure:
   - Execution time
   - Disk accesses
   - Number of records retrieved

### For Each Secondary Index Type
1. **Index Creation** - Build secondary index on existing data
2. **Point Search** - Search on secondary field values
3. **Range Search** (BTREE only) - Range queries on secondary fields
4. **Spatial Search** (RTREE only) - Spatial queries (radius, k-NN)

## Output

### Performance Metrics CSV
All test results are saved to:
```
tests/fuertes/test_results/performance_metrics.csv
```

**Columns**:
- `dataset`: Dataset name
- `index_type`: Index type and role (Primary/Secondary)
- `operation`: Operation type (INSERT, SEARCH, RANGE_SEARCH)
- `execution_time_ms`: Execution time in milliseconds
- `disk_reads`: Number of disk read operations
- `disk_writes`: Number of disk write operations
- `total_disk_accesses`: Total disk I/O operations
- `records_count`: Number of records affected

### Generated Graphs

All graphs are saved to `tests/fuertes/test_results/`:

1. **execution_time_by_operation.png**
   - Compares execution time across index types for each operation
   - Grouped by dataset
   - 4 subplots (INSERT, SEARCH, RANGE_SEARCH, DELETE)

2. **disk_accesses_comparison.png**
   - Left: Total disk reads vs writes by index type
   - Right: Total disk accesses by dataset

3. **index_type_summary.png**
   - Average execution time by index type
   - Disk access efficiency (records per disk access)
   - Operation distribution pie chart
   - Index type usage pie chart

4. **dataset_comparison_{dataset_name}.png** (one per dataset)
   - Execution time by operation and index type
   - Total disk reads by index type
   - Total disk writes by index type
   - Performance heatmap

## Understanding the Results

### Key Performance Indicators

**Execution Time**:
- Lower is better
- BTREE typically fastest for searches
- HASH fastest for exact match lookups
- SEQUENTIAL slowest for random access

**Disk Reads**:
- Lower indicates better index structure
- BTREE typically minimizes reads with tree traversal
- HASH provides O(1) access

**Disk Writes**:
- SEQUENTIAL has fewest writes (append-only)
- BTREE has moderate writes (node splits)
- HASH may have high writes (bucket splits)

**Efficiency (Records per Disk Access)**:
- Higher is better
- Measures how much data retrieved per I/O operation
- BTREE typically most efficient for range queries

### Expected Performance Patterns

**Primary Index Comparison**:
- **BTREE**: Balanced performance, best for range queries
- **ISAM**: Fast reads, slower for insertions after initial load
- **SEQUENTIAL**: Fast sequential scans, slow random access

**Secondary Index Comparison**:
- **BTREE**: Best for range queries on secondary fields
- **HASH**: Fastest for exact equality searches
- **RTREE**: Essential for spatial queries (lat/long)

**Dataset Size Impact**:
- Larger datasets (US Airbnb 226K) show clearer performance differences
- Smaller datasets may have similar performance across indices

## Index Combination Recommendations

### Use BTREE Primary When:
- Frequent range queries on primary key
- Need sorted data access
- Balanced insert/search workload

### Use ISAM Primary When:
- Data mostly static after initial load
- Read-heavy workload
- Historical/archival data

### Use SEQUENTIAL Primary When:
- Append-only insertions
- Sequential scanning common
- Low memory environment

### Use BTREE Secondary When:
- Range queries on non-primary fields
- Need ordered results
- Frequent updates

### Use HASH Secondary When:
- Only exact match queries
- High-cardinality fields
- Fast lookup critical

### Use RTREE Secondary When:
- Spatial data (coordinates)
- k-NN queries ("find nearest")
- Geographic range queries

## Troubleshooting

### Out of Memory
Reduce sample size:
```python
tester.run_all_tests(sample_size=100)
```

### Missing CSV File
Ensure all CSV files are in the correct directories as listed above.

### Import Errors
Make sure you're running from the correct directory and the project root is in the Python path.

### Performance Too Slow
- Reduce sample size
- Test fewer datasets
- Disable graph generation temporarily

## Advanced Usage

### Test Single Dataset

```python
from tests import IndexPerformanceTester, create_airbnb_us_config

base_path = "/path/to/tests/fuertes"
tester = IndexPerformanceTester(base_path)

config = create_airbnb_us_config(base_path)
tester.test_primary_index_performance(config, sample_size=1000)
tester.generate_comparison_graphs()
```

### Custom Metrics Analysis

```python
# After running tests
df = tester.metrics.get_dataframe()

# Filter specific index type
btree_metrics = df[df['index_type'].str.contains('BTREE')]

# Calculate custom metrics
btree_metrics['avg_time_per_record'] = (
    btree_metrics['execution_time_ms'] / btree_metrics['records_count']
)

print(btree_metrics[['dataset', 'operation', 'avg_time_per_record']])
```

## Contributing

To add a new dataset:

1. Create a new config function following the pattern of existing ones
2. Add it to `self.configs` in `IndexPerformanceTester.__init__`
3. Ensure CSV file is in the correct location
4. Run tests

## References

- [B+ Tree Documentation](../../indexes/bplus_tree/)
- [ISAM Documentation](../../indexes/isam/)
- [Extendible Hashing Documentation](../../indexes/extendible_hashing/)
- [R-Tree Documentation](../../indexes/r_tree/)
- [Sequential File Documentation](../../indexes/sequential_file/)

## License

This test suite is part of the Database II Project - UTEC 2025-2

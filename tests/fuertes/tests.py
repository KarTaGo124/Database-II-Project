import sys
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table, Record

# Set plotting style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

class PerformanceMetrics:
    """Store and track performance metrics for index operations"""
    def __init__(self):
        self.metrics = []

    def add_metric(self, dataset: str, index_type: str, operation: str,
                   execution_time: float, disk_reads: int, disk_writes: int,
                   records_count: int = 0):
        self.metrics.append({
            'dataset': dataset,
            'index_type': index_type,
            'operation': operation,
            'execution_time_ms': execution_time,
            'disk_reads': disk_reads,
            'disk_writes': disk_writes,
            'total_disk_accesses': disk_reads + disk_writes,
            'records_count': records_count
        })

    def get_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.metrics)

    def clear(self):
        self.metrics = []


class DatasetTestConfig:
    """Configuration for dataset-specific tests"""
    def __init__(self, name: str, csv_path: str, primary_key: str,
                 fields: Dict[str, Tuple[str, int]],
                 primary_indices: List[str],
                 secondary_indices: Dict[str, List[str]],
                 test_operations: Dict[str, Any]):
        self.name = name
        self.csv_path = csv_path
        self.primary_key = primary_key
        self.fields = fields
        self.primary_indices = primary_indices
        self.secondary_indices = secondary_indices
        self.test_operations = test_operations


def create_airbnb_us_config(base_path: str) -> DatasetTestConfig:
    """Dataset 1: U.S. Airbnb Open Data (226K+ registros)"""
    return DatasetTestConfig(
        name="US Airbnb",
        csv_path=os.path.join(base_path, "U.S. Airbnb Open Data/AB_US_2023.csv"),
        primary_key="id",
        fields={
            "id": ("INT", 8),
            "name": ("VARCHAR", 200),
            "host_id": ("INT", 8),
            "host_name": ("VARCHAR", 100),
            "neighbourhood": ("VARCHAR", 100),
            "latitude": ("FLOAT", 8),
            "longitude": ("FLOAT", 8),
            "room_type": ("VARCHAR", 50),
            "price": ("INT", 4),
            "minimum_nights": ("INT", 4),
            "number_of_reviews": ("INT", 4),
            "availability_365": ("INT", 4),
            "city": ("VARCHAR", 100)
        },
        primary_indices=["ISAM", "BTREE", "SEQUENTIAL"],
        secondary_indices={
            "BTREE": ["price", "number_of_reviews"],
            "HASH": ["city", "room_type"],
            "RTREE": []  # Spatial index on lat/long
        },
        test_operations={
            "search": {"price": [100, 150, 200], "city": ["San Francisco", "New York"]},
            "range_search": {"price": [(50, 150), (100, 300)], "number_of_reviews": [(10, 100)]},
            "insert_count": 100,
            "delete_count": 50
        }
    )


def create_airbnb_nyc_config(base_path: str) -> DatasetTestConfig:
    """Dataset 2: NYC Airbnb Open Data (48,895 registros)"""
    return DatasetTestConfig(
        name="NYC Airbnb",
        csv_path=os.path.join(base_path, "NYC Airbnb Open Data/AB_NYC_2019.csv"),
        primary_key="id",
        fields={
            "id": ("INT", 8),
            "name": ("VARCHAR", 200),
            "host_id": ("INT", 8),
            "host_name": ("VARCHAR", 100),
            "neighbourhood_group": ("VARCHAR", 50),
            "neighbourhood": ("VARCHAR", 100),
            "latitude": ("FLOAT", 8),
            "longitude": ("FLOAT", 8),
            "room_type": ("VARCHAR", 50),
            "price": ("INT", 4),
            "minimum_nights": ("INT", 4),
            "number_of_reviews": ("INT", 4),
            "availability_365": ("INT", 4)
        },
        primary_indices=["BTREE"],
        secondary_indices={
            "BTREE": ["price", "minimum_nights"],
            "HASH": ["neighbourhood_group", "room_type"],
            "RTREE": []
        },
        test_operations={
            "search": {"price": [100, 200], "neighbourhood_group": ["Manhattan", "Brooklyn"]},
            "range_search": {"price": [(100, 200)], "minimum_nights": [(1, 3)]},
            "insert_count": 50,
            "delete_count": 25
        }
    )


def create_world_cities_config(base_path: str) -> DatasetTestConfig:
    """Dataset 3: World Cities Database (41,000 registros)"""
    return DatasetTestConfig(
        name="World Cities",
        csv_path=os.path.join(base_path, "World Cities Database/worldcities.csv"),
        primary_key="id",
        fields={
            "id": ("INT", 8),
            "city": ("VARCHAR", 100),
            "city_ascii": ("VARCHAR", 100),
            "lat": ("FLOAT", 8),
            "lon": ("FLOAT", 8),
            "country": ("VARCHAR", 100),
            "iso2": ("VARCHAR", 2),
            "iso3": ("VARCHAR", 3),
            "admin_name": ("VARCHAR", 100),
            "population": ("INT", 8)
        },
        primary_indices=["BTREE"],
        secondary_indices={
            "BTREE": ["population"],
            "HASH": ["country", "iso2"],
            "RTREE": []
        },
        test_operations={
            "search": {"country": ["Japan", "United States"], "iso2": ["US", "JP"]},
            "range_search": {"population": [(100000, 1000000)]},
            "insert_count": 50,
            "delete_count": 25
        }
    )


def create_bank_marketing_config(base_path: str) -> DatasetTestConfig:
    """Dataset 4: Bank Marketing Dataset (41,188 registros) - NO spatial data"""
    return DatasetTestConfig(
        name="Bank Marketing",
        csv_path=os.path.join(base_path, "Bank Marketing Dataset/bank-additional/bank-additional-full.csv"),
        primary_key="row_id",  # Will be added
        fields={
            "row_id": ("INT", 8),
            "age": ("INT", 4),
            "job": ("VARCHAR", 50),
            "marital": ("VARCHAR", 20),
            "education": ("VARCHAR", 50),
            "balance": ("INT", 8),
            "duration": ("INT", 4),
            "campaign": ("INT", 4)
        },
        primary_indices=["ISAM", "BTREE", "SEQUENTIAL"],
        secondary_indices={
            "BTREE": ["age", "balance", "duration"],
            "HASH": ["job", "marital", "education"]
        },
        test_operations={
            "search": {"job": ["admin.", "technician"], "marital": ["married", "single"]},
            "range_search": {"age": [(30, 45)], "balance": [(1000, 5000)]},
            "insert_count": 100,
            "delete_count": 50
        }
    )


def create_starbucks_config(base_path: str) -> DatasetTestConfig:
    """Dataset 5: Starbucks Locations Worldwide (25-30K registros)"""
    return DatasetTestConfig(
        name="Starbucks Locations",
        csv_path=os.path.join(base_path, "Starbucks Locations Worldwide/startbucks.csv"),
        primary_key="storeNumber",
        fields={
            "storeNumber": ("VARCHAR", 50),
            "countryCode": ("VARCHAR", 2),
            "ownershipTypeCode": ("VARCHAR", 10),
            "latitude": ("FLOAT", 8),
            "longitude": ("FLOAT", 8),
            "city": ("VARCHAR", 100),
            "countrySubdivisionCode": ("VARCHAR", 10),
            "postalCode": ("VARCHAR", 20)
        },
        primary_indices=["BTREE"],
        secondary_indices={
            "BTREE": [],
            "HASH": ["countryCode", "ownershipTypeCode"],
            "RTREE": []
        },
        test_operations={
            "search": {"countryCode": ["US", "HK"], "ownershipTypeCode": ["LS"]},
            "range_search": {},
            "insert_count": 50,
            "delete_count": 25
        }
    )


class IndexPerformanceTester:
    """Main testing class for index performance comparison"""

    def __init__(self, base_data_path: str, output_path: str = "./test_results"):
        self.base_data_path = base_data_path
        self.output_path = output_path
        self.metrics = PerformanceMetrics()
        os.makedirs(output_path, exist_ok=True)

        # Initialize all dataset configurations
        self.configs = [
            create_airbnb_us_config(base_data_path),
            create_airbnb_nyc_config(base_data_path),
            create_world_cities_config(base_data_path),
            create_bank_marketing_config(base_data_path),
            create_starbucks_config(base_data_path)
        ]

    def load_sample_data(self, csv_path: str, sample_size: int = None) -> pd.DataFrame:
        """Load CSV data with optional sampling"""
        print(f"Loading data from {csv_path}...")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Read CSV with semicolon separator (common in UCI datasets)
        try:
            df = pd.read_csv(csv_path)
        except:
            df = pd.read_csv(csv_path, sep=';')

        print(f"  Loaded {len(df)} records")

        if sample_size and len(df) > sample_size:
            df = df.sample(n=sample_size, random_state=42)
            print(f"  Sampled down to {len(df)} records")

        return df

    def create_table_from_config(self, config: DatasetTestConfig) -> Table:
        """Create Table object from dataset configuration"""
        # Convert fields dict to list of tuples (field_name, field_type, field_size)
        sql_fields = [(name, ftype, fsize) for name, (ftype, fsize) in config.fields.items()]

        return Table(
            table_name=config.name.replace(" ", "_").lower(),
            sql_fields=sql_fields,
            key_field=config.primary_key
        )

    def test_primary_index_performance(self, config: DatasetTestConfig, sample_size: int = 1000):
        """Test all primary index types for a dataset"""
        print(f"\n{'='*80}")
        print(f"Testing Primary Indices for {config.name}")
        print(f"{'='*80}")

        # Load sample data
        df = self.load_sample_data(config.csv_path, sample_size)

        for index_type in config.primary_indices:
            print(f"\n--- Testing {index_type} as Primary Index ---")

            # Clean up previous test data
            db = DatabaseManager()
            table = self.create_table_from_config(config)
            table_name = table.table_name

            try:
                # Create table with this index type
                start_time = time.time()
                db.create_table(table, primary_index_type=index_type)
                create_time = (time.time() - start_time) * 1000

                print(f"Table created with {index_type} in {create_time:.2f}ms")

                # Test INSERT performance
                insert_times = []
                insert_reads = []
                insert_writes = []

                print(f"Inserting {len(df)} records...")
                for idx, row in df.iterrows():
                    record = self._create_record_from_row(row, config)
                    result = db.insert(table_name, record)

                    insert_times.append(result.execution_time_ms)
                    insert_reads.append(result.disk_reads)
                    insert_writes.append(result.disk_writes)

                    if (idx + 1) % 100 == 0:
                        print(f"  Inserted {idx + 1}/{len(df)} records")

                avg_insert_time = np.mean(insert_times)
                total_insert_reads = sum(insert_reads)
                total_insert_writes = sum(insert_writes)

                self.metrics.add_metric(
                    dataset=config.name,
                    index_type=f"{index_type} (Primary)",
                    operation="INSERT",
                    execution_time=avg_insert_time,
                    disk_reads=total_insert_reads,
                    disk_writes=total_insert_writes,
                    records_count=len(df)
                )

                print(f"  Avg INSERT time: {avg_insert_time:.4f}ms")
                print(f"  Total disk reads: {total_insert_reads}")
                print(f"  Total disk writes: {total_insert_writes}")

                # Test SEARCH performance
                # For generated keys (like row_id), use the DataFrame index
                if config.primary_key not in df.columns:
                    test_keys = df.index[:min(50, len(df))].tolist()
                else:
                    test_keys = df[config.primary_key].sample(min(50, len(df))).tolist()

                search_times = []
                search_reads = []

                print(f"Searching {len(test_keys)} random keys...")
                for key in test_keys:
                    result = db.search(table_name, key)
                    search_times.append(result.execution_time_ms)
                    search_reads.append(result.disk_reads)

                avg_search_time = np.mean(search_times)
                avg_search_reads = np.mean(search_reads)

                self.metrics.add_metric(
                    dataset=config.name,
                    index_type=f"{index_type} (Primary)",
                    operation="SEARCH",
                    execution_time=avg_search_time,
                    disk_reads=int(avg_search_reads),
                    disk_writes=0,
                    records_count=len(test_keys)
                )

                print(f"  Avg SEARCH time: {avg_search_time:.4f}ms")
                print(f"  Avg disk reads: {avg_search_reads:.2f}")

                # Test RANGE SEARCH if supported
                if index_type in ["BTREE", "ISAM"]:
                    try:
                        # Handle generated keys
                        if config.primary_key not in df.columns:
                            sorted_keys = sorted(df.index.tolist())
                        else:
                            sorted_keys = sorted(df[config.primary_key].tolist())

                        mid_point = len(sorted_keys) // 2
                        start_key = sorted_keys[max(0, mid_point - 25)]
                        end_key = sorted_keys[min(len(sorted_keys) - 1, mid_point + 25)]

                        result = db.range_search(table_name, start_key, end_key)

                        self.metrics.add_metric(
                            dataset=config.name,
                            index_type=f"{index_type} (Primary)",
                            operation="RANGE_SEARCH",
                            execution_time=result.execution_time_ms,
                            disk_reads=result.disk_reads,
                            disk_writes=result.disk_writes,
                            records_count=len(result.data)
                        )

                        print(f"  RANGE SEARCH time: {result.execution_time_ms:.4f}ms")
                        print(f"  Records found: {len(result.data)}")
                    except Exception as e:
                        print(f"  Range search not available: {e}")

                # Cleanup
                db.drop_table(table_name)

            except Exception as e:
                print(f"  ERROR testing {index_type}: {e}")
                import traceback
                traceback.print_exc()

    def test_secondary_index_performance(self, config: DatasetTestConfig, sample_size: int = 1000):
        """Test secondary index types for a dataset"""
        print(f"\n{'='*80}")
        print(f"Testing Secondary Indices for {config.name}")
        print(f"{'='*80}")

        # Load sample data
        df = self.load_sample_data(config.csv_path, sample_size)

        # Use BTREE as primary index for secondary tests
        db = DatabaseManager()
        table = self.create_table_from_config(config)
        table_name = table.table_name

        try:
            db.create_table(table, primary_index_type="BTREE")

            # Insert all data first
            print(f"Populating table with {len(df)} records...")
            for idx, row in df.iterrows():
                record = self._create_record_from_row(row, config)
                db.insert(table_name, record)
                if (idx + 1) % 100 == 0:
                    print(f"  Inserted {idx + 1}/{len(df)} records")

            # Test each secondary index type
            for sec_index_type, fields in config.secondary_indices.items():
                if not fields:
                    continue

                for field_name in fields:
                    print(f"\n--- Testing {sec_index_type} on {field_name} ---")

                    try:
                        # Create secondary index
                        start_time = time.time()
                        db.create_index(table_name, field_name, sec_index_type, scan_existing=True)
                        create_time = (time.time() - start_time) * 1000

                        print(f"  Secondary index created in {create_time:.2f}ms")

                        # Test SEARCH on secondary index
                        if field_name in config.test_operations["search"]:
                            test_values = config.test_operations["search"][field_name]

                            search_times = []
                            search_reads = []

                            for value in test_values:
                                result = db.search(table_name, value, field_name=field_name)
                                search_times.append(result.execution_time_ms)
                                search_reads.append(result.disk_reads)
                                print(f"    Search for {value}: {result.execution_time_ms:.4f}ms, {len(result.data)} records")

                            avg_search_time = np.mean(search_times)
                            avg_search_reads = np.mean(search_reads)

                            self.metrics.add_metric(
                                dataset=config.name,
                                index_type=f"{sec_index_type} (Secondary on {field_name})",
                                operation="SEARCH",
                                execution_time=avg_search_time,
                                disk_reads=int(avg_search_reads),
                                disk_writes=0
                            )

                        # Test RANGE SEARCH on secondary index
                        if sec_index_type == "BTREE" and field_name in config.test_operations["range_search"]:
                            ranges = config.test_operations["range_search"][field_name]

                            for start_val, end_val in ranges:
                                result = db.range_search(table_name, start_val, end_val, field_name=field_name)

                                self.metrics.add_metric(
                                    dataset=config.name,
                                    index_type=f"{sec_index_type} (Secondary on {field_name})",
                                    operation="RANGE_SEARCH",
                                    execution_time=result.execution_time_ms,
                                    disk_reads=result.disk_reads,
                                    disk_writes=result.disk_writes,
                                    records_count=len(result.data)
                                )

                                print(f"    Range [{start_val}, {end_val}]: {result.execution_time_ms:.4f}ms, {len(result.data)} records")

                        # Drop secondary index
                        db.drop_index(table_name, field_name)

                    except Exception as e:
                        print(f"  ERROR testing {sec_index_type} on {field_name}: {e}")
                        import traceback
                        traceback.print_exc()

            # Cleanup
            db.drop_table(table_name)

        except Exception as e:
            print(f"  ERROR in secondary index tests: {e}")
            import traceback
            traceback.print_exc()

    def _create_record_from_row(self, row: pd.Series, config: DatasetTestConfig) -> Record:
        """Create a Record object from a DataFrame row"""
        # Convert fields dict to list of tuples (field_name, field_type, field_size)
        list_of_types = [(name, ftype, fsize) for name, (ftype, fsize) in config.fields.items()]

        # Create record with proper initialization
        record = Record(list_of_types, config.primary_key)

        for field_name, (field_type, field_size) in config.fields.items():
            # Check if field exists in the row
            if field_name not in row.index:
                # Generate default value for missing fields
                if field_name == "row_id":
                    # Use DataFrame index as row_id
                    value = int(row.name) if pd.notna(row.name) else 0
                elif field_type == "INT":
                    value = 0
                elif field_type == "FLOAT":
                    value = 0.0
                elif field_type in ["VARCHAR", "CHAR"]:
                    value = ""
                else:
                    value = 0
            else:
                value = row[field_name]

            # Handle NaN/None values for all fields
            if value is None or pd.isna(value):
                if field_type == "INT":
                    value = 0
                elif field_type == "FLOAT":
                    value = 0.0
                elif field_type in ["VARCHAR", "CHAR"]:
                    value = ""
                else:
                    value = 0

            # Handle VARCHAR/CHAR (ensure proper string formatting)
            if field_type in ["VARCHAR", "CHAR"]:
                value = str(value)[:field_size]  # Convert to string and truncate if needed

            # Set attribute on record
            setattr(record, field_name, value)

        return record

    def generate_comparison_graphs(self):
        """Generate comprehensive comparison graphs"""
        print(f"\n{'='*80}")
        print("Generating Performance Comparison Graphs")
        print(f"{'='*80}")

        df = self.metrics.get_dataframe()

        if df.empty:
            print("No metrics collected, skipping graph generation")
            return

        # Save metrics to CSV
        csv_path = os.path.join(self.output_path, "performance_metrics.csv")
        df.to_csv(csv_path, index=False)
        print(f"Metrics saved to {csv_path}")

        # Graph 1: Execution Time Comparison by Operation
        self._plot_execution_time_by_operation(df)

        # Graph 2: Disk Access Comparison
        self._plot_disk_accesses(df)

        # Graph 3: Index Type Performance Summary
        self._plot_index_type_summary(df)

        # Graph 4: Dataset-specific comparisons
        self._plot_dataset_comparisons(df)

        print(f"All graphs saved to {self.output_path}/")

    def _plot_execution_time_by_operation(self, df: pd.DataFrame):
        """Plot execution time comparison by operation type"""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Execution Time Comparison by Operation Type', fontsize=16, fontweight='bold')

        operations = df['operation'].unique()

        for idx, operation in enumerate(operations[:4]):
            ax = axes[idx // 2, idx % 2]

            op_data = df[df['operation'] == operation]

            if not op_data.empty:
                # Group by dataset and index_type
                grouped = op_data.groupby(['dataset', 'index_type'])['execution_time_ms'].mean().reset_index()

                # Pivot for plotting
                pivot_data = grouped.pivot(index='index_type', columns='dataset', values='execution_time_ms')

                pivot_data.plot(kind='bar', ax=ax, rot=45)
                ax.set_title(f'{operation} - Average Execution Time')
                ax.set_ylabel('Time (ms)')
                ax.set_xlabel('Index Type')
                ax.legend(title='Dataset', bbox_to_anchor=(1.05, 1), loc='upper left')
                ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_path, 'execution_time_by_operation.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  Generated: execution_time_by_operation.png")

    def _plot_disk_accesses(self, df: pd.DataFrame):
        """Plot disk access comparison"""
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        fig.suptitle('Disk Access Comparison', fontsize=16, fontweight='bold')

        # Reads vs Writes by Index Type
        grouped = df.groupby('index_type').agg({
            'disk_reads': 'sum',
            'disk_writes': 'sum'
        }).reset_index()

        ax = axes[0]
        x = np.arange(len(grouped))
        width = 0.35

        ax.bar(x - width/2, grouped['disk_reads'], width, label='Reads', color='skyblue')
        ax.bar(x + width/2, grouped['disk_writes'], width, label='Writes', color='salmon')
        ax.set_xlabel('Index Type')
        ax.set_ylabel('Total Disk Accesses')
        ax.set_title('Total Disk Reads vs Writes by Index Type')
        ax.set_xticks(x)
        ax.set_xticklabels(grouped['index_type'], rotation=45, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Total disk accesses by dataset
        ax = axes[1]
        dataset_grouped = df.groupby('dataset')['total_disk_accesses'].sum().reset_index()

        ax.bar(dataset_grouped['dataset'], dataset_grouped['total_disk_accesses'], color='mediumseagreen')
        ax.set_xlabel('Dataset')
        ax.set_ylabel('Total Disk Accesses')
        ax.set_title('Total Disk Accesses by Dataset')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_path, 'disk_accesses_comparison.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  Generated: disk_accesses_comparison.png")

    def _plot_index_type_summary(self, df: pd.DataFrame):
        """Plot summary performance by index type"""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Index Type Performance Summary', fontsize=16, fontweight='bold')

        # Average execution time by index type
        ax = axes[0, 0]
        index_perf = df.groupby('index_type')['execution_time_ms'].mean().sort_values()
        index_perf.plot(kind='barh', ax=ax, color='steelblue')
        ax.set_xlabel('Average Execution Time (ms)')
        ax.set_title('Average Execution Time by Index Type')
        ax.grid(True, alpha=0.3)

        # Disk efficiency (operations per disk access)
        ax = axes[0, 1]
        df_copy = df.copy()
        df_copy['efficiency'] = df_copy['records_count'] / (df_copy['total_disk_accesses'] + 1)
        efficiency = df_copy.groupby('index_type')['efficiency'].mean().sort_values(ascending=False)
        efficiency.plot(kind='barh', ax=ax, color='coral')
        ax.set_xlabel('Records per Disk Access')
        ax.set_title('Disk Access Efficiency by Index Type')
        ax.grid(True, alpha=0.3)

        # Operation distribution
        ax = axes[1, 0]
        op_counts = df['operation'].value_counts()
        ax.pie(op_counts.values, labels=op_counts.index, autopct='%1.1f%%', startangle=90)
        ax.set_title('Operation Distribution in Tests')

        # Index type usage
        ax = axes[1, 1]
        index_counts = df['index_type'].value_counts()
        ax.pie(index_counts.values, labels=index_counts.index, autopct='%1.1f%%', startangle=90)
        ax.set_title('Index Type Usage in Tests')

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_path, 'index_type_summary.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  Generated: index_type_summary.png")

    def _plot_dataset_comparisons(self, df: pd.DataFrame):
        """Plot individual dataset comparisons"""
        datasets = df['dataset'].unique()

        for dataset in datasets:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            fig.suptitle(f'{dataset} - Index Performance Comparison', fontsize=16, fontweight='bold')

            dataset_df = df[df['dataset'] == dataset]

            # Execution time by operation
            ax = axes[0, 0]
            op_data = dataset_df.groupby(['operation', 'index_type'])['execution_time_ms'].mean().unstack()
            op_data.plot(kind='bar', ax=ax, rot=45)
            ax.set_ylabel('Time (ms)')
            ax.set_title('Execution Time by Operation')
            ax.legend(title='Index Type', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3)

            # Disk reads by index type
            ax = axes[0, 1]
            reads_data = dataset_df.groupby('index_type')['disk_reads'].sum().sort_values()
            reads_data.plot(kind='barh', ax=ax, color='lightblue')
            ax.set_xlabel('Total Disk Reads')
            ax.set_title('Disk Reads by Index Type')
            ax.grid(True, alpha=0.3)

            # Disk writes by index type
            ax = axes[1, 0]
            writes_data = dataset_df.groupby('index_type')['disk_writes'].sum().sort_values()
            writes_data.plot(kind='barh', ax=ax, color='lightcoral')
            ax.set_xlabel('Total Disk Writes')
            ax.set_title('Disk Writes by Index Type')
            ax.grid(True, alpha=0.3)

            # Performance heatmap
            ax = axes[1, 1]
            heatmap_data = dataset_df.pivot_table(
                values='execution_time_ms',
                index='index_type',
                columns='operation',
                aggfunc='mean'
            )

            if not heatmap_data.empty:
                sns.heatmap(heatmap_data, annot=True, fmt='.2f', cmap='YlOrRd', ax=ax, cbar_kws={'label': 'Time (ms)'})
                ax.set_title('Performance Heatmap (Execution Time)')

            plt.tight_layout()
            safe_name = dataset.replace(" ", "_").lower()
            plt.savefig(os.path.join(self.output_path, f'dataset_comparison_{safe_name}.png'), dpi=300, bbox_inches='tight')
            plt.close()
            print(f"  Generated: dataset_comparison_{safe_name}.png")

    def run_all_tests(self, sample_size: int = 1000):
        """Run all tests for all datasets"""
        print(f"\n{'#'*80}")
        print(f"# Starting Comprehensive Index Performance Tests")
        print(f"# Sample size per dataset: {sample_size}")
        print(f"# Total datasets: {len(self.configs)}")
        print(f"{'#'*80}")

        for config in self.configs:
            try:
                # Test primary indices
                self.test_primary_index_performance(config, sample_size)

                # Test secondary indices
                self.test_secondary_index_performance(config, sample_size)

            except Exception as e:
                print(f"\nERROR testing {config.name}: {e}")
                import traceback
                traceback.print_exc()
                continue

        # Generate comparison graphs
        self.generate_comparison_graphs()

        print(f"\n{'#'*80}")
        print(f"# All tests completed!")
        print(f"# Results saved to: {self.output_path}")
        print(f"{'#'*80}")


def main():
    """Main test execution"""
    # Path to test data
    base_data_path = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(base_data_path, "test_results")

    # Create tester
    tester = IndexPerformanceTester(base_data_path, output_path)

    # Run all tests with sample size
    # Adjust sample_size based on your needs (smaller = faster, larger = more accurate)
    tester.run_all_tests(sample_size=500)  # Start with 500 records per dataset


if __name__ == "__main__":
    main()

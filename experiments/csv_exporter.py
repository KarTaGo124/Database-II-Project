
import csv
import os
from datetime import datetime


def export_comparison_1(all_results, output_dir="experiments/results"):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    insert_file = os.path.join(output_dir, f"comp1_insertion_{timestamp}.csv")
    with open(insert_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Index_Type', 'Records', 'Total_Reads', 'Total_Writes', 'Time_ms'])
        for res in all_results:
            ins = res['insert']
            writer.writerow([
                res['index_type'],
                ins['records'],
                ins['total_reads'],
                ins['total_writes'],
                ins['total_time_ms']
            ])

    search_file = os.path.join(output_dir, f"comp1_exact_search_{timestamp}.csv")
    with open(search_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Index_Type', 'Avg_Reads', 'Avg_Time_ms', 'Avg_Results', 'Samples'])
        for res in all_results:
            writer.writerow([
                res['index_type'],
                res['search']['avg_reads'],
                res['search']['avg_time_ms'],
                res['search']['avg_results'],
                res['search']['samples']
            ])

    range_file = os.path.join(output_dir, f"comp1_range_search_{timestamp}.csv")
    with open(range_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Index_Type', 'Avg_Reads', 'Avg_Time_ms', 'Avg_Results', 'Samples'])
        for res in all_results:
            rs = res['range_search']
            writer.writerow([
                res['index_type'],
                rs['avg_reads'],
                rs['avg_time_ms'],
                rs['avg_results'],
                rs['samples']
            ])

    return [insert_file, search_file, range_file]


def export_comparison_2(all_results, output_dir="experiments/results"):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    insert_file = os.path.join(output_dir, f"comp2_insertion_{timestamp}.csv")
    with open(insert_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Configuration', 'Records', 'Total_Insertion_Reads', 'Total_Insertion_Writes', 'Total_Insertion_Time_ms'])
        for res in all_results:
            ins = res['insert']
            writer.writerow([
                res['config'],
                ins['records'],
                ins['total_insertion_reads'],
                ins['total_insertion_writes'],
                ins['total_insertion_time_ms']
            ])

    search_file = os.path.join(output_dir, f"comp2_exact_search_{timestamp}.csv")
    with open(search_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Configuration', 'Avg_Reads', 'Avg_Time_ms', 'Avg_Results', 'Samples'])
        for res in all_results:
            srch = res['search']
            writer.writerow([
                res['config'],
                srch['avg_reads'],
                srch['avg_time_ms'],
                srch['avg_results'],
                srch['samples']
            ])

    return [insert_file, search_file]


def export_comparison_3(all_results, output_dir="experiments/results"):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    insert_file = os.path.join(output_dir, f"comp3_insertion_{timestamp}.csv")
    with open(insert_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Configuration', 'Records', 'Total_Insertion_Reads', 'Total_Insertion_Writes', 'Total_Insertion_Time_ms'])
        for res in all_results:
            ins = res['insert']
            writer.writerow([
                res['config'],
                ins['records'],
                ins['total_insertion_reads'],
                ins['total_insertion_writes'],
                ins['total_insertion_time_ms']
            ])

    search_file = os.path.join(output_dir, f"comp3_range_search_{timestamp}.csv")
    with open(search_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Configuration', 'Avg_Reads', 'Avg_Time_ms', 'Avg_Results', 'Samples'])
        for res in all_results:
            srch = res['search']
            writer.writerow([
                res['config'],
                srch['avg_reads'],
                srch['avg_time_ms'],
                srch['avg_results'],
                srch['samples']
            ])

    return [insert_file, search_file]

def export_comparison_4(all_results, output_dir="experiments/results"):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    insert_file = os.path.join(output_dir, f"comp4_insertion_{timestamp}.csv")
    with open(insert_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Configuration', 'Records', 'Data_Load_Reads', 'Data_Load_Writes', 'Data_Load_Time_ms', 
                        'Index_Creation_Reads', 'Index_Creation_Writes', 'Index_Creation_Time_ms',
                        'Total_Insertion_Reads', 'Total_Insertion_Writes', 'Total_Insertion_Time_ms'])
        for res in all_results:
            ins = res['insert']
            writer.writerow([
                res['config'],
                ins['records'],
                ins['total_reads'],
                ins['total_writes'],
                ins['total_time_ms'],
                ins.get('index_creation_reads', 0),
                ins.get('index_creation_writes', 0),
                ins.get('index_creation_time_ms', 0),
                ins['total_insertion_reads'],
                ins['total_insertion_writes'],
                ins['total_insertion_time_ms']
            ])

    knn_file = os.path.join(output_dir, f"comp4_knn_search_{timestamp}.csv")
    with open(knn_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Configuration', 'Reads', 'Writes', 'Time_ms', 'Results'])
        for res in all_results:
            if 'knn_search' in res and 'reads' in res['knn_search']:
                knn = res['knn_search']
                writer.writerow([
                    res['config'],
                    knn['reads'],
                    knn.get('writes', 0),
                    knn['time_ms'],
                    knn.get('results', 'N/A')
                ])

    radius_file = os.path.join(output_dir, f"comp4_radius_search_{timestamp}.csv")
    with open(radius_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Configuration', 'Reads', 'Writes', 'Time_ms', 'Results'])
        for res in all_results:
            if 'radius_search' in res and 'reads' in res['radius_search']:
                rad = res['radius_search']
                writer.writerow([
                    res['config'],
                    rad['reads'],
                    rad.get('writes', 0),
                    rad['time_ms'],
                    rad.get('results', 'N/A')
                ])

    return [insert_file, knn_file, radius_file]
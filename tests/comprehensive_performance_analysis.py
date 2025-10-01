#!/usr/bin/env python3

import os
import shutil
import sys
import csv
import time
import statistics
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.record import Record, Table
from indexes.sequential_file.sequential_file import SequentialFile
from indexes.isam.primary import ISAMPrimaryIndex
from indexes.obsolete.secondary import create_secondary_index
from indexes.extendible_hashing.extendible_hashing import ExtendibleHashing
from indexes.core.performance_tracker import PerformanceTracker

class NoIndexTable:
    """Implementación simple de tabla sin índices para comparación"""
    def __init__(self, table, filename):
        self.table = table
        self.filename = filename
        self.performance = PerformanceTracker()
        self.records = []

    def insert(self, record):
        self.performance.start_operation()

        # Simular escritura a disco
        with open(self.filename, "ab") as f:
            f.write(record.pack())
            self.performance.track_write()

        self.records.append(record)
        return self.performance.end_operation(True)

    def search(self, key_value):
        self.performance.start_operation()

        # Simular lectura secuencial completa
        found_record = None
        if os.path.exists(self.filename):
            with open(self.filename, "rb") as f:
                while True:
                    try:
                        data = f.read(self.table.record_size)
                        if not data:
                            break
                        self.performance.track_read()

                        record = Record.unpack(data, self.table.all_fields, self.table.key_field)
                        if record.get_key() == key_value:
                            found_record = record
                            break
                    except:
                        break

        return self.performance.end_operation(found_record)

    def range_search(self, start_value, end_value):
        self.performance.start_operation()

        found_records = []
        if os.path.exists(self.filename):
            with open(self.filename, "rb") as f:
                while True:
                    try:
                        data = f.read(self.table.record_size)
                        if not data:
                            break
                        self.performance.track_read()

                        record = Record.unpack(data, self.table.all_fields, self.table.key_field)
                        key = record.get_key()
                        if start_value <= key <= end_value:
                            found_records.append(record)
                    except:
                        break

        return self.performance.end_operation(found_records)

    def delete(self, key_value):
        self.performance.start_operation()

        # Simular eliminación (marcar como eliminado)
        found = False
        if os.path.exists(self.filename):
            temp_file = self.filename + ".tmp"
            with open(self.filename, "rb") as input_f, open(temp_file, "wb") as output_f:
                while True:
                    try:
                        data = input_f.read(self.table.record_size)
                        if not data:
                            break
                        self.performance.track_read()

                        record = Record.unpack(data, self.table.all_fields, self.table.key_field)
                        if record.get_key() != key_value:
                            output_f.write(data)
                            self.performance.track_write()
                        else:
                            found = True
                    except:
                        break

            if found:
                os.replace(temp_file, self.filename)
            else:
                os.remove(temp_file)

        return self.performance.end_operation(found)

def load_sales_data():
    """Carga los datos del CSV de ventas"""
    csv_path = os.path.join("data", "datasets", "sales_dataset_unsorted.csv")
    sales_data = []

    with open(csv_path, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            # Limpiar y convertir datos
            try:
                sale_id = int(row['ID de la venta'])
                product_name = row['Nombre producto'].strip()[:20]  # Truncar a 20 chars
                quantity = int(row['Cantidad vendida'])
                price = float(row['Precio unitario'])
                date = row['Fecha de venta'].strip()[:10]  # Truncar fecha

                sales_data.append({
                    'id': sale_id,
                    'product': product_name,
                    'quantity': quantity,
                    'price': price,
                    'date': date
                })
            except (ValueError, KeyError):
                continue  # Saltar registros con errores

    return sales_data

def create_test_configurations(test_dir):
    """Crea diferentes configuraciones de base de datos para probar"""

    # Tabla común para todos los tests
    table = Table(
        table_name="sales",
        sql_fields=[
            ("id", "INT", 4),
            ("product", "CHAR", 20),
            ("quantity", "INT", 4),
            ("price", "FLOAT", 4),
            ("date", "CHAR", 10)
        ],
        key_field="id",
        extra_fields={"active": ("BOOL", 1)}
    )

    configurations = {}

    # 1. Sin índices (tabla plana)
    no_index_file = os.path.join(test_dir, "no_index.dat")
    configurations["No Index"] = {
        "type": "no_index",
        "instance": NoIndexTable(table, no_index_file),
        "description": "Búsqueda secuencial sin índices"
    }

    # 2. Sequential File
    seq_main = os.path.join(test_dir, "seq_main.dat")
    seq_aux = os.path.join(test_dir, "seq_aux.dat")
    configurations["Sequential File"] = {
        "type": "sequential",
        "instance": SequentialFile(seq_main, seq_aux, table, k_rec=10),
        "description": "Sequential File con k=10"
    }

    # 3. ISAM Primary only
    isam_primary_file = os.path.join(test_dir, "isam_primary.dat")
    configurations["ISAM Primary"] = {
        "type": "isam_primary",
        "instance": ISAMPrimaryIndex(table, isam_primary_file),
        "description": "Solo ISAM Primary en campo ID"
    }

    # 4. ISAM Primary + Secondary en quantity
    isam_primary_qty_file = os.path.join(test_dir, "isam_primary_qty.dat")
    isam_primary_qty = ISAMPrimaryIndex(table, isam_primary_qty_file)
    secondary_qty_file = os.path.join(test_dir, "secondary_qty.dat")
    secondary_qty = create_secondary_index("quantity", "INT", 4, isam_primary_qty, secondary_qty_file)
    configurations["ISAM + Secondary (Quantity)"] = {
        "type": "isam_secondary",
        "instance": isam_primary_qty,
        "secondary": secondary_qty,
        "secondary_field": "quantity",
        "description": "ISAM Primary + Secondary en Quantity"
    }

    # 5. ISAM Primary + Secondary en price
    isam_primary_price_file = os.path.join(test_dir, "isam_primary_price.dat")
    isam_primary_price = ISAMPrimaryIndex(table, isam_primary_price_file)
    secondary_price_file = os.path.join(test_dir, "secondary_price.dat")
    secondary_price = create_secondary_index("price", "FLOAT", 4, isam_primary_price, secondary_price_file)
    configurations["ISAM + Secondary (Price)"] = {
        "type": "isam_secondary",
        "instance": isam_primary_price,
        "secondary": secondary_price,
        "secondary_field": "price",
        "description": "ISAM Primary + Secondary en Price"
    }

    # 6. ISAM Primary + Secondary en product
    isam_primary_product_file = os.path.join(test_dir, "isam_primary_product.dat")
    isam_primary_product = ISAMPrimaryIndex(table, isam_primary_product_file)
    secondary_product_file = os.path.join(test_dir, "secondary_product.dat")
    secondary_product = create_secondary_index("product", "CHAR", 20, isam_primary_product, secondary_product_file)
    configurations["ISAM + Secondary (Product)"] = {
        "type": "isam_secondary",
        "instance": isam_primary_product,
        "secondary": secondary_product,
        "secondary_field": "product",
        "description": "ISAM Primary + Secondary en Product"
    }

    # 7. Extendible Hash
    ext_hash_file = os.path.join(test_dir, "ext_hash.dat")
    configurations["Extendible Hash"] = {
        "type": "extendible_hash",
        "instance": ExtendibleHashing(ext_hash_file, "id", "INT", 4),
        "description": "Extendible Hashing en campo ID"
    }

    return table, configurations

def run_insert_test(config_name, config, sales_data):
    """Ejecuta test de inserción para una configuración"""
    print(f"  Insertando {len(sales_data)} registros...")

    results = []
    total_reads = 0
    total_writes = 0
    total_time = 0

    # Determinar la tabla para crear records
    if hasattr(config["instance"], 'table') and config["instance"].table:
        table_fields = config["instance"].table.all_fields
        key_field = config["instance"].table.key_field
    else:
        # Para ExtendibleHashing y otros que no tienen tabla directa, crear tabla dummy
        table_fields = [("id", "INT", 4), ("product", "CHAR", 20), ("quantity", "INT", 4),
                       ("price", "FLOAT", 4), ("date", "CHAR", 10), ("active", "BOOL", 1)]
        key_field = "id"

    # Para configuraciones ISAM con secondary, necesitamos insertar en ambos
    for i, sale in enumerate(sales_data):

        # Crear registro
        record = Record(table_fields, key_field)
        record.set_values(
            id=sale['id'],
            product=sale['product'],
            quantity=sale['quantity'],
            price=sale['price'],
            date=sale['date'],
            active=True
        )

        # Insertar en índice primario
        result = config["instance"].insert(record)

        # Si hay índice secundario, insertar también ahí
        if config["type"] == "isam_secondary":
            sec_result = config["secondary"].insert(record)
            result.disk_reads += sec_result.disk_reads
            result.disk_writes += sec_result.disk_writes
            result.execution_time_ms += sec_result.execution_time_ms

        results.append(result)
        total_reads += result.disk_reads
        total_writes += result.disk_writes
        total_time += result.execution_time_ms

        # Progreso cada 100 registros
        if (i + 1) % 100 == 0:
            print(f"    Insertados {i + 1}/{len(sales_data)} registros...")

    return {
        "total_records": len(sales_data),
        "total_reads": total_reads,
        "total_writes": total_writes,
        "total_time_ms": total_time,
        "avg_reads": total_reads / len(sales_data),
        "avg_writes": total_writes / len(sales_data),
        "avg_time_ms": total_time / len(sales_data),
        "results": results
    }

def run_search_test(config_name, config, test_keys):
    """Ejecuta test de búsqueda para una configuración"""
    print(f"  Buscando {len(test_keys)} claves...")

    results = []
    for key in test_keys:
        result = config["instance"].search(key)
        results.append({
            "key": key,
            "found": result.data is not None,
            "reads": result.disk_reads,
            "writes": result.disk_writes,
            "time_ms": result.execution_time_ms
        })

    total_reads = sum(r["reads"] for r in results)
    total_time = sum(r["time_ms"] for r in results)

    return {
        "total_searches": len(test_keys),
        "total_reads": total_reads,
        "total_time_ms": total_time,
        "avg_reads": total_reads / len(test_keys),
        "avg_time_ms": total_time / len(test_keys),
        "results": results
    }

def run_range_search_test(config_name, config, ranges):
    """Ejecuta test de búsqueda por rangos"""
    print(f"  Ejecutando {len(ranges)} range searches...")

    results = []
    for range_def in ranges:
        start, end = range_def

        try:
            if config["type"] == "isam_secondary":
                result = config["secondary"].range_search(start, end)
            elif hasattr(config["instance"], 'range_search'):
                result = config["instance"].range_search(start, end)
            else:
                result = config["instance"].search(start)
        except NotImplementedError:
            result = config["instance"].performance.end_operation([])
            result.disk_reads = 0
            result.disk_writes = 0
            result.execution_time_ms = 0.0

        results.append({
            "range": f"{start}-{end}",
            "found_count": len(result.data) if hasattr(result.data, '__len__') else (1 if result.data else 0),
            "reads": result.disk_reads,
            "writes": result.disk_writes,
            "time_ms": result.execution_time_ms
        })

    total_reads = sum(r["reads"] for r in results)
    total_time = sum(r["time_ms"] for r in results)

    return {
        "total_ranges": len(ranges),
        "total_reads": total_reads,
        "total_time_ms": total_time,
        "avg_reads": total_reads / len(ranges),
        "avg_time_ms": total_time / len(ranges),
        "results": results
    }

def run_delete_test(config_name, config, delete_keys):
    """Ejecuta test de eliminación"""
    print(f"  Eliminando {len(delete_keys)} registros...")

    results = []
    for key in delete_keys:
        try:
            if config["type"] == "no_index":
                # NoIndexTable espera solo la key
                result = config["instance"].delete(key)
            elif config["type"] in ["sequential", "isam_primary", "extendible_hash"]:
                # Estos métodos esperan solo la key
                result = config["instance"].delete(key)
            elif config["type"] == "isam_secondary":
                # Para ISAM Secondary, necesitamos crear un record dummy
                # Buscar primero el registro para obtener los valores
                search_result = config["instance"].search(key)
                if search_result.data:
                    # Crear un record para eliminar
                    table_fields = config["instance"].table.all_fields
                    key_field = config["instance"].table.key_field
                    record = Record(table_fields, key_field)
                    record.set_values(
                        id=key,
                        product="dummy",  # No importa para delete
                        quantity=1,
                        price=1.0,
                        date="01/01/2024",
                        active=True
                    )
                    result = config["instance"].delete(record)
                else:
                    # No existe, simular resultado
                    result = config["instance"].performance.end_operation(False)
            else:
                result = config["instance"].delete(key)

            results.append({
                "key": key,
                "deleted": result.data if hasattr(result, 'data') else result,
                "reads": result.disk_reads if hasattr(result, 'disk_reads') else 0,
                "writes": result.disk_writes if hasattr(result, 'disk_writes') else 0,
                "time_ms": result.execution_time_ms if hasattr(result, 'execution_time_ms') else 0
            })
        except Exception as e:
            # Si hay error, agregar resultado vacío
            results.append({
                "key": key,
                "deleted": False,
                "reads": 0,
                "writes": 0,
                "time_ms": 0
            })

    total_reads = sum(r["reads"] for r in results)
    total_writes = sum(r["writes"] for r in results)
    total_time = sum(r["time_ms"] for r in results)

    return {
        "total_deletes": len(delete_keys),
        "total_reads": total_reads,
        "total_writes": total_writes,
        "total_time_ms": total_time,
        "avg_reads": total_reads / len(delete_keys) if delete_keys else 0,
        "avg_writes": total_writes / len(delete_keys) if delete_keys else 0,
        "avg_time_ms": total_time / len(delete_keys) if delete_keys else 0,
        "results": results
    }

def comprehensive_performance_analysis():
    """Análisis comprensivo de rendimiento con datos reales"""
    print("=" * 100)
    print("ANÁLISIS COMPRENSIVO DE RENDIMIENTO - TÉCNICAS DE INDEXACIÓN")
    print("Usando 1000 registros reales del dataset de ventas")
    print("=" * 100)

    test_dir = "comprehensive_performance_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    try:
        # Cargar datos
        print("\n1. Cargando datos del CSV...")
        sales_data = load_sales_data()
        print(f"   Cargados {len(sales_data)} registros de ventas")

        # Usar todos los 1000 registros
        print(f"   Usando {len(sales_data)} registros para análisis completo")

        # Crear configuraciones
        print("\n2. Creando configuraciones de base de datos...")
        table, configurations = create_test_configurations(test_dir)

        for name, config in configurations.items():
            print(f"   - {name}: {config['description']}")

        # Definir claves de prueba para 1000 registros
        data_size = len(sales_data)
        test_indices = [0, 100, 250, 500, 750, 999]
        test_keys = [sales_data[i]['id'] for i in test_indices]  # Claves existentes
        test_keys.extend([9999, 8888, 7777])  # Claves inexistentes

        # Rangos para range search
        id_ranges = [(1, 100), (200, 400), (500, 700), (800, 900)]
        quantity_ranges = [(1, 10), (20, 40), (50, 100)]
        price_ranges = [(100.0, 500.0), (1000.0, 1500.0), (2000.0, 3000.0)]

        # Claves para eliminación (últimos 20 registros)
        delete_keys = [sales_data[i]['id'] for i in range(980, 1000)]

        print(f"   Test keys: {test_keys}")
        print(f"   ID ranges: {id_ranges}")
        print(f"   Delete keys: {len(delete_keys)} claves para eliminación")

        # Resultados
        all_results = {}

        # 3. EJECUTAR TESTS PARA CADA CONFIGURACIÓN
        for config_name, config in configurations.items():
            print(f"\n3. TESTING: {config_name}")
            print("-" * 60)

            config_results = {}

            # Test de inserción
            print("3.1 Test de Inserción:")
            start_time = time.time()
            insert_results = run_insert_test(config_name, config, sales_data)
            insert_wall_time = time.time() - start_time
            insert_results["wall_time_seconds"] = insert_wall_time
            config_results["insert"] = insert_results
            print(f"    Completado en {insert_wall_time:.2f} segundos")

            # Test de búsqueda
            print("3.2 Test de Búsqueda:")
            start_time = time.time()
            search_results = run_search_test(config_name, config, test_keys)
            search_wall_time = time.time() - start_time
            search_results["wall_time_seconds"] = search_wall_time
            config_results["search"] = search_results
            print(f"    Completado en {search_wall_time:.2f} segundos")

            # Test de range search
            print("3.3 Test de Range Search:")
            if config["type"] == "isam_secondary":
                if config["secondary_field"] == "quantity":
                    ranges = quantity_ranges
                elif config["secondary_field"] == "price":
                    ranges = price_ranges
                elif config["secondary_field"] == "product":
                    # Para productos, usar rangos de strings simples para evitar problemas de encoding
                    ranges = [("A", "M"), ("N", "Z")]
                else:
                    ranges = id_ranges
            else:
                ranges = id_ranges

            start_time = time.time()
            range_results = run_range_search_test(config_name, config, ranges)
            range_wall_time = time.time() - start_time
            range_results["wall_time_seconds"] = range_wall_time
            config_results["range_search"] = range_results
            print(f"    Completado en {range_wall_time:.2f} segundos")

            # Test de eliminación
            print("3.4 Test de Eliminación:")
            start_time = time.time()
            delete_results = run_delete_test(config_name, config, delete_keys[:5])  # Solo 5 para no afectar otros tests
            delete_wall_time = time.time() - start_time
            delete_results["wall_time_seconds"] = delete_wall_time
            config_results["delete"] = delete_results
            print(f"    Completado en {delete_wall_time:.2f} segundos")

            all_results[config_name] = config_results
            print(f"  [OK] {config_name} completado")

        # 4. ANÁLISIS DE RESULTADOS
        print("\n" + "=" * 100)
        print("ANÁLISIS DETALLADO DE RESULTADOS")
        print("=" * 100)

        # Tabla resumen de inserción
        print("\n4.1 RENDIMIENTO DE INSERCIÓN (1000 registros)")
        print("-" * 120)
        print(f"{'Técnica':<25} {'Total Reads':<12} {'Total Writes':<13} {'Tiempo Wall (s)':<15} {'Tiempo CPU (ms)':<15} {'Reads/Insert':<12} {'Writes/Insert':<13} {'ms/Insert':<10}")
        print("-" * 120)

        for config_name, results in all_results.items():
            insert = results["insert"]
            print(f"{config_name:<25} {insert['total_reads']:<12} {insert['total_writes']:<13} {insert['wall_time_seconds']:<15.2f} {insert['total_time_ms']:<15.1f} {insert['avg_reads']:<12.2f} {insert['avg_writes']:<13.2f} {insert['avg_time_ms']:<10.2f}")

        # Tabla resumen de búsqueda
        print("\n4.2 RENDIMIENTO DE BÚSQUEDA (9 búsquedas)")
        print("-" * 100)
        print(f"{'Técnica':<25} {'Total Reads':<12} {'Tiempo Wall (s)':<15} {'Tiempo CPU (ms)':<15} {'Reads/Search':<13} {'ms/Search':<10}")
        print("-" * 100)

        for config_name, results in all_results.items():
            search = results["search"]
            print(f"{config_name:<25} {search['total_reads']:<12} {search['wall_time_seconds']:<15.2f} {search['total_time_ms']:<15.1f} {search['avg_reads']:<13.2f} {search['avg_time_ms']:<10.2f}")

        # Tabla resumen de range search
        print("\n4.3 RENDIMIENTO DE RANGE SEARCH")
        print("-" * 100)
        print(f"{'Técnica':<25} {'Total Reads':<12} {'Tiempo Wall (s)':<15} {'Tiempo CPU (ms)':<15} {'Reads/Range':<13} {'ms/Range':<10}")
        print("-" * 100)

        for config_name, results in all_results.items():
            range_search = results["range_search"]
            if range_search['total_reads'] == 0 and range_search['total_time_ms'] == 0:
                print(f"{config_name:<25} {'N/A':<12} {'N/A':<15} {'N/A':<15} {'N/A':<13} {'N/A':<10}")
            else:
                print(f"{config_name:<25} {range_search['total_reads']:<12} {range_search['wall_time_seconds']:<15.2f} {range_search['total_time_ms']:<15.1f} {range_search['avg_reads']:<13.2f} {range_search['avg_time_ms']:<10.2f}")

        # Tabla resumen de eliminación
        print("\n4.4 RENDIMIENTO DE ELIMINACIÓN (5 eliminaciones)")
        print("-" * 90)
        print(f"{'Técnica':<25} {'Total Reads':<12} {'Total Writes':<13} {'Tiempo Total (s)':<15} {'Reads/Delete':<12} {'ms/Delete':<10}")
        print("-" * 90)

        for config_name, results in all_results.items():
            delete = results["delete"]
            print(f"{config_name:<25} {delete['total_reads']:<12} {delete['total_writes']:<13} {delete['wall_time_seconds']:<15.2f} {delete['avg_reads']:<12.2f} {delete['avg_time_ms']:<10.2f}")

        # Ranking de mejor rendimiento
        print("\n4.5 RANKING DE MEJOR RENDIMIENTO")
        print("-" * 60)

        # Inserción
        insert_by_reads = sorted(all_results.items(), key=lambda x: x[1]["insert"]["avg_reads"])
        insert_by_time = sorted(all_results.items(), key=lambda x: x[1]["insert"]["wall_time_seconds"])

        print("INSERCIÓN:")
        print(f"  Mejor en Reads/Insert: {insert_by_reads[0][0]} ({insert_by_reads[0][1]['insert']['avg_reads']:.2f} reads)")
        print(f"  Mejor en Tiempo Total: {insert_by_time[0][0]} ({insert_by_time[0][1]['insert']['wall_time_seconds']:.2f} segundos)")

        # Búsqueda
        search_by_reads = sorted(all_results.items(), key=lambda x: x[1]["search"]["avg_reads"])
        search_by_time = sorted(all_results.items(), key=lambda x: x[1]["search"]["wall_time_seconds"])

        print("\nBÚSQUEDA:")
        print(f"  Mejor en Reads/Search: {search_by_reads[0][0]} ({search_by_reads[0][1]['search']['avg_reads']:.2f} reads)")
        print(f"  Mejor en Tiempo Total: {search_by_time[0][0]} ({search_by_time[0][1]['search']['wall_time_seconds']:.2f} segundos)")

        # Range Search
        range_by_reads = sorted(all_results.items(), key=lambda x: x[1]["range_search"]["avg_reads"])
        range_by_time = sorted(all_results.items(), key=lambda x: x[1]["range_search"]["wall_time_seconds"])

        print("\nRANGE SEARCH:")
        print(f"  Mejor en Reads/Range: {range_by_reads[0][0]} ({range_by_reads[0][1]['range_search']['avg_reads']:.2f} reads)")
        print(f"  Mejor en Tiempo Total: {range_by_time[0][0]} ({range_by_time[0][1]['range_search']['wall_time_seconds']:.2f} segundos)")

        # Estadísticas adicionales
        print("\n4.6 ESTADÍSTICAS COMPARATIVAS")
        print("-" * 60)

        no_index_insert_time = all_results["No Index"]["insert"]["wall_time_seconds"]
        fastest_insert_time = insert_by_time[0][1]["insert"]["wall_time_seconds"]
        speedup_insert = no_index_insert_time / fastest_insert_time

        no_index_search_time = all_results["No Index"]["search"]["avg_time_ms"]
        fastest_search_time = search_by_time[0][1]["search"]["avg_time_ms"]
        speedup_search = no_index_search_time / fastest_search_time

        print(f"Speedup en Inserción vs Sin Índices: {speedup_insert:.1f}x")
        print(f"Speedup en Búsqueda vs Sin Índices: {speedup_search:.1f}x")

        print(f"\nMejor configuración general para INSERCIÓN: {insert_by_time[0][0]}")
        print(f"Mejor configuración general para BÚSQUEDA: {search_by_time[0][0]}")

        # Análisis de tiempos crítico
        print("\n4.7 ANÁLISIS CRÍTICO DE TIEMPOS")
        print("-" * 80)

        print("TIEMPOS DE INSERCIÓN (segundos por 1000 registros):")
        for config_name, results in sorted(all_results.items(), key=lambda x: x[1]["insert"]["wall_time_seconds"]):
            wall_time = results["insert"]["wall_time_seconds"]
            cpu_time = results["insert"]["total_time_ms"] / 1000
            overhead = wall_time - cpu_time
            print(f"  {config_name:<25}: Wall={wall_time:6.2f}s  CPU={cpu_time:6.2f}s  Overhead={overhead:6.2f}s")

        print("\nTIEMPOS DE BÚSQUEDA (milisegundos promedio):")
        for config_name, results in sorted(all_results.items(), key=lambda x: x[1]["search"]["avg_time_ms"]):
            avg_time = results["search"]["avg_time_ms"]
            total_time = results["search"]["total_time_ms"]
            print(f"  {config_name:<25}: Promedio={avg_time:6.2f}ms  Total={total_time:6.1f}ms")

        print("\nVERIFICACIÓN DE CONSISTENCIA TEMPORAL:")
        print("¿Los tiempos son coherentes con las operaciones de disco?")
        for config_name, results in all_results.items():
            insert_reads = results["insert"]["avg_reads"]
            search_reads = results["search"]["avg_reads"]
            insert_time = results["insert"]["avg_time_ms"]
            search_time = results["search"]["avg_time_ms"]

            ratio_reads = search_reads / insert_reads if insert_reads > 0 else 0
            ratio_times = search_time / insert_time if insert_time > 0 else 0
            consistency = "OK" if abs(ratio_reads - ratio_times) < 2.0 else "ALERT"

            print(f"  {config_name:<25}: {consistency} Ratio reads={ratio_reads:.2f} vs tiempo={ratio_times:.2f}")

        print("\n[OK] Análisis comprensivo completado")
        print("=" * 100)

    except Exception as e:
        print(f"\n[ERROR] Error durante el análisis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    comprehensive_performance_analysis()
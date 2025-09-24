import os
import time
from isam import ISAMFile, Record


def load_csv_data(filename):
    """Cargador de datos CSV"""
    records = []
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines[1:]:
            parts = line.strip().split(';')
            if len(parts) == 5:
                id_venta = int(parts[0])
                nombre_producto = parts[1]
                cantidad_vendida = int(parts[2])
                precio_unitario = float(parts[3])
                fecha_venta = parts[4]
                records.append(Record(id_venta, nombre_producto, cantidad_vendida, precio_unitario, fecha_venta))
    return records


def test_with_real_data():
    print("=== PRUEBA ISAM CON DATASET REAL (1000 registros) ===\n")

    files_to_clean = ["datos.dat", "root_index.dat", "leaf_index.dat", "free_list.dat"]
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            os.remove(file_path)

    isam = ISAMFile()

    csv_path = "../../data/sales_dataset_unsorted.csv"
    print(f"Cargando datos desde: {csv_path}")

    start_time = time.time()
    records = load_csv_data(csv_path)
    load_time = time.time() - start_time

    print(f"Registros cargados: {len(records)} en {load_time:.3f}s")

    print("\nInsertando registros en ISAM...")
    start_time = time.time()
    for i, record in enumerate(records):
        isam.add(record)
        if (i + 1) % 100 == 0:
            print(f"  Insertados: {i+1:4d}/{len(records)}")

    insert_time = time.time() - start_time
    print(f"Insercion completada en {insert_time:.3f}s")

    print("\n" + "="*60)
    isam.show_structure()

    print("\n" + "="*60)
    print("=== ANALISIS DE DATOS ===")
    isam.scanAll()

    print("\n" + "="*60)
    print("=== PRUEBAS DE BUSQUEDA ===")
    test_keys = [403, 56, 107, 999, 33]

    start_time = time.time()
    for key in test_keys:
        result = isam.search(key)
        status = f"Encontrado: {result.nombre_producto}" if result else "No encontrado"
        print(f"Busqueda {key:3d}: {status}")
    search_time = time.time() - start_time
    print(f"5 busquedas en {search_time:.4f}s")

    print("\n" + "="*60)
    print("=== BUSQUEDA POR RANGO ===")
    start_time = time.time()
    range_results = isam.range_search(100, 200)
    range_time = time.time() - start_time

    print(f"Registros entre 100 y 200: {len(range_results)} encontrados en {range_time:.4f}s")
    for record in range_results[:5]:
        print(f"  {record.id_venta} - {record.nombre_producto}")
    if len(range_results) > 5:
        print(f"  ... y {len(range_results) - 5} mas")

    print("\n" + "="*60)
    print("=== PRUEBAS DE ELIMINACION ===")
    delete_keys = [403, 56, 107]

    start_time = time.time()
    for key in delete_keys:
        success = isam.delete(key)
        status = "Eliminado" if success else "No encontrado"
        print(f"Eliminar {key:3d}: {status}")
    delete_time = time.time() - start_time
    print(f"3 eliminaciones en {delete_time:.4f}s")

    print("\n" + "="*60)
    errors = isam.validate_index_consistency()

    print("\n=== RESUMEN FINAL ===")
    print(f"Dataset: {len(records)} registros reales")
    print(f"Tiempo insercion: {insert_time:.3f}s ({len(records)/insert_time:.0f} rec/s)")
    print(f"Busquedas: {search_time:.4f}s promedio")
    print(f"Implementacion ISAM funcional con datos reales")
    print(f"Validacion: {'PASO' if not errors else 'FALLO'}")


def test_basic_functionality():
    print("=== PRUEBA BASICA DE FUNCIONALIDAD ===\n")

    files_to_clean = ["datos.dat", "root_index.dat", "leaf_index.dat", "free_list.dat"]
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            os.remove(file_path)

    isam = ISAMFile()

    test_records = [
        Record(100, "ProductoA", 5, 25.50, "2023-01-01"),
        Record(200, "ProductoB", 3, 15.75, "2023-01-02"),
        Record(300, "ProductoC", 8, 45.25, "2023-01-03"),
        Record(150, "ProductoD", 2, 12.00, "2023-01-04"),
        Record(250, "ProductoE", 7, 32.80, "2023-01-05"),
    ]

    for record in test_records:
        isam.add(record)

    isam.show_structure()

    result = isam.search(150)
    print(f"\nBusqueda 150: {'Encontrado' if result else 'No encontrado'}")

    range_results = isam.range_search(100, 200)
    print(f"Rango 100-200: {len(range_results)} registros")

    success = isam.delete(150)
    print(f"Eliminar 150: {'Exitoso' if success else 'Fallo'}")

    print("Prueba basica completada")


if __name__ == "__main__":
    choice = input("Seleccionar test: (1) Basico (2) Dataset real (1000 registros): ")

    if choice == "2":
        test_with_real_data()
    else:
        test_basic_functionality()
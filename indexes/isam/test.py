import os
import time
from .primary import ISAMPrimaryIndex
from ..core.record import Record, Table


def create_sales_table():
    return Table(
        table_name="sales",
        sql_fields=[
            ("id_venta", "INT", 4),
            ("nombre_producto", "CHAR", 50),
            ("cantidad_vendida", "INT", 4),
            ("precio_unitario", "FLOAT", 4),
            ("fecha_venta", "CHAR", 10)
        ],
        key_field="id_venta"
    )

def load_csv_data(filename, table):
    records = []
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines[1:]:
            parts = line.strip().split(';')
            if len(parts) == 5:
                record = Record(table.all_fields, table.key_field)
                record.set_values(
                    id_venta=int(parts[0]),
                    nombre_producto=parts[1],
                    cantidad_vendida=int(parts[2]),
                    precio_unitario=float(parts[3]),
                    fecha_venta=parts[4]
                )
                records.append(record)
    return records


def test_with_real_data():
    print("=== PRUEBA ISAM CON DATASET REAL (1000 registros) ===\n")

    files_to_clean = ["datos.dat", "root_index.dat", "leaf_index.dat", "free_list.dat"]
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            os.remove(file_path)

    sales_table = create_sales_table()
    isam = ISAMPrimaryIndex(sales_table)

    csv_path = "data/sales_dataset_unsorted.csv"
    print(f"Cargando datos desde: {csv_path}")

    start_time = time.time()
    records = load_csv_data(csv_path, sales_table)
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
        if result:
            nombre = result.nombre_producto
            if isinstance(nombre, bytes):
                nombre = nombre.decode('utf-8').rstrip('\x00').strip()
            status = f"Encontrado: {nombre}"
        else:
            status = "No encontrado"
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
        nombre = record.nombre_producto
        if isinstance(nombre, bytes):
            nombre = nombre.decode('utf-8').rstrip('\x00').strip()
        print(f"  {record.get_key()} - {nombre}")
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
    if errors:
        print("Errores encontrados:")
        for error in errors:
            print(f"  - {error}")


def test_basic_functionality():
    print("=== PRUEBA BASICA DE FUNCIONALIDAD ===\n")

    files_to_clean = ["datos.dat", "root_index.dat", "leaf_index.dat", "free_list.dat"]
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            os.remove(file_path)

    sales_table = create_sales_table()
    isam = ISAMPrimaryIndex(sales_table)

    test_data = [
        (100, "ProductoA", 5, 25.50, "2023-01-01"),
        (200, "ProductoB", 3, 15.75, "2023-01-02"),
        (300, "ProductoC", 8, 45.25, "2023-01-03"),
        (150, "ProductoD", 2, 12.00, "2023-01-04"),
        (250, "ProductoE", 7, 32.80, "2023-01-05"),
    ]

    test_records = []
    for data in test_data:
        record = Record(sales_table.all_fields, sales_table.key_field)
        record.set_values(
            id_venta=data[0],
            nombre_producto=data[1],
            cantidad_vendida=data[2],
            precio_unitario=data[3],
            fecha_venta=data[4]
        )
        test_records.append(record)

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
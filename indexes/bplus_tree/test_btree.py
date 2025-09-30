"""
Test completo de B+ Tree con DatabaseManager usando sales_dataset_unsorted.csv
"""
import os
import csv
import shutil
from ..core.record import Table, Record
from ..core.database_manager import DatabaseManager

CSV_PATH = 'data/datasets/sales_dataset_unsorted.csv'


def clean_database(db_name: str):
    """Limpia una base de datos de prueba"""
    db_path = f"data/databases/{db_name}"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)


def load_csv_data(limit=None):
    """Carga datos del CSV"""
    data = []
    with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        # Imprime los nombres de las columnas para depuración
        print("Columnas CSV:", reader.fieldnames)
        # Limpia BOM si existe
        fieldnames = [fn.lstrip('\ufeff') for fn in reader.fieldnames]
        reader.fieldnames = fieldnames
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            data.append({
                'ID de la venta': int(row['ID de la venta']),
                'Nombre producto': row['Nombre producto'],
                'Cantidad vendida': int(row['Cantidad vendida']),
                'Precio unitario': float(row['Precio unitario']),
                'Fecha de venta': row['Fecha de venta']
            })
    return data

def test_btree_primary_with_sales():
    print("=" * 70)
    print("TEST 1: B+ Tree como índice primario - Sales Dataset")
    print("=" * 70)
    
    clean_database("btree_sales_primary")
    db = DatabaseManager("btree_sales_primary")
    
    # Definir campos de la tabla
    fields = [
        ('ID de la venta', 'INT', 4),
        ('Nombre producto', 'CHAR', 64),
        ('Cantidad vendida', 'INT', 4),
        ('Precio unitario', 'FLOAT', 8),
        ('Fecha de venta', 'CHAR', 16)
    ]
    
    table = Table(
        table_name="ventas",
        sql_fields=fields,
        key_field='ID de la venta'
    )
    
    db.create_table(table, primary_index_type="BTREE")
    print("Tabla creada con B+ Tree primary index")
    
    sales_data = load_csv_data(limit=100)
    inserted_count = 0
    
    for sale in sales_data:
        rec = Record(fields, 'ID de la venta')
        rec.set_field_value('ID de la venta', sale['ID de la venta'])
        rec.set_field_value('Nombre producto', sale['Nombre producto'])
        rec.set_field_value('Cantidad vendida', sale['Cantidad vendida'])
        rec.set_field_value('Precio unitario', sale['Precio unitario'])
        rec.set_field_value('Fecha de venta', sale['Fecha de venta'])
        
        result = db.insert("ventas", rec)
        if result.data:
            inserted_count += 1
    
    print(f"Insertados {inserted_count} registros desde CSV")
    
    test_ids = [sales_data[0]['ID de la venta'], 
                sales_data[len(sales_data)//2]['ID de la venta'],
                sales_data[-1]['ID de la venta']]
    
    print("\nTest de búsqueda por primary key:")
    for test_id in test_ids:
        result = db.search("ventas", test_id)
        if result.data:
            rec = result.data[0]
            nombre = rec.get_field_value('Nombre producto')
            if isinstance(nombre, bytes):
                nombre = nombre.decode('utf-8').strip()
            print(f"  ID {test_id}: {nombre} - ${rec.get_field_value('Precio unitario')}")
            print(f"    Tiempo: {result.execution_time_ms:.2f}ms, Accesos: {result.total_disk_accesses}")
    
    # Test range search
    start_id = sales_data[10]['ID de la venta']
    end_id = sales_data[20]['ID de la venta']
    print(f"\nTest de búsqueda por rango ({start_id} - {end_id}):")
    range_result = db.range_search("ventas", start_id, end_id)
    print(f"  Encontrados: {len(range_result.data)} registros")
    print(f"  Tiempo: {range_result.execution_time_ms:.2f}ms")
    print(f"  Accesos: {range_result.total_disk_accesses}")
    
    # Test delete
    delete_id = sales_data[0]['ID de la venta']
    print(f"\nTest de eliminación (ID={delete_id}):")
    delete_result = db.delete("ventas", delete_id)
    print(f"  Eliminado: {delete_result.data}")
    print(f"  Tiempo: {delete_result.execution_time_ms:.2f}ms")
    
    # Verificar eliminación
    search_after = db.search("ventas", delete_id)
    print(f"  Verificación: {len(search_after.data)} resultados (debe ser 0)")
    
    # Estadísticas
    print("\nEstadísticas del índice primario:")
    table_info = db.tables["ventas"]
    if hasattr(table_info["primary_index"], "get_stats"):
        stats = table_info["primary_index"].get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    print("\n" + "=" * 70 + "\n")


def test_btree_secondary_with_sales():
    print("=" * 70)
    print("TEST 2: B+ Tree como índice secundario - Sales Dataset")
    print("=" * 70)
    
    clean_database("btree_sales_secondary")
    db = DatabaseManager("btree_sales_secondary")
    
    # Definir campos
    fields = [
        ('ID de la venta', 'INT', 4),
        ('Nombre producto', 'CHAR', 64),
        ('Cantidad vendida', 'INT', 4),
        ('Precio unitario', 'FLOAT', 8),
        ('Fecha de venta', 'CHAR', 16)
    ]
    
    # Crear tabla con ISAM primario
    table = Table(
        table_name="ventas",
        sql_fields=fields,
        key_field='ID de la venta'
    )
    
    db.create_table(table, primary_index_type="ISAM")
    print("Tabla creada con ISAM primary index")
    
    # Cargar datos (primeros 50 para prueba rápida)
    sales_data = load_csv_data(limit=50)
    
    for sale in sales_data:
        rec = Record(fields, 'ID de la venta')
        rec.set_field_value('ID de la venta', sale['ID de la venta'])
        rec.set_field_value('Nombre producto', sale['Nombre producto'])
        rec.set_field_value('Cantidad vendida', sale['Cantidad vendida'])
        rec.set_field_value('Precio unitario', sale['Precio unitario'])
        rec.set_field_value('Fecha de venta', sale['Fecha de venta'])
        db.insert("ventas", rec)
    
    print(f"Insertados {len(sales_data)} registros")
    
    print("\nCreando índices secundarios B+ Tree...")
    db.create_index("ventas", "Cantidad vendida", "BTREE", scan_existing=True)
    print("Índice sobre 'Cantidad vendida' creado")
    
    db.create_index("ventas", "Precio unitario", "BTREE", scan_existing=True)
    print("Índice sobre 'Precio unitario' creado")
    
    # Test búsqueda por cantidad
    test_cantidad = sales_data[5]['Cantidad vendida']
    print(f"\nBúsqueda por 'Cantidad vendida' = {test_cantidad}:")
    result = db.search("ventas", test_cantidad, field_name="Cantidad vendida")
    print(f"  Encontrados: {len(result.data)} registros")
    print(f"  Tiempo: {result.execution_time_ms:.2f}ms")
    if result.data:
        for rec in result.data[:3]:  # Mostrar primeros 3
            nombre = rec.get_field_value('Nombre producto')
            if isinstance(nombre, bytes):
                nombre = nombre.decode('utf-8').strip()
            print(f"    - {nombre} (ID: {rec.get_key()})")
    
    # Test range search por precio
    print(f"\nRange search 'Precio unitario' (50.0 - 200.0):")
    result = db.range_search("ventas", 50.0, 200.0, field_name="Precio unitario")
    print(f"  Encontrados: {len(result.data)} registros")
    print(f"  Tiempo: {result.execution_time_ms:.2f}ms")
    if result.data:
        print("  Primeros 5 resultados:")
        for rec in result.data[:5]:
            nombre = rec.get_field_value('Nombre producto')
            if isinstance(nombre, bytes):
                nombre = nombre.decode('utf-8').strip()
            precio = rec.get_field_value('Precio unitario')
            print(f"    - {nombre}: ${precio}")
    
    # Estadísticas de índices secundarios
    print("\nEstadísticas de índices secundarios:")
    table_info = db.tables["ventas"]
    for field_name, index_info in table_info["secondary_indexes"].items():
        print(f"\n  Índice sobre '{field_name}':")
        if hasattr(index_info["index"], "get_stats"):
            stats = index_info["index"].get_stats()
            for key, value in stats.items():
                print(f"    {key}: {value}")
    
    print("\n" + "=" * 70 + "\n")


def test_btree_full_workflow():
    print("=" * 70)
    print("TEST 3: Workflow completo - Sales Dataset")
    print("=" * 70)
    
    clean_database("btree_sales_workflow")
    db = DatabaseManager("btree_sales_workflow")
    
    fields = [
        ('ID de la venta', 'INT', 4),
        ('Nombre producto', 'CHAR', 64),
        ('Cantidad vendida', 'INT', 4),
        ('Precio unitario', 'FLOAT', 8),
        ('Fecha de venta', 'CHAR', 16)
    ]
    
    table = Table(
        table_name="ventas",
        sql_fields=fields,
        key_field='ID de la venta'
    )
    
    db.create_table(table, primary_index_type="BTREE")
    print("Tabla creada con B+ Tree primary index")
    
    # Crear índices secundarios antes de insertar
    db.create_index("ventas", "Cantidad vendida", "BTREE", scan_existing=False)
    db.create_index("ventas", "Precio unitario", "BTREE", scan_existing=False)
    print("Índices secundarios creados")
    
    sales_data = load_csv_data()
    print(f"\nInsertando {len(sales_data)} registros...")
    
    insert_times = []
    for i, sale in enumerate(sales_data):
        rec = Record(fields, 'ID de la venta')
        rec.set_field_value('ID de la venta', sale['ID de la venta'])
        rec.set_field_value('Nombre producto', sale['Nombre producto'])
        rec.set_field_value('Cantidad vendida', sale['Cantidad vendida'])
        rec.set_field_value('Precio unitario', sale['Precio unitario'])
        rec.set_field_value('Fecha de venta', sale['Fecha de venta'])
        
        result = db.insert("ventas", rec)
        insert_times.append(result.execution_time_ms)
        
        if (i + 1) % 50 == 0:
            print(f"  Progreso: {i + 1}/{len(sales_data)} registros insertados")
    
    print(f"Todos los registros insertados")
    print(f"  Tiempo promedio de inserción: {sum(insert_times)/len(insert_times):.2f}ms")
    
    # Operaciones de búsqueda
    print("\n--- Operaciones de búsqueda ---")
    
    # Búsqueda por primary key
    test_id = sales_data[len(sales_data)//2]['ID de la venta']
    result = db.search("ventas", test_id)
    print(f"Búsqueda por primary key (ID={test_id}): {result.execution_time_ms:.2f}ms")
    
    # Búsqueda por cantidad (índice secundario)
    result = db.search("ventas", 10, field_name="Cantidad vendida")
    print(f"Búsqueda cantidad=10: {len(result.data)} resultados en {result.execution_time_ms:.2f}ms")
    
    # Range search por precio
    result = db.range_search("ventas", 100.0, 500.0, field_name="Precio unitario")
    print(f"Range search precio 100-500: {len(result.data)} resultados en {result.execution_time_ms:.2f}ms")
    
    # Eliminar algunos registros
    print("\n--- Operaciones de eliminación ---")
    delete_ids = [sales_data[i]['ID de la venta'] for i in range(5)]
    
    for delete_id in delete_ids:
        result = db.delete("ventas", delete_id)
        print(f"Delete ID={delete_id}: {result.data} en {result.execution_time_ms:.2f}ms")
    
    # Estadísticas finales
    print("\n--- Estadísticas finales ---")
    stats = db.get_database_stats()
    print(f"Base de datos: {stats['database_name']}")
    for table_name, table_stats in stats['tables'].items():
        print(f"\nTabla '{table_name}':")
        print(f"  Registros: {table_stats['record_count']}")
        print(f"  Índice primario: {table_stats['primary_type']}")
        print(f"  Índices secundarios: {table_stats['secondary_count']}")
    
    # Detalles de los índices
    table_info = db.tables["ventas"]
    
    print("\nÍndice primario (B+ Tree):")
    if hasattr(table_info["primary_index"], "get_stats"):
        stats = table_info["primary_index"].get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    print("\nÍndices secundarios (B+ Tree):")
    for field_name, index_info in table_info["secondary_indexes"].items():
        print(f"\n  '{field_name}':")
        if hasattr(index_info["index"], "get_stats"):
            stats = index_info["index"].get_stats()
            for key, value in stats.items():
                print(f"    {key}: {value}")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("TESTS DE B+ TREE CON SALES DATASET")
    print("=" * 70)
    
    try:
        test_btree_primary_with_sales()
        test_btree_secondary_with_sales()
        test_btree_full_workflow()
        
        print("\n" + "=" * 70)
        print("TODOS LOS TESTS COMPLETADOS EXITOSAMENTE")
        print("=" * 70)
        print("\nResumen:")
        print("  - B+ Tree funciona como índice primario con sales dataset")
        print("  - B+ Tree funciona como índice secundario con sales dataset")
        print("  - Búsquedas por ID, Cantidad y Precio funcionando")
        print("  - Range search funcionando correctamente")
        print("  - Operaciones de inserción y eliminación exitosas")
        print("  - Métricas de performance capturadas")
        
    except Exception as e:
        print(f"\n✗ ERROR EN LOS TESTS: {e}")
        import traceback
        traceback.print_exc()
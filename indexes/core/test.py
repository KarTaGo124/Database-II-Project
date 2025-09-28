from record import Record, Table

def test_basic_record():
    print("=== Test Record Básico ===")

    sql_fields = [
        ("id", "INT", 4),
        ("nombre", "CHAR", 20),
        ("precio", "FLOAT", 4)
    ]

    table = Table("Productos", sql_fields, "id")

    print(f"Tabla: {table.table_name}")
    print(f"Campo clave: {table.key_field}")
    print(f"Tamaño record: {table.record_size} bytes")

    record = Record(sql_fields, "id")
    record.set_values(id=1, nombre="Laptop", precio=999.99)

    print(f"Record: {record}")
    print(f"Key: {record.get_key()}")

    data = record.pack()
    print(f"Packed size: {len(data)} bytes")

    unpacked = Record.unpack(data, sql_fields, "id")
    print(f"Unpacked: {unpacked}")
    print()

def test_with_extra_fields():
    print("=== Test con Campos Extras ===")

    sql_fields = [
        ("id", "INT", 4),
        ("nombre", "CHAR", 15)
    ]

    extra_fields = {
        "is_deleted": ("BOOL", 1),
        "next_ptr": ("INT", 4)
    }

    table = Table("Test", sql_fields, "id", extra_fields)

    print(f"SQL fields: {table.sql_fields}")
    print(f"All fields: {table.all_fields}")
    print(f"Record size: {table.record_size} bytes")

    record = Record(table.all_fields, "id")
    record.set_values(id=100, nombre="Test", is_deleted=False, next_ptr=-1)

    print(f"Record: {record}")
    record.print_detailed()
    print()

def test_restaurantes():
    print("=== Test Restaurantes Original ===")

    sql_fields = [
        ("id", "INT", 4),
        ("nombre", "CHAR", 20),
        ("ubicacion", "ARRAY", 2)
    ]

    table = Table("Restaurantes", sql_fields, "id")

    record = Record(sql_fields, "id")
    record.set_values(
        id=101,
        nombre="El Buen Sabor",
        ubicacion=[-12.0464, -77.0428]
    )

    print(f"Record: {record}")
    packed_data = record.pack()
    print(f"Packed size: {len(packed_data)} bytes")

    unpacked = Record.unpack(packed_data, sql_fields, "id")
    print(f"Unpacked: {unpacked}")
    record.print_detailed()
    print()

def test_array_default():
    print("=== Test Array Default (3 dimensiones) ===")

    sql_fields = [
        ("id", "INT", 4),
        ("coordenadas", "ARRAY", 3)
    ]

    record = Record(sql_fields, "id")
    record.set_values(id=1, coordenadas=[1.0, 2.0, 3.0])

    print(f"Record: {record}")
    packed_data = record.pack()
    print(f"Packed size: {len(packed_data)} bytes")

    unpacked = Record.unpack(packed_data, sql_fields, "id")
    print(f"Unpacked: {unpacked}")
    print(f"Spatial key: {unpacked.get_spatial_key('coordenadas')}")
    print()

if __name__ == "__main__":
    test_basic_record()
    test_with_extra_fields()
    test_restaurantes()
    test_array_default()
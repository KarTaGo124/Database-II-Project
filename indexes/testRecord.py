from Record import Record, TableMetadata
metadata = TableMetadata(
    "Restaurantes",
    [
        ["id", "INT", 4],
        ["nombre", "CHAR", 20],
        ["ubicacion", "ARRAY", 2] 
    ],
    "id"
)

record = metadata.record 
record.set_values(
    id=101,
    nombre="El Buen Sabor", 
    ubicacion=[-12.0464, -77.0428]
)


packed_data = record.pack() 
print(f"Packed Data: {packed_data}")
print(record.print_detailed())
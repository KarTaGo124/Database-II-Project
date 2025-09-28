#!/usr/bin/env python3

import struct
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from indexes.core.record import Record, Table, IndexRecord, IndexTable


def test_basic_record():
    print("=" * 50)
    print("TEST RECORD - FUNCIONALIDADES BÁSICAS")
    print("=" * 50)

    print("1. CREACIÓN DE RECORD")
    print("-" * 30)

    try:
        fields = [
            ("id", "INT", 4),
            ("name", "CHAR", 20),
            ("age", "INT", 4),
            ("salary", "FLOAT", 4),
            ("active", "BOOL", 1)
        ]

        record = Record(fields, "id")
        print(f"OK Record creado con {len(fields)} campos")
        print(f"   Tamaño del record: {record.RECORD_SIZE} bytes")
        print(f"   Campo clave: {record.key_field}")

    except Exception as e:
        print(f"ERROR Error creando record: {e}")
        return False

    print("\n2. ASIGNACIÓN DE VALORES")
    print("-" * 30)

    try:
        record.set_values(
            id=1001,
            name="Juan Perez",
            age=30,
            salary=5500.50,
            active=True
        )
        print("OK Valores asignados correctamente")
        print(f"   ID: {record.id}")
        print(f"   Name: {record.name}")
        print(f"   Age: {record.age}")
        print(f"   Salary: {record.salary}")
        print(f"   Active: {record.active}")

    except Exception as e:
        print(f"ERROR Error asignando valores: {e}")
        return False

    print("\n3. EMPAQUETADO Y DESEMPAQUETADO")
    print("-" * 30)

    try:
        packed_data = record.pack()
        print(f"OK Record empaquetado: {len(packed_data)} bytes")

        unpacked_record = Record.unpack(packed_data, fields, "id")
        print("OK Record desempaquetado correctamente")
        print(f"   ID: {unpacked_record.id}")
        print(f"   Name: {unpacked_record.name}")
        print(f"   Age: {unpacked_record.age}")
        print(f"   Salary: {unpacked_record.salary}")
        print(f"   Active: {unpacked_record.active}")

        if (record.id == unpacked_record.id and
            record.age == unpacked_record.age and
            abs(record.salary - unpacked_record.salary) < 0.01 and
            record.active == unpacked_record.active):
            print("OK Datos coinciden después del pack/unpack")
        else:
            print("ERROR Datos no coinciden")
            return False

    except Exception as e:
        print(f"ERROR Error en pack/unpack: {e}")
        return False

    print("\n4. MÉTODOS DE ACCESO")
    print("-" * 30)

    try:
        key_value = record.get_key()
        print(f"OK get_key(): {key_value}")

        field_value = record.get_field_value("name")
        print(f"OK get_field_value('name'): {field_value}")

        record.set_field_value("age", 31)
        print(f"OK set_field_value('age', 31): {record.age}")

    except Exception as e:
        print(f"ERROR Error en métodos de acceso: {e}")
        return False

    return True


def test_table():
    print("\n" + "=" * 50)
    print("TEST TABLE - FUNCIONALIDADES")
    print("=" * 50)

    print("1. CREACIÓN DE TABLE")
    print("-" * 30)

    try:
        sql_fields = [
            ("employee_id", "INT", 4),
            ("full_name", "CHAR", 50),
            ("department", "CHAR", 30),
            ("base_salary", "FLOAT", 4)
        ]

        extra_fields = {
            "bonus": ("FLOAT", 4),
            "is_manager": ("BOOL", 1)
        }

        table = Table("employees", sql_fields, "employee_id", extra_fields)
        print(f"OK Table '{table.table_name}' creada")
        print(f"   Campo clave: {table.key_field}")
        print(f"   Campos SQL: {len(table.sql_fields)}")
        print(f"   Campos totales: {len(table.all_fields)}")
        print(f"   Tamaño del record: {table.record_size} bytes")

    except Exception as e:
        print(f"ERROR Error creando table: {e}")
        return False

    print("\n2. CREACIÓN DE RECORD DESDE TABLE")
    print("-" * 30)

    try:
        record = Record(table.all_fields, table.key_field)
        record.set_values(
            employee_id=5001,
            full_name="Maria Rodriguez",
            department="Engineering",
            base_salary=7500.00,
            bonus=1000.00,
            is_manager=True
        )

        print("OK Record creado desde table")
        print(f"   Employee ID: {record.employee_id}")
        print(f"   Full Name: {record.full_name}")
        print(f"   Department: {record.department}")
        print(f"   Base Salary: {record.base_salary}")
        print(f"   Bonus: {record.bonus}")
        print(f"   Is Manager: {record.is_manager}")

    except Exception as e:
        print(f"ERROR Error creando record desde table: {e}")
        return False

    return True


def test_index_record():
    print("\n" + "=" * 50)
    print("TEST INDEX RECORD - ÍNDICES SECUNDARIOS")
    print("=" * 50)

    print("1. CREACIÓN DE INDEX RECORD")
    print("-" * 30)

    try:
        index_record = IndexRecord("CHAR", 30)
        print(f"OK IndexRecord creado")
        print(f"   Tamaño: {index_record.RECORD_SIZE} bytes")
        print(f"   Campo clave: {index_record.key_field}")

    except Exception as e:
        print(f"ERROR Error creando IndexRecord: {e}")
        return False

    print("\n2. ASIGNACIÓN DE DATOS DE ÍNDICE")
    print("-" * 30)

    try:
        index_record.set_index_data("Engineering", 5001)
        print("OK Datos de índice asignados")
        print(f"   Index Value: {index_record.index_value}")
        print(f"   Primary Key: {index_record.primary_key}")

    except Exception as e:
        print(f"ERROR Error asignando datos de índice: {e}")
        return False

    print("\n3. PACK/UNPACK INDEX RECORD")
    print("-" * 30)

    try:
        packed_data = index_record.pack()
        print(f"OK IndexRecord empaquetado: {len(packed_data)} bytes")

        unpacked_index = IndexRecord.unpack(
            packed_data,
            index_record.value_type_size,
            "index_value"
        )
        print("OK IndexRecord desempaquetado")
        print(f"   Index Value: {unpacked_index.index_value}")
        print(f"   Primary Key: {unpacked_index.primary_key}")

    except Exception as e:
        print(f"ERROR Error en pack/unpack IndexRecord: {e}")
        return False

    return True


def test_index_table():
    print("\n" + "=" * 50)
    print("TEST INDEX TABLE - TABLAS DE ÍNDICE")
    print("=" * 50)

    print("1. CREACIÓN DE INDEX TABLE")
    print("-" * 30)

    try:
        index_table = IndexTable.create_index_table("department", "CHAR", 30)
        print(f"OK IndexTable creada: {index_table.table_name}")
        print(f"   Campo clave: {index_table.key_field}")
        print(f"   Número de campos: {len(index_table.all_fields)}")

        for field_name, field_type, field_size in index_table.all_fields:
            print(f"     {field_name}: {field_type}[{field_size}]")

    except Exception as e:
        print(f"ERROR Error creando IndexTable: {e}")
        return False

    return True


def run_all_tests():
    print("EJECUTANDO TODOS LOS TESTS DE RECORD")
    print("=" * 60)

    tests = [
        test_basic_record,
        test_table,
        test_index_record,
        test_index_table
    ]

    passed = 0
    total = len(tests)

    for test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"\nERROR {test_func.__name__} FALLÓ")
        except Exception as e:
            print(f"\nERROR Exception en {test_func.__name__}: {e}")

    print("\n" + "=" * 60)
    print(f"RESULTADOS: {passed}/{total} tests pasaron")

    if passed == total:
        print("OK ¡TODOS LOS TESTS DE RECORD PASARON!")
    else:
        print(f"ERROR {total - passed} tests fallaron")

    print("=" * 60)

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
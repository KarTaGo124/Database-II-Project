import os
from typing import Dict, Any, List, Tuple, Optional
from .record import Table, Record


class DatabaseManager:

    INDEX_TYPES = {
        "SEQUENTIAL": {"primary": True, "secondary": False},
        "ISAM": {"primary": True, "secondary": True},
        "BTREE": {"primary": True, "secondary": True},
        "HASH": {"primary": False, "secondary": True},
        "RTREE": {"primary": False, "secondary": True}
    }

    def __init__(self, database_name: str = "default_db"):
        self.database_name = database_name
        self.tables = {}
        self.base_dir = os.path.join("data", "databases", database_name)
        os.makedirs(self.base_dir, exist_ok=True)

    def create_table(self, table: Table, primary_index_type: str = "ISAM", csv_filename: str = None):
        if not self._validate_primary_index(primary_index_type):
            raise ValueError(f"{primary_index_type} cannot be used as primary index")

        table_name = table.table_name
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists")

        table_info = {
            "table": table,
            "primary_index": None,
            "secondary_indexes": {},
            "primary_type": primary_index_type,
            "csv_filename": csv_filename
        }

        table_info["primary_index"] = self._create_primary_index(
            table, primary_index_type, csv_filename
        )

        self.tables[table_name] = table_info
        return True

    def create_index(self, table_name: str, field_name: str, index_type: str):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        if not self._validate_secondary_index(index_type):
            raise ValueError(f"{index_type} cannot be used as secondary index")

        table_info = self.tables[table_name]
        table = table_info["table"]

        field_info = self._get_field_info(table, field_name)
        if not field_info:
            raise ValueError(f"Field {field_name} not found in table {table_name}")

        if field_name in table_info["secondary_indexes"]:
            raise ValueError(f"Index on {field_name} already exists")

        secondary_index = self._create_secondary_index(
            table, field_name, index_type, table_info["csv_filename"]
        )

        table_info["secondary_indexes"][field_name] = {
            "index": secondary_index,
            "type": index_type
        }

        return True

    def insert(self, table_name: str, record: Record):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]

        primary_index.insert(record)

        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]
            secondary_index.insert(record)

    def search(self, table_name: str, key_value):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]
        return primary_index.search(key_value)

    def search_by_secondary(self, table_name: str, field_name: str, value):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name not in table_info["secondary_indexes"]:
            raise ValueError(f"No index on field {field_name}")

        secondary_index = table_info["secondary_indexes"][field_name]["index"]
        return secondary_index.search(value)

    def range_search(self, table_name: str, start_key, end_key, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name is None:
            primary_index = table_info["primary_index"]
            return primary_index.range_search(start_key, end_key)
        else:
            if field_name not in table_info["secondary_indexes"]:
                raise ValueError(f"No index on field {field_name}")

            secondary_index = table_info["secondary_indexes"][field_name]["index"]
            return secondary_index.range_search(start_key, end_key)

    def delete(self, table_name: str, key_value):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]

        record = primary_index.search(key_value)
        if record is None:
            return False

        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]
            secondary_index.delete(record)

        return primary_index.delete(key_value)

    def drop_index(self, table_name: str, field_name: str):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name not in table_info["secondary_indexes"]:
            return False

        secondary_index = table_info["secondary_indexes"][field_name]["index"]

        if hasattr(secondary_index, 'drop_index'):
            removed_files = secondary_index.drop_index()
        else:
            removed_files = []

        del table_info["secondary_indexes"][field_name]
        return removed_files

    def drop_table(self, table_name: str):
        if table_name not in self.tables:
            return False

        table_info = self.tables[table_name]
        removed_files = []

        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]
            if hasattr(secondary_index, 'drop_index'):
                removed_files.extend(secondary_index.drop_index())

        primary_index = table_info["primary_index"]
        if hasattr(primary_index, 'drop_table'):
            removed_files.extend(primary_index.drop_table())

        del self.tables[table_name]
        return removed_files

    def get_table_info(self, table_name: str):
        if table_name not in self.tables:
            return None

        table_info = self.tables[table_name]
        return {
            "table_name": table_name,
            "primary_type": table_info["primary_type"],
            "secondary_indexes": {
                field_name: index_info["type"]
                for field_name, index_info in table_info["secondary_indexes"].items()
            },
            "field_count": len(table_info["table"].all_fields),
            "csv_filename": table_info.get("csv_filename")
        }

    def list_tables(self):
        return list(self.tables.keys())

    def get_database_stats(self):
        stats = {
            "database_name": self.database_name,
            "table_count": len(self.tables),
            "tables": {}
        }

        for table_name, table_info in self.tables.items():
            table_stats = {
                "primary_type": table_info["primary_type"],
                "secondary_count": len(table_info["secondary_indexes"]),
                "secondary_types": list(table_info["secondary_indexes"].values())
            }

            try:
                primary_index = table_info["primary_index"]
                if hasattr(primary_index, 'scanAll'):
                    records = primary_index.scanAll()
                    table_stats["record_count"] = len(records) if records else 0
                else:
                    table_stats["record_count"] = 0
            except:
                table_stats["record_count"] = 0

            stats["tables"][table_name] = table_stats

        return stats

    def _validate_primary_index(self, index_type: str):
        return self.INDEX_TYPES.get(index_type, {}).get("primary", False)

    def _validate_secondary_index(self, index_type: str):
        return self.INDEX_TYPES.get(index_type, {}).get("secondary", False)

    def _get_field_info(self, table: Table, field_name: str):
        for fname, ftype, fsize in table.all_fields:
            if fname == field_name:
                return (ftype, fsize)
        return None

    def _create_primary_index(self, table: Table, index_type: str, csv_filename: str):
        if index_type == "ISAM":
            from ..isam.primary import ISAMPrimaryIndex

            primary_dir = os.path.join(self.base_dir, "primary")
            os.makedirs(primary_dir, exist_ok=True)
            primary_filename = os.path.join(primary_dir, "datos.dat")

            return ISAMPrimaryIndex(table, primary_filename)

        elif index_type == "SEQUENTIAL":
            raise NotImplementedError(f"Sequential file index not implemented yet")
        elif index_type == "BTREE":
            raise NotImplementedError(f"B+Tree index not implemented yet")

        raise NotImplementedError(f"Primary index type {index_type} not implemented yet")

    def _create_secondary_index(self, table: Table, field_name: str, index_type: str, csv_filename: str):
        field_type, field_size = self._get_field_info(table, field_name)

        if index_type == "ISAM":
            from ..isam.secondary import create_secondary_index

            secondary_dir = os.path.join(self.base_dir, "secondary")
            os.makedirs(secondary_dir, exist_ok=True)
            secondary_filename = os.path.join(secondary_dir, f"{field_name}_secondary.dat")

            primary_index = self.tables[table.table_name]["primary_index"]
            return create_secondary_index(field_name, field_type, field_size, primary_index, secondary_filename)

        elif index_type == "BTREE":
            raise NotImplementedError(f"B+Tree secondary index not implemented yet")
        elif index_type == "HASH":
            raise NotImplementedError(f"Hash secondary index not implemented yet")
        elif index_type == "RTREE":
            raise NotImplementedError(f"R-Tree secondary index not implemented yet")

        raise NotImplementedError(f"Secondary index type {index_type} not implemented yet")
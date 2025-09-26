import csv
import os
import struct
from typing import List
from .record import Record, Table

class SequentialFile:
    def __init__(self, main_file: str, aux_file: str, table: Table, k_rec=None):
        self.main_file = main_file
        self.aux_file = aux_file
        self.table = table
        self.list_of_types = table.all_fields
        self.key_field = table.key_field
        self.k = k_rec if k_rec is not None else 10 
        self.read_count = 0
        self.write_count = 0

        # Asegurar que el campo active está en extra_fields
        if not any(field[0] == 'active' for field in self.list_of_types):
            raise ValueError("La tabla debe tener un campo 'active' de tipo BOOL en extra_fields")

        if not os.path.exists(self.main_file):
            open(self.main_file, 'wb').close()
        if not os.path.exists(self.aux_file):
            open(self.aux_file, 'wb').close()

    def _create_record(self):
        return Record(self.list_of_types, self.key_field)

    def reset_counters(self):
        self.read_count = 0
        self.write_count = 0

    def get_stats(self):
        return {
            "Lecturas": self.read_count,
            "Escrituras": self.write_count,
            "Total": self.read_count + self.write_count
        }
    
    def get_file_size(self, filename: str) -> int:
        if not os.path.exists(filename):
            return 0
        file_size = os.path.getsize(filename)
        record_size = Record(self.list_of_types, self.key_field).RECORD_SIZE
        return file_size // record_size

    def show_all_records_from_main_and_aux(self) -> List[Record]:
        records = []
        record_size = Record(self.list_of_types, self.key_field).RECORD_SIZE
        
        with open(self.main_file, 'rb') as f:
            while data := f.read(record_size):
                self.read_count += 1
                rec = Record.unpack(data, self.list_of_types, self.key_field)
                if rec.active:
                    records.append(rec)
        
        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while data := f.read(record_size):
                    self.read_count += 1
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    if rec.active:
                        records.append(rec)
        
        return records
    
    def rebuild(self):
        records = []
        record_size = Record(self.list_of_types, self.key_field).RECORD_SIZE

        with open(self.main_file, 'rb') as f:
            while data := f.read(record_size):
                self.read_count += 1
                rec = Record.unpack(data, self.list_of_types, self.key_field)
                if rec.active:
                    records.append(rec)

        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while data := f.read(record_size):
                    self.read_count += 1
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    if rec.active:
                        records.append(rec)
            os.remove(self.aux_file)

        records.sort(key=lambda r: r.get_key())

        with open(self.main_file, 'wb') as f:
            for record in records:
                f.write(record.pack())
                self.write_count += 1
        
        open(self.aux_file, 'wb').close()

    def add(self, registro: Record):
        record_size = Record(self.list_of_types, self.key_field).RECORD_SIZE
        
        with open(self.main_file, 'rb') as f:
            while data := f.read(record_size):
                self.read_count += 1
                rec = Record.unpack(data, self.list_of_types, self.key_field)
                if rec.get_key() == registro.get_key() and rec.active:
                    return False, f"Error: Registro con clave {registro.get_key()} ya existe en main_file.", rec
        
        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while data := f.read(record_size):
                    self.read_count += 1
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    if rec.get_key() == registro.get_key() and rec.active:
                        return False, f"Error: Registro con clave {registro.get_key()} ya existe en aux_file.", rec

        registro.active = True
        with open(self.aux_file, 'ab') as f:
            f.write(registro.pack())
            self.write_count += 1
        
        aux_size = self.get_file_size(self.aux_file)
        if aux_size > self.k:
            self.rebuild()
            return True, f"Registro con clave {registro.get_key()} insertado (migrado a main_file por reconstrucción).", registro

        return True, f"Registro con clave {registro.get_key()} insertado en aux_file.", registro

    def remove(self, key):
        record_size = Record(self.list_of_types, self.key_field).RECORD_SIZE
        
        with open(self.main_file, 'r+b') as f:
            i = 0
            while data := f.read(record_size):
                self.read_count += 1
                rec = Record.unpack(data, self.list_of_types, self.key_field)
                if rec.get_key() == key:
                    if rec.active:
                        rec.active = False
                        f.seek(i * record_size)
                        f.write(rec.pack())
                        self.write_count += 1
                        return True, f"Registro con clave {key} eliminado en main_file.", rec
                    else:
                        return False, f"Error: Registro con clave {key} ya estaba eliminado en main_file.", None
                i += 1

        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'r+b') as f:
                i = 0
                while data := f.read(record_size):
                    self.read_count += 1
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    if rec.get_key() == key:
                        if rec.active:
                            rec.active = False
                            f.seek(i * record_size)
                            f.write(rec.pack())
                            self.write_count += 1
                            return True, f"Registro con clave {key} eliminado en aux_file.", rec
                        else:
                            return False, f"Error: Registro con clave {key} ya estaba eliminado en aux_file.", rec
                    i += 1

        return False, f"Error: Registro con clave {key} no existe.", None

    def search(self, key):
        record_size = Record(self.list_of_types, self.key_field).RECORD_SIZE
        
        main_size = self.get_file_size(self.main_file)
        if main_size > 0:
            with open(self.main_file, 'rb') as f:
                left, right = 0, main_size - 1
                
                while left <= right:
                    mid = (left + right) // 2
                    f.seek(mid * record_size)
                    data = f.read(record_size)
                    self.read_count += 1
                    
                    if not data:
                        break
                        
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    rec_key = rec.get_key()
                    
                    if rec_key == key:
                        if rec.active:
                            return True, f"Registro con clave {key} encontrado en main_file.", rec
                        else:
                            return False, f"Registro con clave {key} está eliminado en main_file.", None
                    elif rec_key < key:
                        left = mid + 1
                    else:
                        right = mid - 1

        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while True:
                    data = f.read(record_size)
                    if not data:
                        break
                        
                    self.read_count += 1
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    
                    if rec.get_key() == key:
                        if rec.active:
                            return True, f"Registro con clave {key} encontrado en aux_file.", rec
                        else:
                            return False, f"Registro con clave {key} está eliminado en aux_file.", None
        
        return False, f"Error: Registro con clave {key} no existe.", None

    def rangeSearch(self, begin_key, end_key):
        results = []
        all_records = self.show_all_records_from_main_and_aux()
        
        for record in all_records:
            key = record.get_key()
            if begin_key <= key <= end_key:
                results.append(record)
        
        results.sort(key=lambda r: r.get_key())
        return results

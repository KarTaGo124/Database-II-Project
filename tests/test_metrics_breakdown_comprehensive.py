import unittest
import os
import shutil
from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table, Record


class TestMetricsBreakdownComprehensive(unittest.TestCase):
    """Test completo para verificar el manejo correcto de métricas con breakdown en todas las operaciones"""

    def setUp(self):
        """Configuración inicial para cada test"""
        self.test_db_name = "test_metrics_breakdown_db"
        self.db = DatabaseManager(self.test_db_name)

        # Crear tabla de prueba con índice primario ISAM
        fields = [
            ("id", "INT", 4),
            ("nombre", "CHAR", 20),
            ("edad", "INT", 4),
            ("ciudad", "CHAR", 20)
        ]
        self.table = Table(
            table_name="personas",
            sql_fields=fields,
            key_field="id"
        )
        self.db.create_table(self.table, primary_index_type="ISAM")

        # Crear índices secundarios
        self.db.create_index("personas", "edad", "ISAM", scan_existing=False)
        self.db.create_index("personas", "ciudad", "ISAM", scan_existing=False)

    def tearDown(self):
        """Limpieza después de cada test"""
        db_path = os.path.join("data", "databases", self.test_db_name)
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

    def _create_record(self, id_val, nombre, edad, ciudad):
        """Helper para crear registros"""
        rec = Record(self.table.all_fields, "id")
        rec.set_field_value("id", id_val)
        rec.set_field_value("nombre", nombre)
        rec.set_field_value("edad", edad)
        rec.set_field_value("ciudad", ciudad)
        return rec

    def test_insert_breakdown(self):
        """Test: INSERT debe tener breakdown con primary_metrics y secondary_metrics por campo"""
        rec = self._create_record(1, "Juan", 25, "Lima")
        result = self.db.insert("personas", rec)

        # Verificar que el resultado tiene breakdown
        self.assertIsNotNone(result.operation_breakdown)

        # Verificar estructura del breakdown
        self.assertIn("primary_metrics", result.operation_breakdown)
        self.assertIn("secondary_metrics_edad", result.operation_breakdown)
        self.assertIn("secondary_metrics_ciudad", result.operation_breakdown)

        # Verificar que las métricas son diccionarios con las claves correctas
        for key in ["primary_metrics", "secondary_metrics_edad", "secondary_metrics_ciudad"]:
            self.assertIn("reads", result.operation_breakdown[key])
            self.assertIn("writes", result.operation_breakdown[key])
            self.assertIn("time_ms", result.operation_breakdown[key])

        # Verificar que la suma de métricas del breakdown coincide con el total
        total_reads_breakdown = sum(
            result.operation_breakdown[key]["reads"]
            for key in result.operation_breakdown
        )
        total_writes_breakdown = sum(
            result.operation_breakdown[key]["writes"]
            for key in result.operation_breakdown
        )

        self.assertEqual(result.disk_reads, total_reads_breakdown)
        self.assertEqual(result.disk_writes, total_writes_breakdown)

    def test_search_by_primary_no_breakdown(self):
        """Test: SEARCH por primary key NO debe tener breakdown"""
        # Insertar registro
        rec = self._create_record(1, "Juan", 25, "Lima")
        self.db.insert("personas", rec)

        # Buscar por primary key
        result = self.db.search("personas", 1)

        # No debe tener breakdown o debe estar vacío
        self.assertTrue(
            result.operation_breakdown is None or
            len(result.operation_breakdown) == 0
        )

    def test_search_by_secondary_with_breakdown(self):
        """Test: SEARCH por secondary index debe tener breakdown con primary_metrics y secondary_metrics"""
        # Insertar registros
        rec1 = self._create_record(1, "Juan", 25, "Lima")
        rec2 = self._create_record(2, "Maria", 25, "Cusco")
        self.db.insert("personas", rec1)
        self.db.insert("personas", rec2)

        # Buscar por secondary index (edad)
        result = self.db.search("personas", 25, field_name="edad")

        # Debe tener breakdown
        self.assertIsNotNone(result.operation_breakdown)
        self.assertIn("primary_metrics", result.operation_breakdown)
        self.assertIn("secondary_metrics", result.operation_breakdown)

        # Verificar suma de métricas
        total_reads = (
            result.operation_breakdown["primary_metrics"]["reads"] +
            result.operation_breakdown["secondary_metrics"]["reads"]
        )
        self.assertEqual(result.disk_reads, total_reads)

    def test_search_without_index_no_breakdown(self):
        """Test: SEARCH por campo sin índice NO debe tener breakdown (scan_all)"""
        # Insertar registro
        rec = self._create_record(1, "Juan", 25, "Lima")
        self.db.insert("personas", rec)

        # Buscar por campo sin índice (nombre)
        result = self.db.search("personas", "Juan", field_name="nombre")

        # NO debe tener breakdown o debe estar vacío (scan_all es operación del primary)
        self.assertTrue(
            result.operation_breakdown is None or
            len(result.operation_breakdown) == 0
        )

    def test_delete_by_primary_with_breakdown(self):
        """Test: DELETE por primary key debe tener breakdown con primary_metrics y secondary_metrics_campo"""
        # Insertar registro
        rec = self._create_record(1, "Juan", 25, "Lima")
        self.db.insert("personas", rec)

        # Delete por primary key
        result = self.db.delete("personas", 1)

        # Debe tener breakdown
        self.assertIsNotNone(result.operation_breakdown)
        self.assertIn("primary_metrics", result.operation_breakdown)
        self.assertIn("secondary_metrics_edad", result.operation_breakdown)
        self.assertIn("secondary_metrics_ciudad", result.operation_breakdown)

        # Verificar suma de métricas
        total_reads_breakdown = sum(
            result.operation_breakdown[key]["reads"]
            for key in result.operation_breakdown
        )
        self.assertEqual(result.disk_reads, total_reads_breakdown)

    def test_delete_by_secondary_with_breakdown(self):
        """Test: DELETE por secondary debe tener breakdown con secondary_metrics_{campo_busqueda} sumado"""
        # Insertar registros
        rec1 = self._create_record(1, "Juan", 25, "Lima")
        rec2 = self._create_record(2, "Maria", 25, "Cusco")
        self.db.insert("personas", rec1)
        self.db.insert("personas", rec2)

        # Delete por secondary index (edad)
        result = self.db.delete("personas", 25, field_name="edad")

        # Debe tener breakdown
        self.assertIsNotNone(result.operation_breakdown)
        self.assertIn("primary_metrics", result.operation_breakdown)

        # El campo edad debe tener sus métricas de búsqueda + delete sumadas
        self.assertIn("secondary_metrics_edad", result.operation_breakdown)

        # Los otros secondary indexes deben tener solo métricas de delete
        self.assertIn("secondary_metrics_ciudad", result.operation_breakdown)

        # Verificar suma de métricas
        total_reads_breakdown = sum(
            result.operation_breakdown[key]["reads"]
            for key in result.operation_breakdown
        )
        self.assertEqual(result.disk_reads, total_reads_breakdown)

    def test_delete_without_index_with_breakdown(self):
        """Test: DELETE por campo sin índice debe tener breakdown con primary_metrics y secondary_metrics_campo"""
        # Insertar registros
        rec1 = self._create_record(1, "Juan", 25, "Lima")
        rec2 = self._create_record(2, "Juan", 30, "Cusco")
        self.db.insert("personas", rec1)
        self.db.insert("personas", rec2)

        # Delete por campo sin índice (nombre)
        result = self.db.delete("personas", "Juan", field_name="nombre")

        # Debe tener breakdown
        self.assertIsNotNone(result.operation_breakdown)
        self.assertIn("primary_metrics", result.operation_breakdown)
        self.assertIn("secondary_metrics_edad", result.operation_breakdown)
        self.assertIn("secondary_metrics_ciudad", result.operation_breakdown)

        # Verificar suma de métricas
        total_reads_breakdown = sum(
            result.operation_breakdown[key]["reads"]
            for key in result.operation_breakdown
        )
        self.assertEqual(result.disk_reads, total_reads_breakdown)

    def test_range_search_by_primary_no_breakdown(self):
        """Test: RANGE_SEARCH por primary key NO debe tener breakdown"""
        # Insertar registros
        for i in range(1, 6):
            rec = self._create_record(i, f"Persona{i}", 20 + i, "Lima")
            self.db.insert("personas", rec)

        # Range search por primary key
        result = self.db.range_search("personas", 2, 4)

        # No debe tener breakdown o debe estar vacío
        self.assertTrue(
            result.operation_breakdown is None or
            len(result.operation_breakdown) == 0
        )

    def test_range_search_by_secondary_with_breakdown(self):
        """Test: RANGE_SEARCH por secondary debe tener breakdown"""
        # Insertar registros
        for i in range(1, 6):
            rec = self._create_record(i, f"Persona{i}", 20 + i, "Lima")
            self.db.insert("personas", rec)

        # Range search por secondary index (edad)
        result = self.db.range_search("personas", 22, 24, field_name="edad")

        # Debe tener breakdown
        self.assertIsNotNone(result.operation_breakdown)
        self.assertIn("primary_metrics", result.operation_breakdown)
        self.assertIn("secondary_metrics", result.operation_breakdown)

    def test_range_delete_by_primary_with_breakdown(self):
        """Test: RANGE_DELETE por primary debe tener breakdown"""
        # Insertar registros
        for i in range(1, 6):
            rec = self._create_record(i, f"Persona{i}", 20 + i, "Lima")
            self.db.insert("personas", rec)

        # Range delete por primary key
        result = self.db.range_delete("personas", 2, 4)

        # Debe tener breakdown
        self.assertIsNotNone(result.operation_breakdown)
        self.assertIn("primary_metrics", result.operation_breakdown)
        self.assertIn("secondary_metrics_edad", result.operation_breakdown)
        self.assertIn("secondary_metrics_ciudad", result.operation_breakdown)

    def test_range_delete_by_secondary_with_breakdown(self):
        """Test: RANGE_DELETE por secondary debe tener breakdown con métricas sumadas correctamente"""
        # Insertar registros
        for i in range(1, 6):
            rec = self._create_record(i, f"Persona{i}", 20 + i, "Lima")
            self.db.insert("personas", rec)

        # Range delete por secondary index (edad)
        result = self.db.range_delete("personas", 22, 24, field_name="edad")

        # Debe tener breakdown
        self.assertIsNotNone(result.operation_breakdown)
        self.assertIn("primary_metrics", result.operation_breakdown)
        self.assertIn("secondary_metrics_edad", result.operation_breakdown)
        self.assertIn("secondary_metrics_ciudad", result.operation_breakdown)

        # Verificar suma de métricas
        total_reads_breakdown = sum(
            result.operation_breakdown[key]["reads"]
            for key in result.operation_breakdown
        )
        self.assertEqual(result.disk_reads, total_reads_breakdown)

    def test_metrics_consistency_across_operations(self):
        """Test: Verificar que las métricas totales siempre suman correctamente"""
        # Insertar registros
        for i in range(1, 4):
            rec = self._create_record(i, f"Persona{i}", 20 + i, "Lima")
            result = self.db.insert("personas", rec)
            self._verify_metrics_sum(result)

        # Search por secondary
        result = self.db.search("personas", 21, field_name="edad")
        self._verify_metrics_sum(result)

        # Delete por secondary
        result = self.db.delete("personas", 22, field_name="edad")
        self._verify_metrics_sum(result)

        # Range search por secondary
        result = self.db.range_search("personas", 20, 25, field_name="edad")
        self._verify_metrics_sum(result)

    def _verify_metrics_sum(self, result):
        """Helper para verificar que la suma del breakdown coincide con el total"""
        if result.operation_breakdown:
            total_reads = sum(
                metrics["reads"]
                for metrics in result.operation_breakdown.values()
            )
            total_writes = sum(
                metrics["writes"]
                for metrics in result.operation_breakdown.values()
            )

            self.assertEqual(
                result.disk_reads,
                total_reads,
                f"Total reads mismatch: {result.disk_reads} != {total_reads}"
            )
            self.assertEqual(
                result.disk_writes,
                total_writes,
                f"Total writes mismatch: {result.disk_writes} != {total_writes}"
            )


if __name__ == "__main__":
    unittest.main()

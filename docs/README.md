# Database-II-Project

Proyecto de Base de Datos II. Estructura modular para índices, parser SQL, GUI y utilidades.


Para iniciar el front:

```
streamlit run gui/main.py
```

Ejemplos de consultas SQL:
```
-- BLOQUE: Ventas (CSV real, PK ISAM + secundarios BTREE)
CREATE TABLE Ventas (
  id INT KEY INDEX ISAM,
  nombre VARCHAR[50] INDEX BTREE,
  cantidad INT,
  precio FLOAT,
  fecha DATE INDEX BTREE
);
LOAD DATA FROM FILE "data/datasets/sales_dataset_unsorted.csv" INTO Ventas;
SELECT * FROM Ventas WHERE id = 403;
SELECT * FROM Ventas WHERE id = 56;
SELECT * FROM Ventas WHERE nombre = "Laptop";
SELECT * FROM Ventas WHERE nombre BETWEEN "C" AND "N";
SELECT * FROM Ventas WHERE precio = 813.52;
SELECT * FROM Ventas WHERE precio BETWEEN 700 AND 900;
SELECT * FROM Ventas WHERE fecha = "2024-07-30";
DELETE FROM Ventas WHERE nombre = "Laptop";
SELECT * FROM Ventas WHERE nombre = "Laptop";
SELECT * FROM Ventas WHERE id = 403;
DELETE FROM Ventas WHERE id = 403;
SELECT * FROM Ventas WHERE id = 403;

-- BLOQUE: Ventas_hash (PK ISAM + índice HASH en nombre)
CREATE TABLE Ventas_hash (
  id INT KEY INDEX ISAM,
  nombre VARCHAR[50],
  cantidad INT,
  precio FLOAT,
  fecha DATE
);
LOAD DATA FROM FILE "data/datasets/sales_dataset_unsorted.csv" INTO Ventas_hash;
CREATE INDEX ON Ventas_hash (nombre) USING HASH;
SELECT * FROM Ventas_hash WHERE nombre = "Laptop";
DELETE FROM Ventas_hash WHERE nombre = "Laptop";
SELECT * FROM Ventas_hash WHERE nombre = "Laptop";

-- BLOQUE: Ventas_seq (PK SEQUENTIAL)
CREATE TABLE Ventas_seq (
  id INT KEY INDEX SEQUENTIAL,
  nombre VARCHAR[50] INDEX BTREE,
  cantidad INT,
  precio FLOAT,
  fecha DATE
);
LOAD DATA FROM FILE "data/datasets/sales_dataset_unsorted.csv" INTO Ventas_seq;
SELECT * FROM Ventas_seq WHERE id = 403;
SELECT * FROM Ventas_seq WHERE id BETWEEN 50 AND 70;
DELETE FROM Ventas_seq WHERE id = 403;
SELECT * FROM Ventas_seq WHERE id = 403;

-- BLOQUE: Ventas_btree (PK BTREE + secundarios BTREE)
CREATE TABLE Ventas_btree (
  id INT KEY INDEX BTREE,
  nombre VARCHAR[50] INDEX BTREE,
  cantidad INT,
  precio FLOAT,
  fecha DATE INDEX BTREE
);
LOAD DATA FROM FILE "data/datasets/sales_dataset_unsorted.csv" INTO Ventas_btree;
SELECT * FROM Ventas_btree WHERE id = 403;
SELECT * FROM Ventas_btree WHERE id BETWEEN 50 AND 70;
SELECT * FROM Ventas_btree WHERE nombre = "Laptop";
SELECT * FROM Ventas_btree WHERE nombre BETWEEN "C" AND "N";
SELECT * FROM Ventas_btree WHERE precio = 813.52;
SELECT * FROM Ventas_btree WHERE precio BETWEEN 700 AND 900;
SELECT * FROM Ventas_btree WHERE fecha = "2024-07-30";
SELECT * FROM Ventas_btree WHERE fecha BETWEEN "2024-07-01" AND "2024-07-31";
DELETE FROM Ventas_btree WHERE nombre = "Laptop";
SELECT * FROM Ventas_btree WHERE nombre = "Laptop";
SELECT * FROM Ventas_btree WHERE id = 403;
DELETE FROM Ventas_btree WHERE id = 403;
SELECT * FROM Ventas_btree WHERE id = 403;
```

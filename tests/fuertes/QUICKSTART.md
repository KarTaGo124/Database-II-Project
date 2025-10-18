# Quick Start Guide

## Instalación (Solo la primera vez)

```bash
cd tests/fuertes
./install_dependencies.sh
```

Esto creará un entorno virtual e instalará todas las dependencias necesarias.

## Ejecutar los Tests

### Opción 1: Script Automático (Recomendado)
```bash
cd tests/fuertes
./run_tests.sh
```

### Opción 2: Manual
```bash
cd tests/fuertes
source venv/bin/activate
python tests.py
deactivate
```

## Resultados

Los resultados se guardarán en `tests/fuertes/test_results/`:
- `performance_metrics.csv` - Métricas en CSV
- `execution_time_by_operation.png` - Gráfico comparativo de tiempos
- `disk_accesses_comparison.png` - Gráfico de accesos a disco
- `index_type_summary.png` - Resumen de performance por índice
- `dataset_comparison_*.png` - Comparativas por dataset

## Configuración del Sample Size

Para cambiar el número de registros a probar, edita `tests.py`:

```python
# Línea 761
tester.run_all_tests(sample_size=500)  # Cambia 500 al valor deseado
```

Valores recomendados:
- **100-500**: Tests rápidos (~5-10 min)
- **1000-2000**: Tests intermedios (~15-30 min)
- **5000+**: Tests completos (~1+ hora)

## Datasets Incluidos

1. **U.S. Airbnb** - 226K registros
2. **NYC Airbnb** - 48K registros
3. **World Cities** - 41K registros
4. **Bank Marketing** - 41K registros
5. **Starbucks Locations** - 25-30K registros

## Índices Probados

### Primarios:
- ISAM
- BTREE (B+ Tree Clustered)
- SEQUENTIAL

### Secundarios:
- BTREE (B+ Tree Unclustered)
- HASH (Extendible Hashing)
- RTREE (R-Tree para datos espaciales)

## Operaciones Testeadas

- **INSERT** - Inserción de registros
- **SEARCH** - Búsqueda exacta por clave
- **RANGE_SEARCH** - Búsqueda por rango

## Troubleshooting

### Error: "No module named 'pandas'"
Ejecuta `./install_dependencies.sh` primero.

### Error: "CSV file not found"
Verifica que todos los archivos CSV estén en sus carpetas respectivas.

### Tests muy lentos
Reduce el `sample_size` en `tests.py` (línea 761).

### Out of Memory
Reduce el `sample_size` a 100 o menos.

## Ver Resultados

Los gráficos PNG se pueden abrir con cualquier visor de imágenes.
El CSV se puede analizar con Excel, LibreOffice Calc, o pandas.

```python
import pandas as pd
df = pd.read_csv('test_results/performance_metrics.csv')
print(df.head())
```

## Soporte

Para más detalles, ver [README.md](README.md)

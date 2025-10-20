import os
import sys
import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st
import pandas as pd

# ====== Bootstrap del path del proyecto ======
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sql_parser.parser import parse
from sql_parser.executor import Executor
from indexes.core.database_manager import DatabaseManager


# ======================= Configuración =======================
STATE_DIR = _ROOT / "gui" / "data" / "gui_state"
STATE_FILE = STATE_DIR / "state.json"
DB_BASE_DIR = _ROOT / "gui" / "data" / "databases"

# ======================= CSS Personalizado =======================
def load_custom_css():
    st.markdown("""
        <style>
        /* Estilo general */
        .main .block-container {
            padding-top: 2rem;
            padding-bottom: 3rem;
        }
        
        /* Editor SQL */
        .stTextArea textarea {
            font-family: 'Fira Code', 'Courier New', monospace;
            font-size: 14px;
            line-height: 1.6;
        }
        
        /* Tabs personalizados */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 10px 20px;
            border-radius: 8px 8px 0 0;
            font-weight: 500;
        }
        
        /* Expanders */
        .streamlit-expanderHeader {
            font-size: 16px;
            font-weight: 600;
            border-radius: 8px;
        }
        
        /* Métricas */
        [data-testid="stMetricValue"] {
            font-size: 24px;
            font-weight: 600;
        }
        
        /* Botones en sidebar */
        div[data-testid="stSidebar"] .stButton > button {
            width: 100%;
            padding: 0.7rem 1rem;
            border-radius: 8px;
            text-align: left;
            font-size: 0.95rem;
            border: 1px solid rgba(250,250,250,.1);
            transition: all 0.2s ease;
        }
        
        div[data-testid="stSidebar"] .stButton > button:hover {
            border-color: rgba(250,250,250,.3);
            transform: translateX(4px);
        }
        
        /* Headers */
        h1 {
            color: #1E88E5;
            font-weight: 700;
        }
        
        h2 {
            color: #43A047;
            font-weight: 600;
            margin-top: 2rem;
        }
        
        h3 {
            color: #FB8C00;
            font-weight: 600;
        }
        
        /* Code blocks */
        code {
            background-color: rgba(240, 242, 246, 0.1);
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 13px;
        }
        
        /* Info boxes */
        .info-box {
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #1E88E5;
            background-color: rgba(30, 136, 229, 0.1);
            margin: 1rem 0;
        }
        
        .success-box {
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #43A047;
            background-color: rgba(67, 160, 71, 0.1);
            margin: 1rem 0;
        }
        
        .warning-box {
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #FB8C00;
            background-color: rgba(251, 140, 0, 0.1);
            margin: 1rem 0;
        }
        
        /* Tables */
        .dataframe {
            border-radius: 8px;
            overflow: hidden;
        }
        
        /* Cards */
        .card {
            padding: 1.5rem;
            border-radius: 12px;
            background-color: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(250, 250, 250, 0.1);
            margin: 1rem 0;
        }
        </style>
    """, unsafe_allow_html=True)


# ======================= Helpers de sesión =======================
def _reset_services():
    """Reinicia DB, Executor y estados de UI."""
    keys_to_clear = [
        "db", "executor", "open_table", "schema_changed",
        "is_executing", "pending_sql", "last_results", 
        "last_sql", "last_tables"
    ]
    for key in keys_to_clear:
        st.session_state.pop(key, None)


def get_db() -> DatabaseManager:
    """Obtiene o crea instancia de DatabaseManager en session_state."""
    if "db" not in st.session_state:
        st.session_state.db = DatabaseManager(database_name="frontend_db", base_path=DB_BASE_DIR)
    return st.session_state.db


def get_executor() -> Executor:
    """Obtiene o crea instancia de Executor en session_state."""
    if "executor" not in st.session_state:
        st.session_state.executor = Executor(get_db())
    return st.session_state.executor


def _fmt_value(v):
    """Formatea un valor para display."""
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, (list, tuple)):
        return type(v)(round(x, 4) if isinstance(x, float) else x for x in v)
    if isinstance(v, (bytes, bytearray)):
        try:
            return bytes(v).decode("utf-8", "ignore").rstrip("\x00").strip()
        except Exception:
            return str(v)
    return v


def _project_records(records) -> List[Dict[str, Any]]:
    """Convierte Record objects a lista de diccionarios con valores formateados."""
    if not records:
        return []
    
    try:
        names = [n for (n, _, _) in records[0].value_type_size]
    except (AttributeError, IndexError):
        return []
    
    out: List[Dict[str, Any]] = []
    for r in records:
        obj: Dict[str, Any] = {c: _fmt_value(getattr(r, c, None)) for c in names}
        out.append(obj)
    return out


# ======================= Persistencia de estado UI =======================
def _ensure_state_dir():
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def _serialize_result(out: Any) -> Dict[str, Any]:
    """Serializa un resultado de operación para guardar en JSON."""
    payload: Dict[str, Any] = {}
    
    if hasattr(out, "data"):
        data = out.data
        if isinstance(data, list) and data and hasattr(data[0], "value_type_size"):
            payload["data"] = _project_records(data)
        elif isinstance(data, (dict, list, str, int, float, bool, type(None))):
            payload["data"] = data
        else:
            payload["data"] = str(data)
    else:
        payload["data"] = out if isinstance(out, (dict, list, str, int, float, bool, type(None))) else str(out)
    
    for attr in ("disk_reads", "disk_writes", "execution_time_ms", "rebuild_triggered", "operation_breakdown"):
        if hasattr(out, attr):
            val = getattr(out, attr)
            try:
                json.dumps(val)
                payload[attr] = val
            except Exception:
                payload[attr] = None
    
    return payload


def _save_ui_state():
    """Guarda estado de UI a disco."""
    _ensure_state_dir()
    state: Dict[str, Any] = {
        "open_table": st.session_state.get("open_table"),
        "last_sql": st.session_state.get("last_sql"),
        "last_results": None,
        "last_tables": st.session_state.get("last_tables", []),
    }
    
    if "last_results" in st.session_state:
        serial: List[Dict[str, Any]] = []
        for item in st.session_state["last_results"]:
            if "error" in item:
                serial.append({"plan": item.get("plan"), "error": item.get("error")})
            else:
                serial.append({
                    "plan": item.get("plan"),
                    "result": _serialize_result(item.get("result"))
                })
        state["last_results"] = serial
    
    try:
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _load_ui_state():
    """Carga estado de UI desde disco."""
    if not STATE_FILE.exists():
        return
    
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return
    
    st.session_state.setdefault("open_table", state.get("open_table"))
    st.session_state.setdefault("last_sql", state.get("last_sql"))
    st.session_state.setdefault("last_tables", state.get("last_tables", []))
    
    if "last_results" not in st.session_state and state.get("last_results"):
        hydrated: List[Dict[str, Any]] = []
        for item in state["last_results"]:
            plan = item.get("plan")
            if "error" in item:
                hydrated.append({"plan": plan, "error": item.get("error")})
            else:
                result = item.get("result", {})
                
                class ResultShim:
                    def __init__(self, d: Dict[str, Any]):
                        self.data = d.get("data")
                        self.disk_reads = d.get("disk_reads", 0) or 0
                        self.disk_writes = d.get("disk_writes", 0) or 0
                        self.execution_time_ms = d.get("execution_time_ms", 0.0) or 0.0
                        self.rebuild_triggered = d.get("rebuild_triggered", False)
                        self.operation_breakdown = d.get("operation_breakdown")
                
                hydrated.append({"plan": plan, "result": ResultShim(result)})
        
        st.session_state["last_results"] = hydrated


# ======================= Documentación de Consultas =======================
def render_query_documentation():
    """Renderiza documentación completa de tipos de consultas soportadas."""
    st.header("📚 Documentación de Consultas SQL")
    
    tabs = st.tabs([
        "📋 DDL", 
        "🔍 Consultas", 
        "➕ Inserción", 
        "❌ Eliminación", 
        "🗂️ Índices",
        "🌍 Espaciales"
    ])
    
    # Tab 1: DDL (CREATE TABLE, LOAD DATA, DROP)
    with tabs[0]:
        st.markdown("### Definición de Datos (DDL)")
        
        with st.expander("🏗️ CREATE TABLE - Crear Tabla", expanded=True):
            st.markdown("""
            Crea una nueva tabla con campos y opcionalmente índices.
            
            **Sintaxis:**
            ```sql
            CREATE TABLE nombre_tabla (
                campo1 tipo [KEY] [INDEX tipo_indice],
                campo2 tipo [INDEX tipo_indice],
                ...
            );
            ```
            
            **Tipos de datos soportados:**
            - `INT` - Entero de 4 bytes
            - `FLOAT` - Punto flotante de 4 bytes
            - `VARCHAR[n]` - Cadena de texto de longitud n (ejemplo: VARCHAR[100])
            - `DATE` - Fecha en formato YYYY-MM-DD
            - `ARRAY[FLOAT]` - Array 2D (para datos espaciales, ejemplo: latitud, longitud)
            - `ARRAY[FLOAT, n]` - Array n-dimensional (ejemplo: ARRAY[FLOAT, 3] para 3D)
            
            **Tipos de índices:**
            - `SEQUENTIAL` - Solo primario
            - `ISAM` - Solo primario (predeterminado)
            - `BTREE` - Primario y secundario (recomendado)
            - `HASH` - Solo secundario (búsquedas exactas muy rápidas)
            - `RTREE` - Solo secundario (para datos espaciales ARRAY)
            """)
            
            st.code("""CREATE TABLE Restaurantes (
    id INT KEY INDEX BTREE,
    nombre VARCHAR[100] INDEX BTREE,
    ubicacion ARRAY[FLOAT] INDEX RTREE,
    rating FLOAT INDEX HASH,
    precio_promedio FLOAT,
    fecha_apertura DATE
);""", language="sql")
        
        with st.expander("📂 LOAD DATA - Cargar desde CSV"):
            st.markdown("""
            Carga datos desde un archivo CSV a una tabla existente.
            
            **Sintaxis básica:**
            ```sql
            LOAD DATA FROM FILE "ruta/archivo.csv" INTO nombre_tabla;
            ```
            
            **Con mapeo de arrays (para campos espaciales):**
            ```sql
            LOAD DATA FROM FILE "ruta/archivo.csv" INTO nombre_tabla
            WITH MAPPING (
                campo_array = ARRAY(columna_csv1, columna_csv2)
            );
            ```
            
            **Notas:**
            - La ruta debe ser relativa a la raíz del proyecto
            - Los nombres de columnas del CSV deben coincidir con los campos de la tabla
            - Para campos ARRAY, usa WITH MAPPING para especificar qué columnas del CSV corresponden a cada dimensión
            """)
            
            st.code("""LOAD DATA FROM FILE "data/datasets/restaurantes.csv" INTO Restaurantes
WITH MAPPING (
    ubicacion = ARRAY(latitud, longitud)
);""", language="sql")
        
        with st.expander("🗑️ DROP TABLE - Eliminar Tabla"):
            st.markdown("""
            Elimina una tabla y todos sus índices asociados.
            
            **Sintaxis:**
            ```sql
            DROP TABLE nombre_tabla;
            ```
            
            ⚠️ **Advertencia:** Esta operación es irreversible y elimina todos los datos.
            """)
            
            st.code("""DROP TABLE Restaurantes;""", language="sql")
    
    # Tab 2: Consultas (SELECT)
    with tabs[1]:
        st.markdown("### Consultas de Datos (SELECT)")
        
        with st.expander("🔍 SELECT básico", expanded=True):
            st.markdown("""
            Recupera todos los registros de una tabla.
            
            **Sintaxis:**
            ```sql
            SELECT * FROM nombre_tabla;
            SELECT campo1, campo2 FROM nombre_tabla;
            ```
            """)
            
            st.code("""SELECT * FROM Restaurantes;

SELECT nombre, rating FROM Restaurantes;""", language="sql")
        
        with st.expander("🎯 SELECT con filtro de igualdad (WHERE =)"):
            st.markdown("""
            Busca registros que coincidan exactamente con un valor.
            
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla WHERE campo = valor;
            ```
            
            **Optimización:**
            - Si hay índice en el campo, la búsqueda es O(log n)
            - Sin índice, realiza escaneo completo O(n)
            """)
            
            st.code("""SELECT * FROM Restaurantes WHERE id = 42;

SELECT * FROM Restaurantes WHERE nombre = "La Buena Mesa";

SELECT * FROM Restaurantes WHERE rating = 4.5;""", language="sql")
        
        with st.expander("📊 SELECT con rango (BETWEEN)"):
            st.markdown("""
            Recupera registros dentro de un rango de valores.
            
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla WHERE campo BETWEEN valor_min AND valor_max;
            ```
            
            **Tipos soportados:**
            - Numéricos (INT, FLOAT): rango inclusivo
            - VARCHAR: orden lexicográfico
            - DATE: orden cronológico
            
            **Nota:** BETWEEN es inclusivo en ambos extremos: [min, max]
            """)
            
            st.code("""SELECT * FROM Restaurantes
WHERE rating BETWEEN 4.0 AND 5.0;

SELECT * FROM Restaurantes
WHERE id BETWEEN 100 AND 200;

SELECT * FROM Restaurantes
WHERE fecha_apertura BETWEEN "2023-01-01" AND "2023-12-31";""", language="sql")
    
    # Tab 3: Inserción
    with tabs[2]:
        st.markdown("### Inserción de Datos (INSERT)")
        
        with st.expander("➕ INSERT básico", expanded=True):
            st.markdown("""
            Inserta un nuevo registro en la tabla.
            
            **Sintaxis con todos los campos:**
            ```sql
            INSERT INTO tabla VALUES (valor1, valor2, ...);
            ```
            
            **Sintaxis con campos específicos:**
            ```sql
            INSERT INTO tabla (campo1, campo2) VALUES (valor1, valor2);
            ```
            
            **Notas:**
            - Los valores deben coincidir con el tipo de dato del campo
            - Para arrays espaciales, usa la sintaxis (x, y) o (x, y, z, ...)
            - Si el registro ya existe (clave duplicada), la inserción falla
            """)
            
            st.code("""INSERT INTO Restaurantes VALUES (
    1001,
    "Nuevo Restaurante",
    (-12.0464, -77.0428),
    4.5,
    50.0,
    "2024-01-15"
);

INSERT INTO Restaurantes (id, nombre, ubicacion, rating)
VALUES (1002, "Café Central", (-12.0500, -77.0400), 4.2);""", language="sql")
    
    # Tab 4: Eliminación
    with tabs[3]:
        st.markdown("### Eliminación de Datos (DELETE)")
        
        with st.expander("❌ DELETE con condición", expanded=True):
            st.markdown("""
            Elimina registros que cumplan una condición.
            
            **Sintaxis:**
            ```sql
            DELETE FROM tabla WHERE condicion;
            ```
            
            **Condiciones soportadas:**
            - Igualdad: `campo = valor`
            - Rango: `campo BETWEEN min AND max`
            
            **Proceso:**
            1. Busca registros que cumplan la condición
            2. Elimina de todos los índices secundarios
            3. Elimina del índice primario
            
            ⚠️ **Advertencia:** Sin WHERE, eliminaría todos los registros (actualmente no soportado por seguridad)
            """)
            
            st.code("""DELETE FROM Restaurantes WHERE id = 1001;

DELETE FROM Restaurantes WHERE nombre = "Café Viejo";

DELETE FROM Restaurantes WHERE rating BETWEEN 0.0 AND 2.0;

DELETE FROM Restaurantes
WHERE fecha_apertura BETWEEN "2020-01-01" AND "2020-12-31";""", language="sql")
    
    # Tab 5: Índices
    with tabs[4]:
        st.markdown("### Gestión de Índices")
        
        with st.expander("🔨 CREATE INDEX - Crear Índice Secundario", expanded=True):
            st.markdown("""
            Crea un índice secundario en un campo existente para acelerar búsquedas.
            
            **Sintaxis:**
            ```sql
            CREATE INDEX ON tabla (campo) USING tipo_indice;
            ```
            
            **Tipos disponibles para índices secundarios:**
            - `BTREE` - Árbol B+, soporta búsquedas exactas y por rango
            - `HASH` - Hash extensible, solo búsquedas exactas (muy rápido)
            - `RTREE` - Árbol R, para datos espaciales
            
            **Cuándo usar cada tipo:**
            - **BTREE**: Cuando necesitas rangos o datos ordenados
            - **HASH**: Cuando solo haces búsquedas exactas y quieres máxima velocidad
            - **RTREE**: Para campos ARRAY con coordenadas espaciales
            
            **Proceso:**
            - El sistema escanea todos los registros existentes
            - Construye el índice con todas las entradas
            - Las operaciones futuras mantienen el índice actualizado
            """)
            
            st.code("""CREATE INDEX ON Restaurantes (nombre) USING BTREE;

CREATE INDEX ON Restaurantes (rating) USING HASH;

CREATE INDEX ON Restaurantes (ubicacion) USING RTREE;""", language="sql")
        
        with st.expander("🗑️ DROP INDEX - Eliminar Índice"):
            st.markdown("""
            Elimina un índice secundario de un campo.
            
            **Sintaxis:**
            ```sql
            DROP INDEX nombre_campo;
            ```
            
            **Notas:**
            - Solo puede eliminar índices secundarios (no el primario)
            - Libera espacio en disco
            - Las consultas seguirán funcionando pero más lentas
            """)
            
            st.code("""DROP INDEX nombre;

DROP INDEX ubicacion;""", language="sql")
    
    # Tab 6: Consultas Espaciales
    with tabs[5]:
        st.markdown("### Consultas Espaciales (R-Tree)")
        
        st.markdown("""
        Las consultas espaciales requieren:
        1. Campo tipo `ARRAY[FLOAT]` o `ARRAY[FLOAT, n]`
        2. Índice `RTREE` en ese campo
        
        **Casos de uso comunes:**
        - Encontrar puntos de interés cercanos
        - Búsqueda de vecinos más próximos
        - Análisis geoespacial
        """)
        
        with st.expander("🎯 Búsqueda por Radio (IN RADIUS)", expanded=True):
            st.markdown("""
            Encuentra todos los puntos dentro de un radio desde un punto central.
            
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla
            WHERE campo_espacial IN ((x, y), radio);
            ```

            **IMPORTANTE:**
            - Usa **doble paréntesis**: `IN ((x, y), radio)`
            - El radio está en las **mismas unidades que las coordenadas** (grados para lat/lon)
            
            **Parámetros:**
            - `(x, y)`: Coordenadas del punto central (ejemplo: latitud, longitud)
            - `radio`: Radio de búsqueda en grados decimales
              - Para GPS: ~0.01 grados ≈ 1.1 km
              - Para GPS: ~0.05 grados ≈ 5.5 km
            
            **Cálculo:**
            - Crea un bounding box: [x-radio, y-radio] a [x+radio, y+radio]
            - Retorna todos los puntos dentro de ese rectángulo
            - Usa el índice R-Tree para búsqueda espacial eficiente
            
            **Complejidad:**
            - Con R-Tree: O(log n + k) donde k = resultados
            - Sin índice: O(n) escaneo completo
            """)
            
            st.code("""SELECT * FROM Restaurantes
WHERE ubicacion IN ((40.7614, -73.9776), 0.01);

SELECT nombre, ubicacion, rating FROM Restaurantes
WHERE ubicacion IN ((40.7614, -73.9776), 0.05);

SELECT * FROM Restaurantes
WHERE ubicacion IN ((40.7614, -73.9776), 0.005);""", language="sql")
        
        with st.expander("🏆 K Vecinos Más Cercanos (NEAREST K)", expanded=True):
            st.markdown("""
            Encuentra los K puntos más cercanos a un punto de referencia.
            
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla
            WHERE campo_espacial NEAREST ((x, y), k);
            ```

            **IMPORTANTE:**
            - Usa **doble paréntesis**: `NEAREST ((x, y), k)`

            **Parámetros:**
            - `(x, y)`: Coordenadas del punto de referencia
            - `k`: Número de vecinos más cercanos a retornar
            
            **Características:**
            - Retorna exactamente K resultados (o menos si no hay suficientes)
            - Ordenados por distancia (más cercano primero)
            - Ideal para recomendaciones basadas en proximidad
            
            **Casos de uso:**
            - "Los 5 restaurantes más cercanos a mi ubicación"
            - "Las 10 tiendas más próximas"
            - Sistemas de recomendación geográfica
            """)
            
            st.code("""SELECT nombre, ubicacion, rating FROM Restaurantes
WHERE ubicacion NEAREST ((40.758, -73.9855), 5);

SELECT * FROM Restaurantes
WHERE ubicacion NEAREST ((40.758, -73.9855), 3);

SELECT id, nombre, ubicacion FROM Restaurantes
WHERE ubicacion NEAREST ((40.758, -73.9855), 10);""", language="sql")
        
        st.info("""
        💡 **Consejos para consultas espaciales con R-Tree:**

        - **Sintaxis especial:** Usa doble paréntesis: `IN ((x, y), radio)` y `NEAREST ((x, y), k)`
        - **Unidades:** Para GPS (lat/lon), el radio está en grados decimales:
          - 0.001° ≈ 111 metros
          - 0.01° ≈ 1.1 kilómetros
          - 0.05° ≈ 5.5 kilómetros
          - 0.1° ≈ 11 kilómetros
        - **Índice requerido:** Crea un índice RTREE en campos ARRAY[FLOAT] para mejor rendimiento
        - **Formato de coordenadas:** (latitud, longitud) - ejemplo: (40.758, -73.9855)
        """)


# ======================= Ejecución SQL =======================
DDL_PLANS = {"CreateTablePlan", "DropTablePlan", "CreateIndexPlan", "DropIndexPlan", "LoadDataPlan"}


def _is_ddl_plan(plan_name: str) -> bool:
    return plan_name in DDL_PLANS


def execute_sql_block(sql_text: str) -> List[Dict[str, Any]]:
    """
    Parsea y ejecuta un bloque SQL, retornando lista de resultados.
    """
    if not (sql_text or "").strip():
        return []
    
    execu = get_executor()
    results: List[Dict[str, Any]] = []
    
    try:
        plans = parse(sql_text)
    except Exception as e:
        stmts = [s.strip() for s in sql_text.split(';') if s.strip()]
        err_msg = str(e)
        hint = ""
        if stmts:
            hint = f"\n\n💡 Sentencia problemática:\n`{stmts[-1][:200]}`"
        
        return [{
            "plan": "ParseError",
            "error": f"❌ Error de parseo SQL:\n{err_msg}{hint}"
        }]
    
    if not plans:
        return [{"plan": "EmptyQuery", "error": "⚠️ No se generaron planes ejecutables"}]
    
    total = len(plans)
    progress = st.progress(0, text="Ejecutando…")
    
    for i, plan in enumerate(plans, 1):
        plan_name = type(plan).__name__
        try:
            with st.spinner(f"Ejecutando {i}/{total}: {plan_name}"):
                out = execu.execute(plan)
            results.append({"plan": plan_name, "result": out})
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg.lower():
                error_msg = f"❌ {error_msg}\n\n💡 Tip: Ejecuta CREATE TABLE + LOAD DATA primero."
            results.append({"plan": plan_name, "error": error_msg})
        finally:
            progress.progress(i / total, text=f"Ejecutando… {i}/{total}")
    
    progress.empty()
    return results


def _to_dataframe(data: Any) -> Optional[pd.DataFrame]:
    """Convierte data a DataFrame si es posible."""
    if isinstance(data, list):
        if not data:
            return pd.DataFrame()
        if isinstance(data[0], dict):
            return pd.DataFrame(data)
    return None
# En tu código, verifica qué archivos existen

# ======================= UI: Sidebar =======================
def sidebar_tables():
    """Renderiza panel lateral con lista de tablas."""
    db = get_db()
    st.sidebar.header("📊 Tablas en BD")
    
    try:
        tables_live = list(db.list_tables())
    except Exception as e:
        st.sidebar.error(f"Error: {e}")
        tables_live = []
    
    live_mode = len(tables_live) > 0
    tables = tables_live or st.session_state.get("last_tables", [])
    
    if live_mode:
        st.session_state["last_tables"] = [str(t) for t in tables_live]
        _save_ui_state()
    
    if not tables:
        st.sidebar.info("📭 Sin tablas\n\nEjecuta CREATE TABLE para comenzar")
        return
    
    is_exec = st.session_state.get("is_executing", False)
    if is_exec:
        st.sidebar.info("⏳ Ejecutando…")
        for t in tables:
            st.sidebar.markdown(f"- `{t}`")
        return
    
    disabled = not live_mode
    selected = st.session_state.get("open_table")
    
    for t in tables:
        tname = str(t)
        is_selected = selected == tname
        button_type = "primary" if is_selected else "secondary"
        
        pressed = st.sidebar.button(
            f"{'📂' if is_selected else '📄'} {tname}",
            key=f"tbl_{tname}",
            use_container_width=True,
            type=button_type,
            disabled=disabled
        )
        
        if pressed:
            st.session_state["open_table"] = None if is_selected else tname
            _save_ui_state()
            st.rerun()
    
    # Info de tabla seleccionada
    tname = st.session_state.get("open_table")
    if tname and live_mode:
        st.sidebar.divider()
        st.sidebar.markdown(f"### 📋 Info: `{tname}`")
        
        try:
            info = db.get_table_info(tname)
            if info:
                col1, col2 = st.sidebar.columns(2)
                col1.metric("Campos", info.get("field_count", "—"))
                col2.metric("Índices 2°", len(info.get("secondary_indexes", {})))
                
                st.sidebar.caption("**Índice Primario**")
                st.sidebar.code(info.get("primary_type", "—"), language="text")
                
                sec_idxs = info.get("secondary_indexes", {})
                if sec_idxs:
                    st.sidebar.caption("**Índices Secundarios**")
                    for field, idx_type in sec_idxs.items():
                        st.sidebar.text(f"• {field}: {idx_type}")
        except Exception as e:
            st.sidebar.error(f"Error: {e}")
        
        if st.sidebar.button("✖️ Cerrar", use_container_width=True):
            st.session_state["open_table"] = None
            _save_ui_state()
            st.rerun()


# ======================= UI Principal =======================
def main():
    st.set_page_config(
        page_title="SGBD - Sistema de Gestión", 
        layout="wide", 
        initial_sidebar_state="expanded",
        page_icon="🗄️"
    )
    
    load_custom_css()
    
    # Header principal
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("🗄️ Sistema de Gestión de Base de Datos")
        st.caption("Sistema de indexación multi-estructura con soporte espacial")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📚 Ver Documentación", use_container_width=True):
            st.session_state["show_docs"] = not st.session_state.get("show_docs", False)
    
    # Mostrar/Ocultar documentación
    if st.session_state.get("show_docs", False):
        render_query_documentation()
        st.divider()
    
    _load_ui_state()
    
    # Ejecución diferida
    if st.session_state.get("is_executing") and st.session_state.get("pending_sql") is not None:
        sql_to_run = st.session_state.pop("pending_sql", "")
        
        try:
            results = execute_sql_block(sql_to_run)
        except Exception as e:
            results = [{"plan": "SystemError", "error": f"❌ Error interno: {e}"}]
        
        st.session_state["last_results"] = results
        st.session_state["last_sql"] = sql_to_run
        st.session_state["is_executing"] = False
        
        if any(_is_ddl_plan(r.get("plan", "")) for r in results if isinstance(r, dict)):
            try:
                live = list(get_db().list_tables())
                st.session_state["last_tables"] = [str(t) for t in live]
            except Exception:
                pass
        
        _save_ui_state()
        st.rerun()
    
    # Sidebar
    sidebar_tables()
    
    # Tabs principales
    tab_editor, tab_csv = st.tabs(["✏️ Editor SQL", "📂 Cargar CSV"])
    
    # Tab: Editor SQL
    with tab_editor:
        st.subheader("📝 Editor de Consultas")
        
        default_sql = """CREATE TABLE Restaurantes (
    id INT KEY INDEX BTREE,
    nombre VARCHAR[100] INDEX BTREE,
    ubicacion ARRAY[FLOAT] INDEX RTREE,
    rating FLOAT INDEX HASH,
    fecha_apertura DATE
);

LOAD DATA FROM FILE "data/datasets/restaurantes.csv" INTO Restaurantes
WITH MAPPING (ubicacion = ARRAY(latitud, longitud));

SELECT * FROM Restaurantes
WHERE ubicacion NEAREST ((40.758, -73.9855), 10);"""

        # Mantener el SQL default si no hay last_sql guardado
        if "last_sql" not in st.session_state or not st.session_state["last_sql"]:
            st.session_state["last_sql"] = default_sql

        sql_text = st.text_area(
            "Escribe tus consultas SQL (separadas por `;`)",
            value=st.session_state["last_sql"],
            height=350,
            key="sql_editor",
            help="Escribe múltiples sentencias SQL separadas por punto y coma"
        )
        
        col1, col2, col3, col4 = st.columns([2, 2, 2, 6])
        
        with col1:
            run_btn = st.button(
                "▶️ Ejecutar",
                type="primary",
                disabled=st.session_state.get("is_executing", False),
                use_container_width=True
            )
        
        with col2:
            if st.button("📋 Limpiar Editor", use_container_width=True):
                st.session_state["last_sql"] = ""
                st.rerun()
        
        with col3:
            clear_btn = st.button(
                "🗑️ Limpiar BD",
                disabled=st.session_state.get("is_executing", False),
                use_container_width=True,
                help="Elimina todas las tablas e índices"
            )
        
        if clear_btn:
            db_dir = DB_BASE_DIR
            try:
                # Primero cerrar todas las tablas e índices
                if "db" in st.session_state:
                    db = st.session_state.db
                    for table_name in list(db.tables.keys()):
                        try:
                            db.drop_table(table_name)
                        except Exception:
                            pass

                # Resetear servicios
                _reset_services()

                # Ahora borrar archivos
                if db_dir.exists():
                    shutil.rmtree(db_dir)
                db_dir.mkdir(parents=True, exist_ok=True)

                if STATE_FILE.exists():
                    STATE_FILE.unlink()

                st.success("✅ Base de datos limpiada exitosamente")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error al limpiar: {e}")
        
        if run_btn and not st.session_state.get("is_executing"):
            if not (sql_text or "").strip():
                st.warning("⚠️ No hay sentencias para ejecutar")
            else:
                st.session_state["pending_sql"] = sql_text
                st.session_state["is_executing"] = True
                _save_ui_state()
                st.rerun()
    
    # Tab: Cargar CSV
    with tab_csv:
        st.subheader("📂 Cargar Archivo CSV")
        
        st.info("""
        📌 **Instrucciones:**
        1. Sube tu archivo CSV usando el botón de abajo
        2. El archivo se guardará en `gui/data/datasets/`
        3. Usa la sentencia LOAD DATA en el editor SQL para importarlo
        """)
        
        uploaded = st.file_uploader(
            "Selecciona un archivo CSV",
            type=["csv"],
            key="csv_uploader",
            help="El archivo debe tener headers que coincidan con los nombres de campos de tu tabla"
        )
        
        if uploaded:
            upload_dir = _ROOT / "gui" / "data" / "datasets"
            upload_dir.mkdir(parents=True, exist_ok=True)
            dst = upload_dir / uploaded.name
            
            with open(dst, "wb") as f:
                f.write(uploaded.getbuffer())
            
            st.success(f"✅ Archivo guardado: `{dst.relative_to(_ROOT)}`")
            
            # Preview del CSV
            try:
                df_preview = pd.read_csv(dst, nrows=5)
                st.markdown("**Vista previa (primeras 5 filas):**")
                st.dataframe(df_preview, use_container_width=True)
            except Exception as e:
                st.warning(f"No se pudo previsualizar: {e}")
            
            st.markdown("**Sentencia SQL para cargar:**")
            st.code(f'LOAD DATA FROM FILE "{dst.relative_to(_ROOT).as_posix()}" INTO TuTabla;', language="sql")
    

    # Resultados
    if "last_results" in st.session_state:
        st.divider()
        st.header("📊 Resultados de Ejecución")
        
        results = st.session_state["last_results"]
        
        for i, item in enumerate(results, 1):
            plan_name = item.get("plan", "Unknown")
            
            # Determinar icono según tipo de operación
            icon = "📋"
            if "Create" in plan_name:
                icon = "🏗️"
            elif "Load" in plan_name:
                icon = "📂"
            elif "Select" in plan_name:
                icon = "🔍"
            elif "Insert" in plan_name:
                icon = "➕"
            elif "Delete" in plan_name:
                icon = "❌"
            elif "Drop" in plan_name:
                icon = "🗑️"
            
            with st.expander(f"{icon} **Operación {i}**: `{plan_name}`", expanded=True):
                if "error" in item:
                    st.error(item["error"])
                    continue
                
                res = item["result"]
                data = getattr(res, "data", None)
                time_ms = getattr(res, "execution_time_ms", 0.0) or 0.0
                reads = getattr(res, "disk_reads", 0) or 0
                writes = getattr(res, "disk_writes", 0) or 0
                breakdown = getattr(res, "operation_breakdown", None)
                rebuild = getattr(res, "rebuild_triggered", False)
                
                if isinstance(data, list) and data and hasattr(data[0], "value_type_size"):
                    data = _project_records(data)
                
                df = _to_dataframe(data)
                
                # Tabs para resultados
                if df is not None and not df.empty:
                    result_tabs = st.tabs(["📋 Datos", "⚡ Rendimiento", "🔧 Detalles Técnicos"])
                else:
                    result_tabs = st.tabs(["✅ Resultado", "⚡ Rendimiento", "🔧 Detalles Técnicos"])
                
                # Tab 1: Datos o Resultado
                with result_tabs[0]:
                    if df is not None and not df.empty:
                        st.dataframe(
                            df,
                            use_container_width=True,
                            hide_index=True,
                            height=min(600, len(df) * 35 + 38)
                        )
                        
                        # Estadísticas básicas para columnas numéricas
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            st.markdown("**📊 Estadísticas:**")
                            stats_cols = st.columns(min(4, len(numeric_cols)))
                            for idx, col in enumerate(numeric_cols[:4]):
                                with stats_cols[idx]:
                                    st.metric(
                                        f"Promedio {col}",
                                        f"{df[col].mean():.2f}",
                                        help=f"Min: {df[col].min():.2f} | Max: {df[col].max():.2f}"
                                    )
                        
                        st.success(f"✅ **{len(df)} registro(s)** encontrado(s)")
                    elif data:
                        if isinstance(data, (str, int, float, bool)):
                            st.success(f"✅ {data}")
                        else:
                            st.json(data)
                    else:
                        st.info("✅ Operación completada sin datos de retorno")
                
                # Tab 2: Rendimiento
                with result_tabs[1]:
                    # Métricas principales
                    perf_cols = st.columns(4)
                    perf_cols[0].metric(
                        "⏱️ Tiempo Total",
                        f"{time_ms:.2f} ms",
                        help="Tiempo de ejecución en milisegundos"
                    )
                    perf_cols[1].metric(
                        "📖 Lecturas",
                        reads,
                        help="Número de lecturas a disco"
                    )
                    perf_cols[2].metric(
                        "✏️ Escrituras",
                        writes,
                        help="Número de escrituras a disco"
                    )
                    perf_cols[3].metric(
                        "📊 Total I/O",
                        reads + writes,
                        help="Total de accesos a disco"
                    )
                    
                    if rebuild:
                        st.warning("⚠️ **Reconstrucción de índice ejecutada** durante esta operación")
                    
                    # Desglose por índices
                    if breakdown:
                        st.divider()
                        st.markdown("### 📈 Desglose Detallado")
                        
                        # Crear visualización de métricas
                        metrics_data = []
                        
                        if "primary_metrics" in breakdown:
                            pm = breakdown["primary_metrics"]
                            metrics_data.append({
                                "Índice": "Primario",
                                "Lecturas": pm.get("reads", 0),
                                "Escrituras": pm.get("writes", 0),
                                "Tiempo (ms)": pm.get("time_ms", 0)
                            })
                        
                        sec_metrics = {k: v for k, v in breakdown.items() if k.startswith("secondary_metrics_")}
                        for key, sm in sec_metrics.items():
                            field = key.replace("secondary_metrics_", "")
                            metrics_data.append({
                                "Índice": f"Secundario ({field})",
                                "Lecturas": sm.get("reads", 0),
                                "Escrituras": sm.get("writes", 0),
                                "Tiempo (ms)": sm.get("time_ms", 0)
                            })
                        
                        if metrics_data:
                            df_metrics = pd.DataFrame(metrics_data)
                            st.dataframe(df_metrics, use_container_width=True, hide_index=True)
                            
                            # Gráfico de barras de I/O
                            st.markdown("**Distribución de Accesos a Disco:**")
                            chart_data = df_metrics.set_index("Índice")[["Lecturas", "Escrituras"]]
                            st.bar_chart(chart_data)
                
                # Tab 3: Detalles Técnicos
                with result_tabs[2]:
                    # Plan de ejecución
                    last_sql = st.session_state.get("last_sql", "")
                    try:
                        plans = parse(last_sql)
                        plan_obj = plans[i - 1] if i - 1 < len(plans) else None
                    except Exception:
                        plan_obj = None
                    
                    if plan_obj:
                        st.markdown("**📋 Plan de Ejecución:**")
                        attrs = {k: v for k, v in vars(plan_obj).items() if not k.startswith("_")}
                        plan_info = {"tipo": type(plan_obj).__name__, **attrs}
                        st.json(plan_info, expanded=True)
                        
                        # Información de tabla relacionada
                        table_name = attrs.get("table") or attrs.get("into_table")
                        if table_name:
                            st.markdown("**🗂️ Información de Tabla:**")
                            try:
                                info = get_db().get_table_info(str(table_name))
                                if info:
                                    info_cols = st.columns(3)
                                    info_cols[0].metric("Tipo Primario", info.get("primary_type", "—"))
                                    info_cols[1].metric("Campos", info.get("field_count", 0))
                                    info_cols[2].metric("Índices 2°", len(info.get("secondary_indexes", {})))
                                    
                                    if info.get("secondary_indexes"):
                                        st.markdown("**Índices Secundarios:**")
                                        idx_df = pd.DataFrame([
                                            {"Campo": k, "Tipo": v}
                                            for k, v in info["secondary_indexes"].items()
                                        ])
                                        st.dataframe(idx_df, use_container_width=True, hide_index=True)
                            except Exception as e:
                                st.warning(f"No se pudo obtener info de tabla: {e}")
                    
                    # Estadísticas generales de BD
                    st.markdown("**📊 Estadísticas de Base de Datos:**")
                    try:
                        stats = get_db().get_database_stats()
                        stats_cols = st.columns(2)
                        stats_cols[0].metric("Total Tablas", stats.get("table_count", 0))
                        
                        total_records = sum(
                            t.get("record_count", 0) 
                            for t in stats.get("tables", {}).values()
                        )
                        stats_cols[1].metric("Total Registros", total_records)
                        
                        if stats.get("tables"):
                            st.markdown("**Tablas en BD:**")
                            tables_data = []
                            for tname, tdata in stats["tables"].items():
                                tables_data.append({
                                    "Tabla": tname,
                                    "Registros": tdata.get("record_count", 0),
                                    "Primario": tdata.get("primary_type", "—"),
                                    "Índices 2°": tdata.get("secondary_count", 0)
                                })
                            
                            st.dataframe(
                                pd.DataFrame(tables_data),
                                use_container_width=True,
                                hide_index=True
                            )
                    except Exception as e:
                        st.warning(f"No se pudieron obtener estadísticas: {e}")
    
    _save_ui_state()
    
    # Footer
    st.divider()
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.caption("🗄️ Sistema de Gestión de Base de Datos Multi-índice")
    with col2:
        st.caption("Soporta: BTREE, HASH, RTREE, ISAM, Sequential File")
    with col3:
        if st.button("🔄 Refrescar", key="refresh_footer"):
            st.rerun()


if __name__ == "__main__":
    main()
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
STATE_DIR = _ROOT / "data" / "gui_state"
STATE_FILE = STATE_DIR / "state.json"


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
        st.session_state.db = DatabaseManager(database_name="frontend_db")
    return st.session_state.db


def get_executor() -> Executor:
    """Obtiene o crea instancia de Executor en session_state."""
    if "executor" not in st.session_state:
        st.session_state.executor = Executor(get_db())
    return st.session_state.executor


def _project_records(records) -> List[Dict[str, Any]]:
    """Convierte Record objects a lista de diccionarios con valores formateados."""
    if not records:
        return []
    
    def _fmt(v):
        if isinstance(v, float):
            return round(v, 2)
        if isinstance(v, (list, tuple)):
            return type(v)(round(x, 2) if isinstance(x, float) else x for x in v)
        if isinstance(v, (bytes, bytearray)):
            try:
                return bytes(v).decode("utf-8", "ignore").rstrip("\x00").strip()
            except Exception:
                return str(v)
        return v
    
    try:
        names = [n for (n, _, _) in records[0].value_type_size]
    except (AttributeError, IndexError):
        return []
    
    out: List[Dict[str, Any]] = []
    for r in records:
        obj: Dict[str, Any] = {c: _fmt(getattr(r, c, None)) for c in names}
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
        # Convertir Record objects a dicts para JSON
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


# ======================= Ejecución SQL =======================
DDL_PLANS = {"CreateTablePlan", "DropTablePlan", "CreateIndexPlan", "DropIndexPlan", "LoadDataPlan"}


def _is_ddl_plan(plan_name: str) -> bool:
    return plan_name in DDL_PLANS


def execute_sql_block(sql_text: str) -> List[Dict[str, Any]]:
    """
    Parsea y ejecuta un bloque SQL, retornando lista de resultados.
    Cada resultado: {"plan": str, "result": OperationResult} o {"plan": str, "error": str}
    """
    if not (sql_text or "").strip():
        return []
    
    execu = get_executor()
    results: List[Dict[str, Any]] = []
    
    # Parseo con manejo robusto de errores
    try:
        plans = parse(sql_text)
    except Exception as e:
        # Intentar identificar la sentencia problemática
        stmts = [s.strip() for s in sql_text.split(';') if s.strip()]
        err_msg = str(e)
        hint = ""
        if stmts:
            hint = f"\n\n💡 Sentencia sospechosa:\n`{stmts[-1][:200]}`"
        
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
                error_msg = f"❌ {error_msg}\n\n💡 Tip: Tras F5, las tablas en memoria se pierden. Ejecuta LIMPIAR BD + CREATE TABLE + LOAD DATA de nuevo."
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


# ======================= UI: Sidebar (Tablas) =======================
def sidebar_tables():
    """Renderiza panel lateral con lista de tablas e información."""
    st.sidebar.markdown(
        """
        <style>
        div[data-testid="stSidebar"] { font-size: 0.95rem; }
        div[data-testid="stSidebar"] .stButton > button {
            width: 100%;
            padding: 0.6rem 1rem;
            border-radius: 8px;
            text-align: left;
            font-size: 0.95rem;
            border: 1px solid rgba(250,250,250,.1);
        }
        div[data-testid="stSidebar"] .stButton > button:hover {
            border-color: rgba(250,250,250,.3);
        }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    db = get_db()
    st.sidebar.header("📊 Tablas")
    
    try:
        tables_live = list(db.list_tables())
    except Exception as e:
        st.sidebar.error(f"Error listando tablas: {e}")
        tables_live = []
    
    live_mode = len(tables_live) > 0
    tables = tables_live or st.session_state.get("last_tables", [])
    
    if live_mode:
        st.session_state["last_tables"] = [str(t) for t in tables_live]
        _save_ui_state()
    
    if not tables:
        st.sidebar.info("Sin tablas. Ejecuta CREATE TABLE para empezar.")
        return
    
    is_exec = st.session_state.get("is_executing", False)
    if is_exec:
        st.sidebar.info("⏳ Ejecutando consulta…")
        for t in tables:
            st.sidebar.markdown(f"- `{t}`")
        return
    
    disabled = not live_mode
    selected = st.session_state.get("open_table")
    
    for t in tables:
        tname = str(t)
        pressed = st.sidebar.button(
            f"📄 {tname}",
            key=f"tbl_{tname}",
            use_container_width=True,
            type="secondary",
            disabled=disabled
        )
        if pressed:
            st.session_state["open_table"] = None if selected == tname else tname
            _save_ui_state()
    
    tname = st.session_state.get("open_table")
    if tname:
        with st.sidebar.expander(f"ℹ️ {tname}", expanded=True):
            if not live_mode:
                st.warning("⚠️ Metadatos no disponibles tras recarga. Ejecuta una operación para refrescar.")
                if st.button("Cerrar", key="close_offline", use_container_width=True):
                    st.session_state["open_table"] = None
                    _save_ui_state()
                    st.rerun()
                return
            
            try:
                info = db.get_table_info(tname)
                if not info:
                    st.warning("Tabla no disponible.")
                else:
                    col1, col2 = st.columns(2)
                    col1.metric("Campos", info.get("field_count", "—"))
                    col2.metric("Índices 2°", len(info.get("secondary_indexes", {})))
                    
                    st.caption("**Detalles**")
                    st.json({
                        "Primario": info.get("primary_type", "—"),
                        "Secundarios": info.get("secondary_indexes", {}),
                        "CSV": info.get("csv_filename", "—")
                    })
            except Exception as e:
                st.error(f"Error: {e}")
            
            if st.button("Cerrar", key="close_info", use_container_width=True):
                st.session_state["open_table"] = None
                _save_ui_state()
                st.rerun()


# ======================= UI Principal =======================
def main():
    st.set_page_config(page_title="Mi SGBD — Frontend", layout="wide", initial_sidebar_state="expanded")
    st.title("🗄️ Mi SGBD")
    
    _load_ui_state()
    
    # Ejecución diferida (tras rerun)
    if st.session_state.get("is_executing") and st.session_state.get("pending_sql") is not None:
        sql_to_run = st.session_state.pop("pending_sql", "")
        
        try:
            results = execute_sql_block(sql_to_run)
        except Exception as e:
            results = [{"plan": "SystemError", "error": f"❌ Error interno: {e}"}]
        
        st.session_state["last_results"] = results
        st.session_state["last_sql"] = sql_to_run
        st.session_state["is_executing"] = False
        
        # Actualizar snapshot de tablas si hubo DDL
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
    
    # Subir CSV
    with st.expander("📂 Subir CSV (opcional)"):
        uploaded = st.file_uploader("Selecciona un archivo CSV", type=["csv"], key="csv_uploader")
        if uploaded:
            upload_dir = Path("data/datasets/_uploads")
            upload_dir.mkdir(parents=True, exist_ok=True)
            dst = upload_dir / uploaded.name
            with open(dst, "wb") as f:
                f.write(uploaded.getbuffer())
            st.success(f"✅ Guardado: `{dst.as_posix()}`")
            st.code(f'LOAD DATA FROM FILE "{dst.as_posix()}" INTO TuTabla;', language="sql")
    
    # Editor SQL
    st.subheader("📝 Editor SQL")
    placeholder = (
        "-- Ejemplo completo:\n"
        "CREATE TABLE Ventas (\n"
        "  id INT KEY INDEX ISAM,\n"
        "  nombre VARCHAR[50] INDEX BTREE,\n"
        "  cantidad INT,\n"
        "  precio FLOAT,\n"
        "  fecha DATE INDEX BTREE\n"
        ");\n\n"
        'LOAD DATA FROM FILE "data/datasets/sales_dataset_unsorted.csv" INTO Ventas;\n\n'
        "SELECT * FROM Ventas WHERE id = 403;\n"
        "SELECT * FROM Ventas WHERE nombre BETWEEN \"C\" AND \"N\";"
    )
    
    sql_text = st.text_area(
        "Sentencias SQL (separadas por `;`)",
        value=placeholder,
        height=240,
        key="sql_editor"
    )
    
    col1, col2, col3 = st.columns([1, 1, 6])
    
    with col1:
        run_btn = st.button(
            "▶️ Ejecutar",
            type="primary",
            disabled=st.session_state.get("is_executing", False),
            use_container_width=True
        )
    
    with col2:
        clear_btn = st.button(
            "🗑️ Limpiar BD",
            disabled=st.session_state.get("is_executing", False),
            use_container_width=True
        )
    
    if clear_btn:
        db_dir = _ROOT / "data" / "database"
        try:
            if db_dir.exists():
                shutil.rmtree(db_dir)
            db_dir.mkdir(parents=True, exist_ok=True)
            
            if STATE_FILE.exists():
                STATE_FILE.unlink()
            
            _reset_services()
            st.success("✅ Base de datos limpiada y reinicializada")
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
    
    # Render resultados
    if "last_results" in st.session_state:
        results = st.session_state["last_results"]
        last_sql = st.session_state.get("last_sql", "")
        
        st.divider()
        st.subheader("📊 Resultados")
        
        for i, item in enumerate(results, 1):
            plan_name = item.get("plan", "—")
            
            with st.expander(f"**Plan {i}**: `{plan_name}`", expanded=True):
                if "error" in item:
                    st.error(item["error"])
                    
                    # Mantener estructura de tabs para consistencia
                    tab1, tab2, tab3 = st.tabs(["Result", "Explain", "Transx"])
                    with tab1:
                        st.info("Sin datos por error")
                    with tab2:
                        st.info("No disponible")
                    with tab3:
                        st.info("No disponible")
                    continue
                
                res = item["result"]
                data = getattr(res, "data", None)
                time_ms = getattr(res, "execution_time_ms", 0.0) or 0.0
                reads = getattr(res, "disk_reads", 0) or 0
                writes = getattr(res, "disk_writes", 0) or 0
                breakdown = getattr(res, "operation_breakdown", None)
                
                # Convertir Records a dicts si es necesario
                if isinstance(data, list) and data and hasattr(data[0], "value_type_size"):
                    data = _project_records(data)
                
                df = _to_dataframe(data)
                
                tab1, tab2, tab3 = st.tabs(["Result", "Explain", "Transx"])
                
                with tab1:
                    if df is not None and not df.empty:
                        st.dataframe(
                            df,
                            use_container_width=True,
                            hide_index=True,
                            height=min(400, len(df) * 35 + 38)
                        )
                        st.caption(f"📋 {len(df)} filas")
                    elif data:
                        if isinstance(data, (str, int, float, bool)):
                            st.success(f"✅ {data}")
                        else:
                            st.json(data)
                    else:
                        st.info("Sin datos de retorno")
                
                with tab2:
                    try:
                        plans = parse(last_sql)
                        plan_obj = plans[i - 1] if i - 1 < len(plans) else None
                    except Exception:
                        plan_obj = None
                    
                    if plan_obj:
                        attrs = {k: v for k, v in vars(plan_obj).items() if not k.startswith("_")}
                        st.json({"tipo_plan": type(plan_obj).__name__, **attrs})
                        
                        table_name = attrs.get("table") or attrs.get("into_table")
                        if table_name:
                            try:
                                info = get_db().get_table_info(str(table_name))
                                if info:
                                    st.markdown("**📊 Metadatos de tabla**")
                                    st.json(info)
                            except Exception:
                                pass
                    else:
                        st.info("Plan no disponible")
                
                with tab3:
                    st.markdown("**📈 Estadísticas generales**")
                    try:
                        stats = get_db().get_database_stats()
                        st.json(stats)
                    except Exception:
                        st.info("No disponibles")
                    
                    st.markdown("**🔍 Detalle I/O**")
                    if breakdown:
                        st.json(breakdown)
                    else:
                        st.info("Sin desglose")
                
                # Footer con métricas
                try:
                    row_count = len(data) if isinstance(data, list) else (0 if data is None else 1)
                except Exception:
                    row_count = 0
                
                st.caption(
                    f"⏱️ {time_ms / 1000.0:.3f}s  |  "
                    f"📖 {reads} lecturas  |  "
                    f"✏️ {writes} escrituras  |  "
                    f"📊 {row_count} registros"
                )
    
    _save_ui_state()


if __name__ == "__main__":
    main()

import os
import re
import io
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# -----------------------
# Config / UI (vanguardista)
# -----------------------
st.set_page_config(page_title="Leontief ETL", page_icon="🧮", layout="wide")

st.markdown(
    """
    <style>
      .stApp {
        background: radial-gradient(1200px 600px at 20% 10%, rgba(170,0,255,0.18), transparent 60%),
                    radial-gradient(1000px 500px at 80% 30%, rgba(0,255,200,0.14), transparent 60%),
                    radial-gradient(900px 550px at 50% 90%, rgba(255,180,0,0.12), transparent 60%),
                    #06060a;
        color: #e8e8ef;
      }
      h1, h2, h3 { letter-spacing: 0.5px; }
      .card {
        border: 1px solid rgba(255,255,255,0.10);
        background: rgba(255,255,255,0.04);
        border-radius: 18px;
        padding: 18px 18px;
        box-shadow: 0 18px 60px rgba(0,0,0,0.35);
      }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
      .ok { color: #62ffa5; }
      .bad { color: #ff5a7a; }
      .muted { color: rgba(232,232,239,0.65); }
      .pill {
        display:inline-block; padding: 4px 10px; border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.14);
        background: rgba(255,255,255,0.04);
        margin-right: 6px;
        font-size: 12px;
      }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("# 🧮 Leontief ETL")
st.markdown(
    '<div class="muted">Sube tu matriz <b>A</b> (input-output) y tu matriz <b>D</b> (demanda). Validamos, calculamos <b>X</b> y lo insertamos en MySQL.</div>',
    unsafe_allow_html=True
)

# -----------------------
# Helpers
# -----------------------
def sanitize_table_name(name: str) -> str:
    """
    Convierte el "código de producto" (derivado del nombre del archivo) en un nombre de tabla seguro.
    - Solo: letras, números y guion bajo
    - No puede empezar por número
    - Longitud razonable (MySQL permite hasta 64 chars)
    """
    base = os.path.splitext(os.path.basename(name))[0]
    base = base.strip().lower()
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "producto"
    if re.match(r"^[0-9]", base):
        base = "p_" + base
    return base[:64]

def read_csv_matrix(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.read()
    text_data = raw.decode("utf-8", errors="replace")

    # Quita líneas vacías
    lines = [ln for ln in text_data.splitlines() if ln.strip() != ""]
    text_data = "\n".join(lines)

    candidates = [",", ";", "\t", "|"]
    best = None

    for sep in candidates:
        try:
            df = pd.read_csv(
                io.StringIO(text_data),
                sep=sep,
                engine="python",
                header=None,          # <- clave
                on_bad_lines="error"  # si hay líneas malas, explota aquí
            )
            # Guardamos el que tenga más columnas (suele ser el correcto)
            if best is None or df.shape[1] > best.shape[1]:
                best = df
        except Exception:
            pass

    if best is None:
        # Último intento: separadores mixtos (coma/; /tab) como regex
        df = pd.read_csv(
            io.StringIO(text_data),
            sep=r"[;,|\t]+",
            engine="python",
            header=None
        )
        return df

    return best

def get_engine():
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    pwd = os.getenv("MYSQL_PASSWORD", "")
    db = os.getenv("MYSQL_DB", "")
    if not db:
        raise ValueError("MYSQL_DB no está definido en .env")
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)

def to_numeric_matrix(df: pd.DataFrame) -> np.ndarray:
    # Si viene con índice/labels en primera columna, intenta descartarla si no es numérica
    df2 = df.copy()
    # convierto todo a numérico donde se pueda
    for c in df2.columns:
        df2[c] = pd.to_numeric(df2[c], errors="coerce")
    # si hay una primera columna totalmente NaN (típico de labels), la tiro
    if df2.shape[1] >= 2 and df2.iloc[:, 0].isna().all():
        df2 = df2.iloc[:, 1:]
    if df2.isna().any().any():
        # si hay NaNs, es porque había texto/valores no convertibles
        bad_cells = int(df2.isna().sum().sum())
        raise ValueError(f"Hay {bad_cells} celdas no numéricas o vacías tras conversión. Revisa el CSV.")
    return df2.to_numpy(dtype=float)

# -----------------------
# Layout
# -----------------------
left, right = st.columns([1, 1], gap="large")

with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 📥 Carga de archivos")
    file_a = st.file_uploader("Matriz A (CSV) — input-output / tecnológica", type=["csv"])
    file_d = st.file_uploader("Matriz D (CSV) — demanda (n×1 o n×k)", type=["csv"])

    st.markdown('<div class="pill mono">Regla 1: A debe ser n×n</div>', unsafe_allow_html=True)
    st.markdown('<div class="pill mono">Regla 2: D debe tener n filas</div>', unsafe_allow_html=True)

    st.markdown("---")
    do_insert = st.toggle("Insertar resultado en MySQL", value=True)
    st.markdown('<div class="muted">La tabla se crea automáticamente (si no existe) usando el nombre del archivo A como código de producto.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### ⚙️ Validación, cálculo y carga")

    if file_a and file_d:
        try:
            df_a_raw = read_csv_matrix(file_a)
            df_d_raw = read_csv_matrix(file_d)

            A = to_numeric_matrix(df_a_raw)
            D = to_numeric_matrix(df_d_raw)

            n, m = A.shape
            if n != m:
                st.markdown(f'<div class="bad">✗ A no es cuadrada: {n}×{m}</div>', unsafe_allow_html=True)
                st.stop()

            if D.shape[0] != n:
                st.markdown(
                    f'<div class="bad">✗ D no tiene las mismas filas que A: D es {D.shape[0]}×{D.shape[1]} y A es {n}×{n}</div>',
                    unsafe_allow_html=True
                )
                st.stop()

            st.markdown(f'<div class="ok">✓ A es cuadrada: {n}×{n}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="ok">✓ D compatible: {D.shape[0]}×{D.shape[1]}</div>', unsafe_allow_html=True)

            I = np.eye(n, dtype=float)
            M = I - A

            # chequeo de invertibilidad (condición numérica)
            cond = np.linalg.cond(M)
            st.markdown(f'<div class="muted mono">cond(I−A) = {cond:.3e}</div>', unsafe_allow_html=True)

            try:
                X = np.linalg.solve(M, D)  # mejor que invertir explícitamente
            except np.linalg.LinAlgError:
                st.markdown('<div class="bad">✗ (I−A) no es invertible (matriz singular). No puedo calcular X.</div>', unsafe_allow_html=True)
                st.stop()

            # dataframe de salida
            if D.shape[1] == 1:
                out = pd.DataFrame({"i": np.arange(1, n+1), "x": X[:, 0]})
            else:
                cols = {"i": np.arange(1, n+1)}
                for j in range(D.shape[1]):
                    cols[f"x_{j+1}"] = X[:, j]
                out = pd.DataFrame(cols)

            st.markdown("#### 📤 Resultado X")
            st.dataframe(out, use_container_width=True, height=340)

            # Inserción MySQL
            if do_insert:
                table_name = sanitize_table_name(file_a.name)

                engine = get_engine()
                with engine.begin() as conn:
                    # Creamos tabla (por producto) según forma de X
                    if D.shape[1] == 1:
                        conn.execute(text(f"""
                            CREATE TABLE IF NOT EXISTS `{table_name}` (
                              id BIGINT AUTO_INCREMENT PRIMARY KEY,
                              i INT NOT NULL,
                              x DOUBLE NOT NULL,
                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """))
                        # inserta
                        conn.execute(text(f"TRUNCATE TABLE `{table_name}`;"))
                        conn.execute(
                            text(f"INSERT INTO `{table_name}` (i, x) VALUES (:i, :x)"),
                            [{"i": int(row["i"]), "x": float(row["x"])} for _, row in out.iterrows()]
                        )
                    else:
                        # tabla ancha: x_1..x_k
                        x_cols = [c for c in out.columns if c.startswith("x_")]
                        cols_sql = ",\n".join([f"`{c}` DOUBLE NOT NULL" for c in x_cols])
                        conn.execute(text(f"""
                            CREATE TABLE IF NOT EXISTS `{table_name}` (
                              id BIGINT AUTO_INCREMENT PRIMARY KEY,
                              i INT NOT NULL,
                              {cols_sql},
                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """))
                        conn.execute(text(f"TRUNCATE TABLE `{table_name}`;"))
                        # bulk insert
                        rows = []
                        for _, row in out.iterrows():
                            d = {"i": int(row["i"])}
                            for c in x_cols:
                                d[c] = float(row[c])
                            rows.append(d)
                        cols_list = ["i"] + x_cols
                        placeholders = ", ".join([f":{c}" for c in cols_list])
                        conn.execute(
                            text(f"INSERT INTO `{table_name}` ({', '.join(cols_list)}) VALUES ({placeholders})"),
                            rows
                        )

                st.markdown(f'<div class="ok">✓ Insertado en MySQL en la tabla <span class="mono">`{table_name}`</span></div>', unsafe_allow_html=True)

        except Exception as e:
            st.markdown(f'<div class="bad">Error: {str(e)}</div>', unsafe_allow_html=True)

    else:
        st.markdown('<div class="muted">Sube ambos CSV para empezar.</div>', unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

# Footer
st.markdown('<div class="muted" style="margin-top:14px;">Tip: si tus CSV llevan etiquetas en la primera columna y no son numéricas, conviértelas a un CSV puramente numérico o asegúrate de que esa columna no se lea como parte de la matriz.</div>', unsafe_allow_html=True)

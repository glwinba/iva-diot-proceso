# ============================================================================
# src/db_pendientes.py - PROYECTO_DIOT
# Persistencia de pedimentos pendientes (excluidos) en SQL Server
# Almacena TODAS las filas del R2 excluido con columnas individuales.
# ============================================================================

import os
import logging
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Detectar driver: pyodbc (rápido) o pymssql (fallback)
# ---------------------------------------------------------------------------
_USE_PYODBC = False
_ODBC_DRIVER = None

try:
    import pyodbc
    for _drv in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
        if _drv in pyodbc.drivers():
            _ODBC_DRIVER = _drv
            _USE_PYODBC = True
            break
except ImportError:
    pyodbc = None

try:
    import pymssql
except ImportError:
    pymssql = None

SCHEMA = os.getenv("DB_SCHEMA", "dbo")
TABLE_NAME = "PedimentosPendientes"

# Columnas R2 → tipo SQL
R2_COLUMNS = {
    'TaxID Proveedor':              'NVARCHAR(50)',
    'Código Proveedor':             'NVARCHAR(100)',
    'Nombre del Proveedor':         'NVARCHAR(200)',
    'País Vendedor':                'NVARCHAR(10)',
    'Nacionalidad':                 'NVARCHAR(100)',
    'Base Gravable MXN':            'DECIMAL(18,4)',
    'Cálculo IVA Exceptuado':       'DECIMAL(18,4)',
    'IVA al 16% MXN':               'DECIMAL(18,4)',
    'Prevalidación MXN':            'DECIMAL(18,4)',
    'IVA Prevalidación MXN':        'DECIMAL(18,4)',
    'Año':                          'INT',
    'Aduana':                       'NVARCHAR(10)',
    'Patente':                      'NVARCHAR(10)',
    'Pedimento':                    'NVARCHAR(20)',
    'Fecha de Pago (Data Stage)':   'NVARCHAR(30)',
    'Clave del Pedimento':          'NVARCHAR(10)',
    'Fecha Recepción Pedimento':    'NVARCHAR(30)',
    'Fecha Real Pago (Bancario)':   'NVARCHAR(200)',
    'Clave Forma de Pago':          'NVARCHAR(100)',
    'TIPO CAMBIO MXP':              'DECIMAL(18,6)',
    'Valor Aduana DLLS':            'DECIMAL(18,4)',
    'Valor Comercial DLLS':         'DECIMAL(18,4)',
    'Total Pagado Impuestos MXP':   'DECIMAL(18,4)',
    'Total Pagado Impuestos DLLS':  'DECIMAL(18,4)',
    'Tipo de Material':             'NVARCHAR(100)',
    'Tipo Operación':               'NVARCHAR(10)',
    'NOTAS':                        'NVARCHAR(500)',
}

# Nombres SQL-safe (sin espacios ni acentos) para cada columna R2
def _sql_col(name: str) -> str:
    return f"[{name}]"


def get_connection():
    """Crea conexión a SQL Server."""
    server   = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([server, database, user, password]):
        raise ConnectionError("Credenciales de BD no configuradas en .env")

    if _USE_PYODBC:
        conn_str = (
            f"DRIVER={{{_ODBC_DRIVER}}};"
            f"SERVER={server},1433;"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=60;"
        )
        return pyodbc.connect(conn_str, timeout=300)
    elif pymssql:
        return pymssql.connect(
            server=server, user=user, password=password,
            database=database, port=1433,
            tds_version='7.4', timeout=300, login_timeout=60
        )
    else:
        raise ImportError("Ni pyodbc ni pymssql están disponibles")


# ---------------------------------------------------------------------------
# 1. Crear/migrar tabla
# ---------------------------------------------------------------------------
def crear_tabla_si_no_existe():
    """Crea la tabla PedimentosPendientes v3 (columnas individuales)."""
    conn = get_connection()
    cursor = conn.cursor()

    # Verificar si la tabla existe
    cursor.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{TABLE_NAME}'
    """)
    exists = cursor.fetchone()[0] > 0

    if exists:
        # Verificar si es v2 (tiene RowData) o v1 → recrear
        cursor.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{TABLE_NAME}'
            AND COLUMN_NAME = 'RowData'
        """)
        has_rowdata = cursor.fetchone()[0] > 0

        # También revisar si es v1 (sin RowData y sin columnas R2)
        cursor.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{TABLE_NAME}'
            AND COLUMN_NAME = 'Nacionalidad'
        """)
        has_r2_cols = cursor.fetchone()[0] > 0

        if has_rowdata or not has_r2_cols:
            logger.info(f"   🔄 Migrando tabla {TABLE_NAME} a v3 (columnas individuales)...")
            cursor.execute(f"DROP TABLE {SCHEMA}.[{TABLE_NAME}]")
            conn.commit()
            exists = False

    if not exists:
        # Construir columnas R2
        r2_cols_ddl = ",\n            ".join(
            f"{_sql_col(name)} {sql_type} NULL" for name, sql_type in R2_COLUMNS.items()
        )

        ddl = f"""
        CREATE TABLE {SCHEMA}.[{TABLE_NAME}] (
            Id              INT IDENTITY(1,1) PRIMARY KEY,
            MesOrigen       NVARCHAR(6) NOT NULL,
            {r2_cols_ddl},
            Utilizado       BIT DEFAULT 0,
            MesUtilizado    NVARCHAR(6) NULL,
            FechaInsercion  DATETIME DEFAULT GETDATE(),
            FechaUtilizado  DATETIME NULL
        );
        """
        cursor.execute(ddl)
        conn.commit()
        logger.info(f"✅ Tabla {SCHEMA}.{TABLE_NAME} v3 creada ({len(R2_COLUMNS)} columnas R2 + 5 de control)")
    else:
        logger.info(f"✅ Tabla {SCHEMA}.{TABLE_NAME} v3 verificada")

    conn.close()


# ---------------------------------------------------------------------------
# 2. Insertar pendientes (todas las filas, sin duplicados)
# ---------------------------------------------------------------------------
def _clean_val(val):
    """Limpia un valor para INSERT."""
    if val is None:
        return None
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        return None
    s = str(val)
    if s in ('nan', 'None', 'NaT', ''):
        return None
    return s.strip()


def insertar_pendientes(df_excluido: pd.DataFrame, mes_origen: str):
    """
    Inserta TODAS las filas excluidas en BD.
    Evita duplicados: si ya existen filas para un pedimento en este mes, no las reinserta.
    """
    if df_excluido.empty:
        logger.info("   Sin pendientes nuevos para insertar")
        return 0

    conn = None
    inserted = 0
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Obtener pedimentos ya insertados para este mes (Patente|Pedimento|Aduana)
        cursor.execute(f"""
            SELECT DISTINCT [Patente] + '|' + [Pedimento] + '|' + [Aduana]
            FROM {SCHEMA}.[{TABLE_NAME}]
            WHERE MesOrigen = ?
        """, [mes_origen])
        ya_insertados = set(row[0] for row in cursor.fetchall())

        # Columnas R2 que existen en el DataFrame
        cols_presentes = [c for c in R2_COLUMNS.keys() if c in df_excluido.columns]
        cols_sql = ", ".join([f"[MesOrigen]"] + [_sql_col(c) for c in cols_presentes])
        placeholders = ", ".join(["?"] * (1 + len(cols_presentes)))
        insert_sql = f"INSERT INTO {SCHEMA}.[{TABLE_NAME}] ({cols_sql}) VALUES ({placeholders})"

        for _, row in df_excluido.iterrows():
            patente = str(row.get('Patente', '')).strip()
            try:
                pedimento = str(int(float(row.get('Pedimento', 0))))
            except (ValueError, TypeError):
                pedimento = str(row.get('Pedimento', '')).strip()
            aduana = str(row.get('Aduana', '')).strip()

            key = f"{patente}|{pedimento}|{aduana}"
            if key in ya_insertados:
                continue

            values = [mes_origen] + [_clean_val(row.get(c)) for c in cols_presentes]
            cursor.execute(insert_sql, values)
            inserted += 1

        conn.commit()
        logger.info(f"📤 Pendientes: {inserted} filas insertadas de {len(df_excluido)} (mes {mes_origen})")

    except Exception as e:
        logger.error(f"❌ Error insertando pendientes: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass
    return inserted


# ---------------------------------------------------------------------------
# 3. Obtener pendientes anteriores (filas completas)
# ---------------------------------------------------------------------------
def obtener_pendientes_anteriores() -> pd.DataFrame:
    """Lee pendientes WHERE Utilizado=0 como DataFrame con columnas R2."""
    cols_sql = ", ".join([f"[MesOrigen]"] + [_sql_col(c) for c in R2_COLUMNS.keys()])
    sql = f"""
        SELECT {cols_sql}
        FROM {SCHEMA}.[{TABLE_NAME}]
        WHERE Utilizado = 0
        ORDER BY MesOrigen, Id
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        conn.close()

        if not rows:
            logger.info("📥 Pendientes anteriores: 0 registros en BD")
            return pd.DataFrame()

        df = pd.DataFrame([list(r) for r in rows], columns=cols)
        meses = df['MesOrigen'].nunique()
        logger.info(f"📥 Pendientes anteriores: {len(df)} filas de {meses} mes(es)")
        return df

    except Exception as e:
        logger.error(f"❌ Error leyendo pendientes: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


# ---------------------------------------------------------------------------
# 4. Marcar pedimentos como utilizados (todas sus filas)
# ---------------------------------------------------------------------------
def marcar_utilizados(keys: list, mes_utilizado: str):
    """
    UPDATE Utilizado=1 para TODAS las filas de los pedimentos resueltos.
    keys: lista de tuplas (patente, pedimento, aduana)
    """
    if not keys:
        return

    conn = None
    total = 0
    try:
        conn = get_connection()
        cursor = conn.cursor()

        for patente, pedimento, aduana in keys:
            cursor.execute(f"""
                UPDATE {SCHEMA}.[{TABLE_NAME}]
                SET Utilizado = 1,
                    MesUtilizado = ?,
                    FechaUtilizado = GETDATE()
                WHERE [Patente] = ? AND [Pedimento] = ? AND [Aduana] = ?
                  AND Utilizado = 0
            """, [mes_utilizado, patente, pedimento, aduana])
            total += cursor.rowcount

        conn.commit()
        logger.info(f"✅ {total} filas marcadas como utilizadas ({len(keys)} pedimentos, mes {mes_utilizado})")

    except Exception as e:
        logger.error(f"❌ Error marcando utilizados: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

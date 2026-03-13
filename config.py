# ============================================================================
# config.py - PROYECTO_DIOT (Reportes Ford)
# ============================================================================

from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Archivos .asc que necesitamos de la glosa
ASC_FILES = {
    '501': 'DatosGenerales',
    '505': 'Proveedores',
    '510': 'ContribucionesPedimento',
    '551': 'Partidas',
    '552': 'Mercancias',
    '557': 'ContribucionesPartida',
    'Sel': 'Seleccion',
    '701': 'RectificacionesHeader',
    '702': 'RectificacionesDetalle',
}

# Archivos Excel que nos comparten mensualmente
# Se identifican por keyword en el nombre del archivo
# Opciones: sheet_name (str), header_row (int 0-indexed)
EXCEL_SOURCES = {
    'AccPolicyReport': {'name': 'AccPolicyReport'},
    'Poliza contable': {'name': 'PolizaContable', 'sheet_name': 'IMP', 'header_row': 5, 'dtype_str': True, 'password': 'r2719ra1'},
    'Shippers': {'name': 'Shippers'},
    'SupplierReport': {'name': 'SupplierReport'},
    'VehicleReport': {'name': 'VehicleReport'},
    'InvoiceReport': {'name': 'InvoiceReport'},
}

# Archivos de catálogo (viven en catalogos/, no cambian cada mes)
CATALOG_FILES = {
    'FechaDOF': {'name': 'FechaDOF'},
}

# Nombres de las pestañas del Excel de salida
REPORT_SHEETS = {
    'R1_IMP': '1_Pedimentos_IMP',
    'R1_EXP': '1_Pedimentos_EXP',
    'R2': '2_IVA_Detalle',
    'R2_PEND': '2B_IVA_Pendientes',
    'R3': '3_IVA_Consolidado',
    'R4': '4_Facturas',
    'R4B_EXP': '4b_Facturas Exportación',
    'R5': '5_DatosGenerales',
    'R6': '6_Partidas',
    'R7': '7_Mercancias',
}

CONFIG = {
    "input_dir": Path("input"),
    "output_dir": Path("output"),
    "processed_dir": Path("processed"),
    "logs_dir": Path("logs"),
    "catalogos_dir": Path("catalogos"),
    "delimiter": "|",
    "encoding": "latin-1",

    # BD - activa solo en producción
    "insert_to_db": os.getenv("ENVIRONMENT", "desarrollo").lower() == "produccion",
    "db_server": os.getenv("DB_SERVER"),
    "db_database": os.getenv("DB_DATABASE"),
    "db_user": os.getenv("DB_USER"),
    "db_password": os.getenv("DB_PASSWORD"),
    "db_schema": os.getenv("DB_SCHEMA", "dbo"),
}

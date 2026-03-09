# ============================================================================
# main.py - PROYECTO_DIOT (Generador de Reportes Ford)
# ============================================================================
# Uso:
#   python main.py                          → busca carpeta en input/
#   python main.py --input /ruta/a/carpeta  → carpeta específica con .asc
# ============================================================================

import argparse
import sys
from pathlib import Path
from datetime import datetime

from config import CONFIG, ASC_FILES, EXCEL_SOURCES, REPORT_SHEETS
from src.logger_config import setup_logger
from src.asc_parser import AscParser
from src.excel_writer import write_excel
from src.reportes.reporte_1_pedimentos import Reporte1Pedimentos
from src.reportes.reporte_2_iva_proveedor import Reporte2IvaProveedor
from src.reportes.reporte_3_iva_consolidado import Reporte3IvaConsolidado
from src.reportes.reporte_4_facturas import Reporte4Facturas


def find_input_folder(input_dir: Path) -> Path:
    """
    Busca la carpeta con archivos .asc o .zip dentro de input_dir.
    Puede estar directamente o dentro de una subcarpeta.
    """
    # Buscar .asc directamente
    if list(input_dir.glob('*.asc')) or list(input_dir.glob('*.zip')):
        return input_dir

    # Buscar en subcarpetas
    for subdir in sorted(input_dir.iterdir()):
        if subdir.is_dir() and (list(subdir.glob('*.asc')) or list(subdir.glob('*.zip'))):
            return subdir

    return None


def main():
    # --- Argumentos ---
    parser = argparse.ArgumentParser(description='Generador de Reportes DIOT Ford')
    parser.add_argument('--input', type=str, help='Carpeta con archivos .asc')
    args = parser.parse_args()

    # --- Logger ---
    CONFIG['logs_dir'].mkdir(parents=True, exist_ok=True)
    setup_logger(CONFIG['logs_dir'])

    import logging
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("🚀 PROYECTO DIOT - Generador de Reportes Ford")
    logger.info("=" * 60)

    # --- Inicializar tabla de pendientes en BD ---
    try:
        from src.db_pendientes import crear_tabla_si_no_existe
        crear_tabla_si_no_existe()
    except Exception as e:
        logger.warning(f"⚠️  No se pudo inicializar tabla de pendientes: {e}")
        logger.warning("   El proceso continuará sin persistencia de pendientes")

    # --- Localizar carpeta de entrada ---
    if args.input:
        asc_folder = Path(args.input)
    else:
        input_dir = CONFIG['input_dir']
        if not input_dir.exists():
            input_dir.mkdir(parents=True, exist_ok=True)
            logger.error(f"❌ Carpeta input/ vacía. Coloca los archivos .asc ahí.")
            sys.exit(1)

        asc_folder = find_input_folder(input_dir)
        if not asc_folder:
            logger.error(f"❌ No se encontraron archivos .asc en {input_dir}")
            sys.exit(1)

    logger.info(f"📂 Carpeta de entrada: {asc_folder}")

    # --- Cargar fuentes .asc ---
    asc_parser = AscParser(
        delimiter=CONFIG['delimiter'],
        encoding=CONFIG['encoding']
    )
    sources = asc_parser.load_all_sources(asc_folder, ASC_FILES)

    if not sources:
        logger.error("❌ No se cargaron fuentes. Verifica los archivos .asc")
        sys.exit(1)

    # --- Cargar fuentes Excel (AccPolicyReport, etc.) ---
    excel_sources = asc_parser.load_excel_sources(asc_folder, EXCEL_SOURCES)
    sources.update(excel_sources)

    # --- Actualizar catálogo FechaDOF desde DOF (dof.gob.mx) ---
    try:
        from src.dof_scraper import actualizar_fecha_dof
        actualizar_fecha_dof(CONFIG['catalogos_dir'])
    except Exception as e:
        logger.warning(f"⚠️  No se pudo actualizar FechaDOF: {e}")
        logger.warning("   Se usará el catálogo existente")

    # --- Cargar catálogos (FechaDOF, etc. — no cambian mensualmente) ---
    from config import CATALOG_FILES
    catalog_sources = asc_parser.load_excel_sources(CONFIG['catalogos_dir'], CATALOG_FILES)
    sources.update(catalog_sources)

    # --- Generar reportes ---
    reports = {}

    # Reporte 4: Relación de Facturas IMPO/EXPO (se genera primero para R1)
    r4 = Reporte4Facturas()
    df_r4 = r4.generar(sources)
    if not df_r4.empty:
        reports[REPORT_SHEETS['R4']] = df_r4
        
        # Reporte 4b: Facturas de Exportación (filtrado de R4)
        df_r4_expo = df_r4[df_r4['Tipo Operación'] == 'Exportación'].copy()
        if not df_r4_expo.empty:
            reports[REPORT_SHEETS['R4B_EXP']] = df_r4_expo

    # Reporte 1: Pedimentos Mensuales (IMP + EXP) — recibe R4 para Venta/NoVenta
    r1 = Reporte1Pedimentos()
    r1_result = r1.generar(sources, df_r4=df_r4)
    if isinstance(r1_result, dict):
        for tipo, df_tipo in r1_result.items():
            if not df_tipo.empty:
                sheet_key = f'R1_{tipo}'
                if sheet_key in REPORT_SHEETS:
                    reports[REPORT_SHEETS[sheet_key]] = df_tipo

    # Extraer mes_proceso de los datos (YYYYMM)
    mes_proceso = ''
    if 'DatosGenerales' in sources:
        try:
            fecha = pd.to_datetime(
                sources['DatosGenerales']['FechaPagoReal'].iloc[0],
                format='mixed', errors='coerce'
            )
            if pd.notna(fecha):
                mes_proceso = fecha.strftime('%Y%m')
                logger.info(f"📅 Mes de proceso: {mes_proceso}")
        except:
            pass

    # Reporte 2: IVA por Proveedor con Detalle
    r2 = Reporte2IvaProveedor()
    r2_result = r2.generar(sources, mes_proceso=mes_proceso)
    df_r2 = pd.DataFrame()
    df_r2_pend = pd.DataFrame()
    if isinstance(r2_result, tuple):
        df_r2, df_r2_pend = r2_result
    elif isinstance(r2_result, pd.DataFrame):
        df_r2 = r2_result
    if not df_r2.empty:
        reports[REPORT_SHEETS['R2']] = df_r2
    if not df_r2_pend.empty:
        reports[REPORT_SHEETS['R2_PEND']] = df_r2_pend

    # Reporte 3: IVA Consolidado por Proveedor (GROUP BY TaxID de R2)
    if not df_r2.empty:
        r3 = Reporte3IvaConsolidado()
        df_r3 = r3.generar(df_r2)
        if not df_r3.empty:
            reports[REPORT_SHEETS['R3']] = df_r3

    # Reportes 5, 6, 7: DataStage directo (501, 551, 552)
    if 'DatosGenerales' in sources:
        reports[REPORT_SHEETS['R5']] = sources['DatosGenerales']
    if 'Partidas' in sources:
        reports[REPORT_SHEETS['R6']] = sources['Partidas']
    if 'Mercancias' in sources:
        reports[REPORT_SHEETS['R7']] = sources['Mercancias']

    # --- Escribir Excel ---
    if reports:
        CONFIG['output_dir'].mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Intentar extraer período de los datos
        periodo = ""
        if 'DatosGenerales' in sources:
            try:
                fecha = pd.to_datetime(
                    sources['DatosGenerales']['FechaPagoReal'].iloc[0],
                    format='mixed', errors='coerce'
                )
                if pd.notna(fecha):
                    periodo = f"_{fecha.strftime('%Y%m')}"
            except:
                pass

        output_file = CONFIG['output_dir'] / f"Reporte_DIOT{periodo}_{timestamp}.xlsx"

        # Orden de pestañas
        sheet_order = list(REPORT_SHEETS.values())
        write_excel(reports, output_file, sheet_order)

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ PROCESO COMPLETADO")
        logger.info(f"   Reportes generados: {len(reports)}")
        logger.info(f"   Archivo: {output_file}")
        logger.info(f"{'='*60}")
    else:
        logger.error("❌ No se generaron reportes")
        sys.exit(1)


if __name__ == '__main__':
    import pandas as pd
    main()

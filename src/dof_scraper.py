# ============================================================================
# src/dof_scraper.py - PROYECTO_DIOT
# Scraper del tipo de cambio DOF (Diario Oficial de la Federación)
# Actualiza automáticamente catalogos/FechaDOF.xlsx desde dof.gob.mx
# ============================================================================

import logging
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DOF_URL = 'https://dof.gob.mx/indicadores_detalle.php'
COD_DOLAR = '158'  # Código del indicador DOLAR en DOF
FECHA_INICIO = '01/07/2025'  # Fecha fija de inicio


def actualizar_fecha_dof(catalogos_dir: Path) -> bool:
    """
    Descarga el tipo de cambio FIX del DOF desde julio 2025 hasta hoy
    y actualiza catalogos/FechaDOF.xlsx.

    Returns:
        True si se actualizó correctamente, False si falló.
    """
    hoy = datetime.now().strftime('%d/%m/%Y')
    url = f"{DOF_URL}?cod_tipo_indicador={COD_DOLAR}&dfecha={FECHA_INICIO}&hfecha={hoy}"

    logger.info(f"   🌐 Consultando DOF: {FECHA_INICIO} → {hoy}...")

    try:
        warnings.filterwarnings('ignore', message='Unverified HTTPS request')
        r = requests.get(url, timeout=20, verify=False)
        r.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"   ⚠️ No se pudo conectar al DOF: {e}")
        return False

    try:
        soup = BeautifulSoup(r.text, 'html.parser')

        # Buscar la tabla con columnas Fecha|Valor
        df_dof = None
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if len(rows) < 5:
                continue

            # Verificar si la primera fila tiene headers Fecha|Valor
            first_cells = [td.text.strip() for td in rows[0].find_all(['td', 'th'])]
            if 'Fecha' in first_cells and 'Valor' in first_cells:
                data = []
                for row in rows[1:]:
                    cells = [td.text.strip() for td in row.find_all('td')]
                    if len(cells) == 2 and cells[0] and cells[1]:
                        try:
                            fecha = pd.to_datetime(cells[0], format='%d-%m-%Y')
                            valor = float(cells[1])
                            data.append({'FECHA': fecha, 'VALOR': valor})
                        except (ValueError, TypeError):
                            continue

                if data:
                    df_dof = pd.DataFrame(data)
                    break

        if df_dof is None or df_dof.empty:
            logger.warning("   ⚠️ No se encontraron datos de tipo de cambio en DOF")
            return False

        # Ordenar por fecha
        df_dof = df_dof.sort_values('FECHA').reset_index(drop=True)

        # Guardar en catalogos/
        output_path = catalogos_dir / 'FechaDOF.xlsx'
        df_dof.to_excel(output_path, index=False)

        logger.info(f"   ✅ FechaDOF actualizado: {len(df_dof)} fechas "
                    f"({df_dof['FECHA'].min().strftime('%Y-%m-%d')} → "
                    f"{df_dof['FECHA'].max().strftime('%Y-%m-%d')})")
        return True

    except Exception as e:
        logger.warning(f"   ⚠️ Error procesando datos del DOF: {e}")
        return False

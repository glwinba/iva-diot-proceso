# ============================================================================
# src/reportes/reporte_3_iva_consolidado.py - PROYECTO_DIOT
# Reporte 3: IVA Consolidado por Proveedor
# Agrupación (GROUP BY TaxID) del Reporte 2
# ============================================================================

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class Reporte3IvaConsolidado:
    """
    Reporte 3: IVA Consolidado por Proveedor.
    Agrupa el Reporte 2 por TaxID Proveedor, aplicando:
    - FIRST para campos de texto (Nombre, País, etc.)
    - SUM para montos (Base Gravable, IVA, Impuestos, etc.)
    - COUNT para Pedimento
    - DISTINCT para Aduana, Patente, Clave Pedimento, Tipo Material
    - MIN-MAX para fechas
    - AVG para tipo de cambio
    """

    nombre = "R3_IVA_Consolidado"
    fuentes_requeridas = []  # No necesita fuentes directas, usa R2

    def generar(self, df_r2: pd.DataFrame) -> pd.DataFrame:
        """
        Genera R3 a partir del DataFrame de R2.

        Args:
            df_r2: DataFrame del Reporte 2 ya generado.

        Returns:
            DataFrame consolidado por TaxID Proveedor.
        """
        if df_r2.empty:
            logger.warning("⚠️ R2 vacío, no se puede generar R3")
            return pd.DataFrame()

        logger.info("🔧 Generando Reporte 3: IVA Consolidado por Proveedor...")
        logger.info(f"   R2 entrada: {len(df_r2):,} registros")

        # Excluir filas de resumen (Data Stage, Diferencia, etc.) que no tienen TaxID
        if 'TaxID Proveedor' in df_r2.columns:
            df_r2 = df_r2.dropna(subset=['TaxID Proveedor']).copy()
            df_r2 = df_r2[df_r2['TaxID Proveedor'].astype(str).str.strip() != '']
            logger.info(f"   R2 tras filtrar resumen: {len(df_r2):,} registros de datos")

        # Asegurar tipos numéricos
        num_cols = [
            'Base Gravable MXN', 'Cálculo IVA Exceptuado', 'IVA al 16% MXN',
            'Prevalidación MXN', 'IVA Prevalidación MXN',
            'TIPO CAMBIO MXP', 'Valor Aduana DLLS', 'Valor Comercial DLLS',
            'Total Pagado Impuestos MXP', 'Total Pagado Impuestos DLLS'
        ]
        for col in num_cols:
            if col in df_r2.columns:
                df_r2[col] = pd.to_numeric(df_r2[col], errors='coerce').fillna(0)

        # Parsear fechas
        for col in ['Fecha de Pago (Data Stage)', 'Fecha Recepción Pedimento', 'Fecha Real Pago (Bancario)']:
            if col in df_r2.columns:
                df_r2[col] = pd.to_datetime(df_r2[col], format='mixed', errors='coerce')

        # GROUP BY TaxID Proveedor
        grouped = df_r2.groupby('TaxID Proveedor', sort=False)

        result = pd.DataFrame()

        # 1. TaxID Proveedor (key)
        result['TaxID Proveedor'] = grouped['TaxID Proveedor'].first().values

        # 2. Código Proveedor — FIRST
        result['Código Proveedor'] = grouped['Código Proveedor'].first().values

        # 3. Nombre del Proveedor — FIRST
        result['Nombre del Proveedor'] = grouped['Nombre del Proveedor'].first().values

        # 4. País Vendedor — FIRST (not empty)
        result['País Vendedor'] = grouped['País Vendedor'].apply(
            lambda x: next((v for v in x if str(v).strip()), '')
        ).values

        # 5. Nacionalidad — FIRST (not empty)
        result['Nacionalidad'] = grouped['Nacionalidad'].apply(
            lambda x: next((v for v in x if str(v).strip()), '')
        ).values

        # 6-10. SUM de montos
        result['Base Gravable MXN'] = grouped['Base Gravable MXN'].sum().values
        result['Cálculo IVA Exceptuado'] = grouped['Cálculo IVA Exceptuado'].sum().values
        result['IVA al 16% MXN'] = grouped['IVA al 16% MXN'].sum().values
        result['Prevalidación MXN'] = grouped['Prevalidación MXN'].sum().values
        result['IVA Prevalidación MXN'] = grouped['IVA Prevalidación MXN'].sum().values

        # 11. Año — FIRST
        result['Año'] = grouped['Año'].first().values

        # 12. Aduana — DISTINCT
        result['Aduana'] = grouped['Aduana'].apply(
            lambda x: ', '.join(sorted(set(str(v) for v in x if str(v).strip())))
        ).values

        # 13. Patente — DISTINCT
        result['Patente'] = grouped['Patente'].apply(
            lambda x: ', '.join(sorted(set(str(v) for v in x if str(v).strip())))
        ).values

        # 14. Pedimento — COUNT
        result['Pedimento'] = grouped['Pedimento'].count().values

        # 15. Fecha de Pago — MIN-MAX
        result['Fecha de Pago'] = grouped['Fecha de Pago (Data Stage)'].apply(
            self._min_max_fecha
        ).values

        # 16. Clave del Pedimento — DISTINCT
        result['Clave del Pedimento'] = grouped['Clave del Pedimento'].apply(
            lambda x: ', '.join(sorted(set(str(v) for v in x if str(v).strip())))
        ).values

        # 17. Fecha Recepción Pedimento — MIN-MAX
        result['Fecha Recepción Pedimento'] = grouped['Fecha Recepción Pedimento'].apply(
            self._min_max_fecha
        ).values

        # 18. Fecha Real de Pago — MIN-MAX
        result['Fecha Real de Pago'] = grouped['Fecha Real Pago (Bancario)'].apply(
            self._min_max_fecha
        ).values

        # 19. TIPO CAMBIO — AVG
        result['TIPO CAMBIO'] = grouped['TIPO CAMBIO MXP'].mean().values

        # 20-21. SUM valores DLLS
        result['Valor Aduana DLLS'] = grouped['Valor Aduana DLLS'].sum().values
        result['Valor Comercial DLLS'] = grouped['Valor Comercial DLLS'].sum().values

        # 22-23. SUM total impuestos
        result['Total Pagado Impuestos MXP'] = grouped['Total Pagado Impuestos MXP'].sum().values
        result['Total Pagado Impuestos DLLS'] = grouped['Total Pagado Impuestos DLLS'].sum().values

        # 24. Tipo de Material — DISTINCT
        result['Tipo de Material'] = grouped['Tipo de Material'].apply(
            lambda x: ', '.join(sorted(set(str(v) for v in x if str(v).strip() and str(v) != 'nan')))
        ).values

        # 25. Tipo Operación — DISTINCT (si existe en R2)
        if 'Tipo Operación' in df_r2.columns:
            result['Tipo Operación'] = grouped['Tipo Operación'].apply(
                lambda x: ', '.join(sorted(set(str(v) for v in x if str(v).strip() and str(v) != 'nan')))
            ).values

        logger.info(f"✅ Reporte 3 generado: {len(result):,} proveedores únicos, {len(result.columns)} columnas")
        return result

    @staticmethod
    def _min_max_fecha(series: pd.Series) -> str:
        """Retorna 'YYYY-MM-DD' si una sola fecha, o 'YYYY-MM-DD / YYYY-MM-DD' si rango."""
        valid = series.dropna()
        if valid.empty:
            return ''
        f_min = valid.min()
        f_max = valid.max()
        if f_min == f_max:
            return f_min.strftime('%Y-%m-%d')
        return f"{f_min.strftime('%Y-%m-%d')} / {f_max.strftime('%Y-%m-%d')}"

# ============================================================================
# src/reportes/reporte_1_pedimentos.py - PROYECTO_DIOT
# Reporte 1: Total de Pedimentos Mensuales
# ============================================================================
# Fuentes:
#   .asc: 501 (DatosGenerales) + 505 (Proveedores) + Sel (Seleccion)
#         + 510 (ContribucionesPedimento) + 551 (Partidas) + 701 (Rectificaciones)
#   Excel: AccPolicyReport (IVA PREV, CLAVE TSMX), PolizaContable (Ped Edo Cta, PLANTA)
#
# Campos del reporte:
#   1.  TIPO OPERACION              <- 501.TipoOperacion
#   2.  FECHA PAGO REAL             <- 501.FechaPagoReal (solo fecha, sin hora)
#   3.  DIA                         <- calculado de FechaPagoReal
#   4.  MES                         <- calculado de FechaPagoReal
#   5.  AÑO                         <- calculado de FechaPagoReal
#   6.  SECCION ADUANERA            <- 501.SeccionAduanera
#   7.  PATENTE                     <- 501.Patente
#   8.  PEDIMENTO                   <- 501.Pedimento
#   9.  SECCION ADUANERA ENTRADA    <- 501.SeccionAduaneraEntrada
#  10.  PEDIMENTO COMPLETO          <- AA-SS-PPPP-NNNNNNN (SS = 2 dígitos izq de SecAduanera)
#  11.  Pedimento Edo. Cuenta       <- PolizaContable[IMP]."Pedimentos / Factura"
#  12.  PROVEEDOR                   <- 505 JOIN por Patente+Pedimento+SecAduanera
#  13.  FECHA DESADUANAMIENTO       <- Sel JOIN
#  14.  TIPO DE CAMBIO PEDIMENTO    <- 501.TipoCambio
#  15.  FECHA DE PUBLICACIÓN D.O.F. <- Pendiente (catálogo)
#  16.  Total Operación MXN         <- 551 Suma ValorComercial
#  17.  Contribuciones pagadas MXN  <- 510 Suma ImportePago
#  18.  IVA PREV MXN               <- AccPolicyReport.prev_0 * 0.16
#  19.  PLANTA                      <- PolizaContable[IMP]."Planta Destino"
#  20.  CLAVE TSMX                  <- AccPolicyReport.pedimento_type || Shippers.cve_doc
#  21.  CLAVE DE PEDIMENTO DS       <- 501.ClaveDocumento
#  22.  Observaciones               <- 701: pedimento original si fue rectificado
# ============================================================================

import pandas as pd
import logging
from .base_reporte import BaseReporte

logger = logging.getLogger(__name__)


def build_pedimento_key(df, col_patente='Patente', col_pedimento='Pedimento',
                        col_seccion='SeccionAduanera'):
    """
    Construye llave Patente|Pedimento|SeccionAduanera para cruces.
    Estos 3 campos identifican de forma única un pedimento.
    SeccionAduanera se normaliza como int para quitar ceros (071→71).
    """
    seccion = pd.to_numeric(df[col_seccion], errors='coerce').fillna(0).astype(int).astype(str)
    # Patente y Pedimento: asegurar entero string para evitar "4012.0" vs "4012"
    patente = pd.to_numeric(df[col_patente], errors='coerce').fillna(0).astype(int).astype(str)
    pedimento = pd.to_numeric(df[col_pedimento], errors='coerce').fillna(0).astype(int).astype(str)
    return (
        patente + '|' +
        pedimento + '|' +
        seccion
    )


def build_pedimento_completo(fecha_parsed, seccion_aduanera, patente, pedimento):
    """
    Construye PEDIMENTO COMPLETO: AA-SS-PPPP-NNNNNNN
    SS = primeros 2 dígitos de la izquierda de SeccionAduanera
    """
    anio = fecha_parsed.dt.strftime('%y').fillna('')
    # Solo primeros 2 dígitos de SeccionAduanera (ej: 670 → 67, 071 → 07, 43 → 43)
    seccion_2d = seccion_aduanera.astype(str).str.strip().str.zfill(3).str[:2]
    return (
        anio + '-' +
        seccion_2d + '-' +
        patente.astype(str).str.zfill(4) + '-' +
        pedimento.astype(str).str.zfill(7)
    )


class Reporte1Pedimentos(BaseReporte):
    """Reporte 1: Total de Pedimentos Mensuales."""

    nombre = "R1_Pedimentos"
    fuentes_requeridas = ['DatosGenerales']  # 501 es obligatoria

    def generar(self, sources: dict, df_r4: pd.DataFrame = None) -> dict:
        """
        Genera el Reporte 1. Retorna dict con 2 DataFrames:
        {'IMP': df_importaciones, 'EXP': df_exportaciones}
        """
        if not self.validar_fuentes(sources):
            return {}

        logger.info("🔧 Generando Reporte 1: Total de Pedimentos Mensuales...")

        # === Fuente principal: 501 DatosGenerales ===
        df_501 = sources['DatosGenerales'].copy()
        logger.info(f"   501 DatosGenerales: {len(df_501):,} registros")

        df = pd.DataFrame()
        df['TIPO OPERACION'] = df_501['TipoOperacion'].values

        # FECHA PAGO REAL sin horas (solo YYYY-MM-DD)
        fecha_parsed = pd.to_datetime(df_501['FechaPagoReal'], format='mixed', errors='coerce')
        df['FECHA PAGO REAL'] = fecha_parsed.dt.strftime('%Y-%m-%d')

        # Extraer DIA, MES, AÑO
        df['DIA'] = fecha_parsed.dt.day
        df['MES'] = fecha_parsed.dt.month
        df['AÑO'] = fecha_parsed.dt.year

        df['SECCION ADUANERA'] = df_501['SeccionAduanera'].values
        df['PATENTE'] = df_501['Patente'].values
        df['PEDIMENTO'] = df_501['Pedimento'].values
        df['SECCION ADUANERA ENTRADA'] = df_501['SeccionAduaneraEntrada'].values

        # PEDIMENTO COMPLETO: AA-SS-PPPP-NNNNNNN (SS = 2 dígitos izq de SecAduanera)
        df['PEDIMENTO COMPLETO'] = build_pedimento_completo(
            fecha_parsed, df_501['SeccionAduanera'], df_501['Patente'], df_501['Pedimento']
        )

        # Llave de cruce (Patente|Pedimento|SeccionAduanera)
        df['_key'] = build_pedimento_key(df_501)

        # === PolizaContable → Pedimento Edo. Cuenta ===
        df = self._cruzar_poliza_contable(df, sources)

        # === SIN FILTRAR: pedimentos sin Póliza reciben leyenda ===
        vacios_ped = df['Pedimento Edo. Cuenta'].isna() | (df['Pedimento Edo. Cuenta'].astype(str).str.strip() == '')
        if vacios_ped.any():
            df.loc[vacios_ped, 'Pedimento Edo. Cuenta'] = 'No se encontró en el estado cuenta de este mes'
            logger.info(f"   📋 {vacios_ped.sum()}/{len(df)} pedimentos sin match en Póliza Contable → leyenda asignada")

        # === 505 Proveedores: TODOS por pedimento, concatenados ===
        if 'Proveedores' in sources:
            df_505 = sources['Proveedores'].copy()
            logger.info(f"   505 Proveedores: {len(df_505):,} registros")
            df_505['_key'] = build_pedimento_key(df_505)
            df_505['_prov'] = df_505['ProveedorMercancia'].astype(str).str.strip()
            # Agrupar todos los proveedores únicos por pedimento, separados con " / "
            prov_agg = df_505.groupby('_key')['_prov'].apply(
                lambda x: ' / '.join(sorted(set(v for v in x if v and v != 'nan')))
            ).reset_index()
            prov_agg.columns = ['_key', 'PROVEEDOR']
            df = df.merge(prov_agg, on='_key', how='left')
            df['PROVEEDOR'] = df['PROVEEDOR'].fillna('')
            provs_ok = (df['PROVEEDOR'] != '').sum()
            logger.info(f"   PROVEEDOR (505 concatenado): {provs_ok}/{len(df)} con valor")
        else:
            df['PROVEEDOR'] = ''
            logger.warning("   ⚠️ 505 no disponible, PROVEEDOR vacío")

        # Fallback: si PROVEEDOR aún vacío, buscar en Shippers
        df = self._fallback_proveedor_shippers(df, sources)

        # === FECHA DESADUANAMIENTO = FechaPagoReal del 501 ===
        df['FECHA DESADUANAMIENTO'] = fecha_parsed.dt.strftime('%Y-%m-%d').values
        logger.info(f"   FECHA DESADUANAMIENTO: {(df['FECHA DESADUANAMIENTO'] != '').sum()}/{len(df)} desde FechaPagoReal")

        # === Tipo de cambio ===
        df['TIPO DE CAMBIO PEDIMENTO'] = df_501['TipoCambio'].values

        # === FECHA DE PUBLICACIÓN D.O.F. (catálogo FechaDOF) ===
        df = self._buscar_fecha_dof(df, sources)

        # === 551 Partidas → Total Operación MXN ===
        if 'Partidas' in sources:
            df_551 = sources['Partidas'].copy()
            logger.info(f"   551 Partidas: {len(df_551):,} registros")
            df_551['_key'] = build_pedimento_key(df_551)
            df_551['ValorComercial'] = pd.to_numeric(df_551['ValorComercial'], errors='coerce').fillna(0)
            total_op = df_551.groupby('_key')['ValorComercial'].sum().reset_index()
            total_op.columns = ['_key', 'Total Operación MXN']
            df = df.merge(total_op, on='_key', how='left')
            df['Total Operación MXN'] = df['Total Operación MXN'].fillna(0)
        else:
            df['Total Operación MXN'] = 0
            logger.warning("   ⚠️ 551 no disponible, Total Operación = 0")

        # === 510 Contribuciones → Contribuciones pagadas MXN ===
        # Solo FormaPago='0' (pago efectivo)
        if 'ContribucionesPedimento' in sources:
            df_510 = sources['ContribucionesPedimento'].copy()
            logger.info(f"   510 Contribuciones: {len(df_510):,} registros")
            df_510['_key'] = build_pedimento_key(df_510)
            df_510['ImportePago'] = pd.to_numeric(df_510['ImportePago'], errors='coerce').fillna(0)
            # Filtrar solo FormaPago = 0
            df_510_efectivo = df_510[df_510['FormaPago'].astype(str).str.strip() == '0']
            logger.info(f"   510 filtrado FormaPago=0: {len(df_510_efectivo):,} de {len(df_510):,}")
            contrib = df_510_efectivo.groupby('_key')['ImportePago'].sum().reset_index()
            contrib.columns = ['_key', 'Contribuciones pagadas MXN']
            df = df.merge(contrib, on='_key', how='left')
            df['Contribuciones pagadas MXN'] = df['Contribuciones pagadas MXN'].fillna(0)
        else:
            df['Contribuciones pagadas MXN'] = 0
            logger.warning("   ⚠️ 510 no disponible, Contribuciones = 0")

        # === AccPolicyReport → IVA PREV MXN + CLAVE TSMX ===
        df = self._cruzar_acc_policy(df, sources)

        # === PLANTA (posición 19, después de IVA PREV) ===
        df = self._agregar_planta(df, sources)

        # Reordenar: mover CLAVE TSMX después de PLANTA
        if 'CLAVE TSMX' in df.columns:
            clave_tsmx = df.pop('CLAVE TSMX')
            df.insert(df.columns.get_loc('PLANTA') + 1, 'CLAVE TSMX', clave_tsmx)

        # === CLAVE TSMX: fallback con Shippers si no se encontró en AccPolicy ===
        df = self._fallback_clave_tsmx_shippers(df, sources)

        # === CLAVE DE PEDIMENTO DS ===
        df['CLAVE DE PEDIMENTO DS'] = df_501['ClaveDocumento'].values

        # === 701 Rectificaciones → Observaciones + sustitución ===
        df = self._procesar_rectificaciones_701(df, sources)

        # === Venta/No Venta (desde R4) ===
        df = self._clasificar_venta_noventa(df, df_r4)

        # Limpiar columna auxiliar
        df.drop(columns=['_key'], inplace=True, errors='ignore')

        # === SPLIT: Importaciones (TipoOperacion=1) / Exportaciones (TipoOperacion=2) ===
        df_imp = df[df['TIPO OPERACION'].astype(str).str.strip() == '1'].copy().reset_index(drop=True)
        df_exp = df[df['TIPO OPERACION'].astype(str).str.strip() == '2'].copy().reset_index(drop=True)

        logger.info(f"✅ Reporte 1 generado: IMP={len(df_imp):,}, EXP={len(df_exp):,} ({len(df):,} total)")
        return {'IMP': df_imp, 'EXP': df_exp}

    # ------------------------------------------------------------------
    # Nuevos métodos helper
    # ------------------------------------------------------------------

    def _clasificar_venta_noventa(self, df: pd.DataFrame, df_r4: pd.DataFrame = None) -> pd.DataFrame:
        """
        Agrega columna 'Tipo Factura (Venta/No Venta)' desde R4.
        Para cada pedimento, revisa las facturas de R4 y determina
        si son Venta, No Venta, o ambas.
        """
        if df_r4 is None or df_r4.empty:
            df['Tipo Factura (Venta/No Venta)'] = ''
            logger.info("   Venta/NoVenta: R4 no disponible, columna vacía")
            return df

        # Construir key de pedimento en R4
        # R4.Pedimento tiene formato "seccion-patente-pedimento"
        # Necesitamos hacer match con _key de R1 que es "patente|pedimento|seccion"
        if 'Pedimento' not in df_r4.columns or 'Venta/No Venta' not in df_r4.columns:
            df['Tipo Factura (Venta/No Venta)'] = ''
            logger.warning("   ⚠️ R4 sin columnas necesarias (Pedimento / Venta/No Venta)")
            return df

        df_r4_copy = df_r4[['Pedimento', 'Venta/No Venta']].copy()
        # R4 Pedimento: "AA-SS-PPPP-NNNNNNN" (año-aduana2d-patente-folio)
        # → convertir a key "patente|pedimento|seccion" (seccion = aduana2d * 10)
        parts = df_r4_copy['Pedimento'].astype(str).str.split('-', expand=True)
        if parts.shape[1] >= 4:
            # parts[0]=año, parts[1]=aduana(2d), parts[2]=patente, parts[3]=pedimento
            seccion = (pd.to_numeric(parts[1], errors='coerce').fillna(0).astype(int) * 10).astype(str)
            df_r4_copy['_key'] = (
                parts[2].str.strip() + '|' +
                parts[3].str.strip() + '|' +
                seccion
            )
        elif parts.shape[1] >= 3:
            # Formato legacy 3 partes: seccion-patente-pedimento
            df_r4_copy['_key'] = (
                parts[1].str.strip() + '|' +
                parts[2].str.strip() + '|' +
                parts[0].str.strip()
            )
        else:
            df['Tipo Factura (Venta/No Venta)'] = ''
            logger.warning("   ⚠️ R4 formato Pedimento inesperado")
            return df

        # Filtrar solo filas con clasificación
        r4_clasif = df_r4_copy[df_r4_copy['Venta/No Venta'].astype(str).str.strip() != ''].copy()

        # Agrupar por pedimento: obtener tipos únicos
        if not r4_clasif.empty:
            clasif_agg = r4_clasif.groupby('_key')['Venta/No Venta'].apply(
                lambda x: ' / '.join(sorted(set(v.strip() for v in x if v.strip())))
            ).reset_index()
            clasif_agg.columns = ['_key', 'Tipo Factura (Venta/No Venta)']
            df = df.merge(clasif_agg, on='_key', how='left')
        else:
            df['Tipo Factura (Venta/No Venta)'] = ''

        df['Tipo Factura (Venta/No Venta)'] = df['Tipo Factura (Venta/No Venta)'].fillna('')
        # Rellenar vacíos con "Sin clasificar"
        vacios_vn = df['Tipo Factura (Venta/No Venta)'].astype(str).str.strip() == ''
        df.loc[vacios_vn, 'Tipo Factura (Venta/No Venta)'] = 'Sin clasificar'
        matched = (~vacios_vn).sum()
        logger.info(f"   Venta/NoVenta: {matched}/{len(df)} clasificados, {vacios_vn.sum()} → 'Sin clasificar'")
        return df

    def _fallback_proveedor_shippers(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        Si PROVEEDOR está vacío después del cruce con 505, busca en Shippers.proveedor.
        """
        if 'Shippers' not in sources:
            return df

        vacios = df['PROVEEDOR'].isna() | (df['PROVEEDOR'].astype(str).str.strip() == '')
        if vacios.sum() == 0:
            return df

        df_ship = sources['Shippers'].copy()
        if 'proveedor' not in df_ship.columns:
            logger.warning("   ⚠️ Shippers sin columna 'proveedor'")
            return df

        df_ship['_key'] = build_pedimento_key(
            df_ship, col_patente='patente', col_pedimento='pedimento',
            col_seccion='adua_sec_desp'
        )
        ship_prov = df_ship.drop_duplicates(subset='_key', keep='first')[['_key', 'proveedor']]
        ship_prov.columns = ['_key', '_prov_ship']

        df = df.merge(ship_prov, on='_key', how='left')
        # Rellenar vacíos
        vacios_after = df['PROVEEDOR'].isna() | (df['PROVEEDOR'].astype(str).str.strip() == '')
        df.loc[vacios_after, 'PROVEEDOR'] = df.loc[vacios_after, '_prov_ship']
        filled = vacios.sum() - (df['PROVEEDOR'].isna() | (df['PROVEEDOR'].astype(str).str.strip() == '')).sum()
        df.drop(columns=['_prov_ship'], inplace=True, errors='ignore')

        logger.info(f"   PROVEEDOR fallback Shippers: +{filled} completados")
        return df

    def _buscar_fecha_dof(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        Busca FECHA DE PUBLICACIÓN D.O.F. usando catálogo FechaDOF.xlsx.
        Lógica:
        1. Para cada pedimento, tomar FECHA PAGO REAL
        2. Buscar en el catálogo la fecha inmediatamente ANTERIOR a FECHA PAGO REAL
        3. Comparar VALOR del catálogo con TIPO DE CAMBIO PEDIMENTO
        4. Si coinciden → FECHA del catálogo = DOF
        5. Si NO → 2ª búsqueda: buscar CUALQUIER fecha del catálogo donde VALOR == TipoCambio
           (la más reciente si hay varias)
        """
        if 'FechaDOF' not in sources:
            df['FECHA DE PUBLICACIÓN D.O.F.'] = ''
            logger.warning("   ⚠️ FechaDOF no disponible")
            return df

        df_dof = sources['FechaDOF'].copy()
        df_dof['FECHA'] = pd.to_datetime(df_dof['FECHA'], format='mixed', errors='coerce')
        df_dof['VALOR'] = pd.to_numeric(df_dof['VALOR'], errors='coerce')
        df_dof = df_dof.dropna(subset=['FECHA', 'VALOR']).sort_values('FECHA').reset_index(drop=True)
        logger.info(f"   FechaDOF: {len(df_dof)} fechas, rango {df_dof['FECHA'].min().strftime('%Y-%m-%d')} a {df_dof['FECHA'].max().strftime('%Y-%m-%d')}")

        # Parsear fechas del reporte
        fecha_pago = pd.to_datetime(df['FECHA PAGO REAL'], format='mixed', errors='coerce')
        tipo_cambio = pd.to_numeric(df['TIPO DE CAMBIO PEDIMENTO'], errors='coerce')

        # Catálogo como arrays para búsqueda eficiente
        dof_fechas = df_dof['FECHA'].values  # sorted
        dof_valores = df_dof['VALOR'].values

        resultados = []
        encontrados_1 = 0
        encontrados_2 = 0
        for i in range(len(df)):
            fp = fecha_pago.iloc[i]
            tc = tipo_cambio.iloc[i]

            if pd.isna(fp) or pd.isna(tc):
                resultados.append('')
                continue

            # --- PRIMERA BÚSQUEDA: fecha anterior con match de tipo de cambio ---
            mask = dof_fechas < fp
            found = False
            if mask.any():
                idx = mask.sum() - 1
                dof_fecha = pd.Timestamp(dof_fechas[idx])
                dof_valor = dof_valores[idx]
                if abs(float(tc) - float(dof_valor)) < 0.001:
                    resultados.append(dof_fecha.strftime('%Y-%m-%d'))
                    encontrados_1 += 1
                    found = True

            # --- SEGUNDA BÚSQUEDA: cualquier fecha con el mismo valor ---
            if not found:
                matches_valor = [
                    j for j in range(len(dof_valores))
                    if abs(float(tc) - float(dof_valores[j])) < 0.001
                ]
                if matches_valor:
                    # Tomar la fecha más reciente con match
                    best_j = matches_valor[-1]  # array ya está sorted by fecha
                    dof_fecha = pd.Timestamp(dof_fechas[best_j])
                    resultados.append(dof_fecha.strftime('%Y-%m-%d'))
                    encontrados_2 += 1
                else:
                    resultados.append('')

        df['FECHA DE PUBLICACIÓN D.O.F.'] = resultados
        total = encontrados_1 + encontrados_2
        logger.info(f"   FECHA DOF: {total}/{len(df)} encontradas (1ª búsqueda={encontrados_1}, 2ª búsqueda valor={encontrados_2})")
        return df

    def _cruzar_poliza_contable(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        Cruza con PolizaContable (hoja IMP) para obtener:
        - 'Pedimento Edo. Cuenta' de columna 'Pedimentos / Factura'
        El cruce se hace por PEDIMENTO COMPLETO.
        """
        if 'PolizaContable' not in sources:
            df['Pedimento Edo. Cuenta'] = ''
            logger.warning("   ⚠️ PolizaContable no disponible, Ped Edo Cta vacío")
            return df

        df_pol = sources['PolizaContable'].copy()
        logger.info(f"   PolizaContable[IMP]: {len(df_pol):,} registros")

        col_pedimento = 'Pedimentos / Factura'
        if col_pedimento not in df_pol.columns:
            matches = [c for c in df_pol.columns if 'edimento' in str(c) or 'actura' in str(c)]
            col_pedimento = matches[0] if matches else None

        if not col_pedimento:
            df['Pedimento Edo. Cuenta'] = ''
            logger.warning("   ⚠️ Columna pedimento no encontrada en PolizaContable")
            return df

        pol_map = df_pol[[col_pedimento]].copy()
        # Normalizar formato: Póliza usa espacios, Reporte usa guiones
        # Convertir "26 16 3949 5004814" → "26-16-3949-5004814"
        pol_map['_ped_completo'] = pol_map[col_pedimento].astype(str).str.strip().str.replace(' ', '-')
        pol_map['Pedimento Edo. Cuenta'] = pol_map['_ped_completo']
        pol_map = pol_map.drop_duplicates(subset='_ped_completo', keep='first')

        df = df.merge(
            pol_map[['_ped_completo', 'Pedimento Edo. Cuenta']],
            left_on='PEDIMENTO COMPLETO', right_on='_ped_completo',
            how='left'
        )
        df.drop(columns=['_ped_completo'], inplace=True)
        df['Pedimento Edo. Cuenta'] = df['Pedimento Edo. Cuenta'].fillna('')

        matched = (df['Pedimento Edo. Cuenta'] != '').sum()
        logger.info(f"   Póliza: {matched}/{len(df)} pedimentos en Edo. Cuenta")
        return df

    def _agregar_planta(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        Agrega PLANTA desde PolizaContable (hoja IMP), columna 'Planta Destino'.
        Se añade en posición 19, después de IVA PREV y antes de CLAVE TSMX.
        """
        if 'PolizaContable' not in sources:
            df['PLANTA'] = ''
            return df

        df_pol = sources['PolizaContable'].copy()

        col_planta = 'Planta Destino'
        if col_planta not in df_pol.columns:
            matches = [c for c in df_pol.columns if 'lanta' in str(c) and 'estino' in str(c)]
            if not matches:
                matches = [c for c in df_pol.columns if 'lanta' in str(c)]
            col_planta = matches[0] if matches else None

        if not col_planta:
            df['PLANTA'] = ''
            logger.warning("   ⚠️ Columna Planta Destino no encontrada")
            return df

        col_pedimento = 'Pedimentos / Factura'
        if col_pedimento not in df_pol.columns:
            matches = [c for c in df_pol.columns if 'edimento' in str(c) or 'actura' in str(c)]
            col_pedimento = matches[0] if matches else None

        if not col_pedimento:
            df['PLANTA'] = ''
            return df

        pol_map = pd.DataFrame()
        # Normalizar formato: Póliza usa espacios, Reporte usa guiones
        pol_map['_ped_completo'] = df_pol[col_pedimento].astype(str).str.strip().str.replace(' ', '-')
        pol_map['PLANTA'] = df_pol[col_planta].astype(str).str.strip()
        pol_map = pol_map.drop_duplicates(subset='_ped_completo', keep='first')

        df = df.merge(pol_map, left_on='PEDIMENTO COMPLETO', right_on='_ped_completo', how='left')
        df.drop(columns=['_ped_completo'], inplace=True)
        df['PLANTA'] = df['PLANTA'].fillna('')

        matched = (df['PLANTA'] != '').sum()
        logger.info(f"   PLANTA: {matched}/{len(df)} pedimentos")
        return df

    def _cruzar_acc_policy(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        Cruza con AccPolicyReport para obtener:
        - IVA PREV MXN = prev_0 * 0.16
        - CLAVE TSMX = pedimento_type
        """
        if 'AccPolicyReport' not in sources:
            df['IVA PREV MXN'] = ''
            df['CLAVE TSMX'] = ''
            logger.warning("   ⚠️ AccPolicyReport no disponible")
            return df

        df_acc = sources['AccPolicyReport'].copy()
        logger.info(f"   AccPolicyReport: {len(df_acc):,} registros")

        # Parsear pedimento_number: "25 07 3949 5060519"
        if 'pedimento_number' not in df_acc.columns:
            df['IVA PREV MXN'] = ''
            df['CLAVE TSMX'] = ''
            logger.warning("   ⚠️ AccPolicyReport sin columna 'pedimento_number'")
            return df

        parts = df_acc['pedimento_number'].astype(str).str.strip().str.split(r'\s+', expand=True)
        if parts.shape[1] >= 4:
            df_acc['Patente'] = parts[2]
            df_acc['Pedimento'] = parts[3]
            # customs_office como SeccionAduanera (normalizado int)
            df_acc['SeccionAduanera'] = pd.to_numeric(
                df_acc['customs_office'], errors='coerce'
            ).fillna(0).astype(int).astype(str)
            df_acc['_key'] = build_pedimento_key(df_acc)
        else:
            df['IVA PREV MXN'] = ''
            df['CLAVE TSMX'] = ''
            logger.warning("   ⚠️ Formato de pedimento_number no esperado")
            return df

        # IVA PREV = prev_0 (valor directo de la fuente, sin multiplicar)
        df_acc['prev_0'] = pd.to_numeric(df_acc.get('prev_0', 0), errors='coerce').fillna(0)
        df_acc['IVA PREV MXN'] = df_acc['prev_0']

        # CLAVE TSMX = pedimento_type
        df_acc['CLAVE TSMX'] = df_acc.get('pedimento_type', '').astype(str).str.strip()

        # Merge
        acc_map = df_acc.drop_duplicates(subset='_key', keep='first')[['_key', 'IVA PREV MXN', 'CLAVE TSMX']]
        df = df.merge(acc_map, on='_key', how='left')
        df['IVA PREV MXN'] = df['IVA PREV MXN'].fillna(0)
        df['CLAVE TSMX'] = df['CLAVE TSMX'].fillna('')

        matched_iva = (df['IVA PREV MXN'] > 0).sum()
        matched_clave = (df['CLAVE TSMX'] != '').sum()
        logger.info(f"   IVA PREV: {matched_iva}/{len(df)}, CLAVE TSMX (AccPolicy): {matched_clave}/{len(df)}")
        return df

    def _fallback_clave_tsmx_shippers(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        Busca CLAVE TSMX en Shippers (columna cve_doc) para los registros
        que no se encontraron en AccPolicyReport.
        """
        if 'Shippers' not in sources:
            logger.debug("   Shippers no disponible para fallback CLAVE TSMX")
            return df

        # Solo buscar los que no tienen CLAVE TSMX
        mask_empty = (df['CLAVE TSMX'] == '') | df['CLAVE TSMX'].isna()
        if not mask_empty.any():
            logger.info("   CLAVE TSMX completa, no se necesita fallback de Shippers")
            return df

        df_ship = sources['Shippers'].copy()
        logger.info(f"   Shippers: {len(df_ship):,} registros (fallback CLAVE TSMX)")

        # Verificar columnas
        needed = ['patente', 'pedimento', 'adua_sec_desp', 'cve_doc']
        if not all(c in df_ship.columns for c in needed):
            logger.warning(f"   ⚠️ Shippers sin columnas requeridas: {needed}")
            return df

        # Construir key para Shippers
        df_ship['_key'] = build_pedimento_key(
            df_ship,
            col_patente='patente',
            col_pedimento='pedimento',
            col_seccion='adua_sec_desp'
        )
        df_ship['_cve_doc'] = df_ship['cve_doc'].astype(str).str.strip()

        # Tomar un valor por pedimento
        ship_map = df_ship.drop_duplicates(subset='_key', keep='first')[['_key', '_cve_doc']]

        # Merge
        df = df.merge(ship_map, on='_key', how='left')

        # Rellenar solo donde CLAVE TSMX está vacía
        fill_mask = mask_empty & df['_cve_doc'].notna() & (df['_cve_doc'] != '')
        df.loc[fill_mask, 'CLAVE TSMX'] = df.loc[fill_mask, '_cve_doc']
        df.drop(columns=['_cve_doc'], inplace=True)

        filled = fill_mask.sum()
        total_tsmx = (df['CLAVE TSMX'] != '').sum()
        logger.info(f"   CLAVE TSMX (Shippers fallback): +{filled}, total: {total_tsmx}/{len(df)}")
        return df

    def _procesar_rectificaciones_701(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        Procesa rectificaciones desde 701.
        Si un pedimento de la 501 aparece como PedimentoAnterior en 701:
        1. Pone el pedimento ORIGINAL en Observaciones (formato completo)
        2. SUSTITUYE los valores de la fila con los del pedimento rectificado
           (nuevo pedimento, nueva sección, nueva patente, etc.)
        """
        if 'RectificacionesHeader' not in sources:
            df['Observaciones'] = ''
            logger.info("   701 no disponible (RectificacionesHeader), Observaciones vacío")
            return df

        df_701 = sources['RectificacionesHeader'].copy()
        logger.info(f"   701 Rectificaciones: {len(df_701):,} registros")

        # Llave del pedimento anterior (el que fue rectificado)
        df_701['_key_anterior'] = build_pedimento_key(
            df_701,
            col_patente='PatenteAnterior',
            col_pedimento='PedimentoAnterior',
            col_seccion='SeccionAduaneraAnterior'
        )

        # Parsear fechas de la 701
        fecha_701 = pd.to_datetime(df_701['FechaPagoReal'], format='mixed', errors='coerce')

        # Datos del pedimento NUEVO (la rectificación)
        df_701['_nuevo_pedimento'] = df_701['Pedimento'].astype(str).str.strip()
        df_701['_nuevo_patente'] = df_701['Patente'].astype(str).str.strip()
        df_701['_nuevo_seccion'] = df_701['SeccionAduanera'].astype(str).str.strip()
        df_701['_nuevo_fecha'] = fecha_701.dt.strftime('%Y-%m-%d')
        df_701['_nuevo_clave'] = df_701['ClaveDocumento'].astype(str).str.strip()
        df_701['_nuevo_ped_completo'] = build_pedimento_completo(
            fecha_701, df_701['SeccionAduanera'], df_701['Patente'], df_701['Pedimento']
        )
        df_701['_nuevo_dia'] = fecha_701.dt.day
        df_701['_nuevo_mes'] = fecha_701.dt.month
        df_701['_nuevo_anio'] = fecha_701.dt.year

        # Construir pedimento original completo para Observaciones
        fecha_ant = pd.to_datetime(df_701['FechaOperacionAnterior'], format='mixed', errors='coerce')
        df_701['_ped_original_completo'] = build_pedimento_completo(
            fecha_ant, df_701['SeccionAduaneraAnterior'],
            df_701['PatenteAnterior'], df_701['PedimentoAnterior']
        )

        # Merge info de rectificación
        rect_cols = ['_key_anterior', '_nuevo_pedimento', '_nuevo_patente', '_nuevo_seccion',
                     '_nuevo_fecha', '_nuevo_clave', '_nuevo_ped_completo',
                     '_nuevo_dia', '_nuevo_mes', '_nuevo_anio', '_ped_original_completo']
        rect_map = df_701[rect_cols].drop_duplicates(subset='_key_anterior', keep='first')
        rect_map = rect_map.rename(columns={'_key_anterior': '_key'})

        df = df.merge(rect_map, on='_key', how='left')

        # Donde hay rectificación → sustituir valores y poner original en observaciones
        mask = df['_nuevo_pedimento'].notna()
        if mask.any():
            # Observaciones = pedimento original
            df.loc[mask, 'Observaciones'] = df.loc[mask, '_ped_original_completo']

            # Sustituir valores con los del pedimento rectificado
            df.loc[mask, 'PEDIMENTO'] = df.loc[mask, '_nuevo_pedimento']
            df.loc[mask, 'PATENTE'] = df.loc[mask, '_nuevo_patente']
            df.loc[mask, 'SECCION ADUANERA'] = df.loc[mask, '_nuevo_seccion']
            df.loc[mask, 'FECHA PAGO REAL'] = df.loc[mask, '_nuevo_fecha']
            df.loc[mask, 'CLAVE DE PEDIMENTO DS'] = df.loc[mask, '_nuevo_clave']
            df.loc[mask, 'PEDIMENTO COMPLETO'] = df.loc[mask, '_nuevo_ped_completo']
            df.loc[mask, 'DIA'] = df.loc[mask, '_nuevo_dia']
            df.loc[mask, 'MES'] = df.loc[mask, '_nuevo_mes']
            df.loc[mask, 'AÑO'] = df.loc[mask, '_nuevo_anio']

            n_rect = mask.sum()
            logger.info(f"   Rectificaciones aplicadas: {n_rect}/{len(df)}")
        else:
            df['Observaciones'] = ''
            logger.info("   Sin rectificaciones que aplicar")

        df['Observaciones'] = df['Observaciones'].fillna('')

        # Limpiar columnas temporales de rectificación
        temp_cols = [c for c in df.columns if c.startswith('_nuevo_') or c == '_ped_original_completo']
        df.drop(columns=temp_cols, inplace=True)

        return df

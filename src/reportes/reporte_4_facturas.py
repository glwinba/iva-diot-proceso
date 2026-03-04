# ============================================================================
# src/reportes/reporte_4_facturas.py - PROYECTO_DIOT
# Reporte 4: Relación de Facturas de Importación y Exportación
# Fuente principal: Shippers, complementado con 501, MIC InvoiceReport
# ============================================================================

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# Mapeo tipo_operacion de Shippers → texto
TIPO_OPER_MAP = {'I': 'Importación', 'E': 'Exportación'}

# Catálogo Localidad Ford (basado en catálogo plantas)
LOCALIDAD_MAP = {
    'TC': 'Trading Company',
    'CSAP': 'Cuautitlan Stamping and Assembly Plant',
    'HSAP': 'Hermosillo Stamping and Assembly Plant',
    'CHEP': 'Chihuahua Engine Plant',
    'DRAGON': 'Chihuahua',
    'IEPC': 'Irapuato Electric Powertrain Center',
    'CBAP': 'Cuautitlán Battery Assembly Plant',
    'ODC': 'San Luis Potosí',
    'FCSD': 'FCSD',
    'GTBC VEHICULOS': 'Global Technology Business Center (El Cristo)',
    'GTBC PD': 'Global Technology Business Center (El Cristo)',
    'IRAPUATO': 'Irapuato Electric Powertrain Center',
    'CUAUTITLAN': 'Cuautitlan Stamping and Assembly Plant',
    'HERMOSILLO': 'Hermosillo Stamping and Assembly Plant',
    'CHIHUAHUA': 'Chihuahua Engine Plant',
    'SAN LUIS': 'San Luis Potosí',
}

MIC_LOCATION_MAP = {
    'AP24A': 'HSAP',
    'AP23A': 'HSAP',
    'EF18A': 'CHEP',
    '4133A': 'CSAP',
    '4125A': 'CSAP',
    'GMP5': 'IEPC',
    'HWQXA': 'FCSD',
    'GATKA': 'TC',
}

# Mapeo planta -> código numérico (Catálogo NAD Ford)
PLANTA_CODES_MAP = {
    'TC': '15', 'TRADING COMPANY': '15',
    'CSAP': '11', 'CUAUTITLAN': '11',
    'HSAP': '13', 'HERMOSILLO': '13',
    'CHEP': '12', 'CHIHUAHUA': '12',
    'DRAGON': '17',
    'IEPC': '16', 'IRAPUATO': '16',
    'CBAP': '18',
    'ODC': '14', 'SAN LUIS POTOSI': '14',
    'FCSD': '20',
    'EL CRISTO': '50', 'GTBC VEHICULOS': '50', 'GTBC PD': '51'
}

# Mapeo material -> código numérico (Catálogo NAD Ford)
MATERIAL_CODES_MAP = {
    'PRODUCTIVO': '01',
    'NO PRODUCTIVO': '02',
    'MAQUINARIA Y EQUIPO': '03',
    'AUTOS': '04', 'VEHICULOS': '04',
    'LLANTAS': '05',
    'D2D': '06',
    'REFACCIONES': '07',
    'PROTOTIPO': '08',
    'LAMINA': '09',
    'RACKS': '10', 'RETORNABLES': '10',
    'ALUMINIO': '11'
}


def _build_ship_key(df: pd.DataFrame) -> pd.Series:
    """Construye clave patente|pedimento|seccion para cruce con 501."""
    patente = df['patente'].astype(str).str.strip()
    pedimento = df['pedimento'].astype(str).str.strip()
    seccion = df['adua_sec_desp'].astype(str).str.strip()
    # Normalizar sección a int para quitar ceros
    sec_norm = pd.to_numeric(seccion, errors='coerce').fillna(0).astype(int).astype(str)
    return patente + '|' + pedimento + '|' + sec_norm


class Reporte4Facturas:
    """
    Reporte 4: Relación de Facturas de Importación y Exportación.
    29 columnas, fuente principal Shippers, complementado con 501 e InvoiceReport.
    """

    nombre = "R4_Facturas"

    def generar(self, sources: dict) -> pd.DataFrame:
        """Genera R4 a partir de las fuentes."""
        if 'Shippers' not in sources:
            logger.error("❌ Shippers no disponible para R4")
            return pd.DataFrame()

        logger.info("🔧 Generando Reporte 4: Relación de Facturas IMPO/EXPO...")

        df_ship = sources['Shippers'].copy()
        logger.info(f"   Shippers: {len(df_ship):,} registros")

        # Preparar MIC InvoiceReport para cruces
        mic_cove_map, mic_vin_map, mic_loc_map = self._preparar_mic(sources)

        df = pd.DataFrame()

        # key auxiliar para cruces con 501
        df['_ship_key'] = _build_ship_key(df_ship)

        # === 1. Pedimento: formato AA-SS-PPPP-NNNNNNN ===
        df = self._formatear_pedimento(df, df_ship, sources)

        # === 2. Unidad Negocio ===
        destino_raw = df_ship['Destino'].astype(str).str.strip()
        cove_raw = df_ship['num_factura'].astype(str).str.strip()
        df['Unidad Negocio'] = self._resolver_unidad_negocio(destino_raw, cove_raw, mic_loc_map)

        # === 3. Tipo Operación (texto) ===
        df['Tipo Operación'] = df_ship['tipo_operacion'].astype(str).str.strip().map(TIPO_OPER_MAP).fillna('')

        # 4. Clave (cve_doc)
        df['Clave'] = df_ship['cve_doc'].astype(str).str.strip()

        # === 5. Factura: reemplazar COVE por factura real de MIC ===
        factura_raw = df_ship['num_factura'].astype(str).str.strip()
        df['Factura'] = self._resolver_factura(factura_raw, mic_cove_map)

        # 6. Venta/No Venta — se calcula después de resolver Destino

        # === 7. VIN: desde MIC InvoiceReport, concatenados por factura ===
        df['VIN'] = self._resolver_vin(cove_raw, mic_vin_map)

        # 8. Fecha Pago (FechaPagoReal del 501)
        df = self._cruzar_fecha_pago(df, sources)

        # 9. Tipo de Cambio (del 501)
        df = self._cruzar_tipo_cambio(df, sources)

        # 10. Fecha Factura
        df['Fecha Factura'] = df_ship['fecha_fac'].astype(str).str.strip()

        # 11. Incoterms
        df['Incoterms'] = df_ship['termino_fac'].astype(str).str.strip().replace('nan', '')

        # 12. Moneda
        df['Moneda'] = df_ship['moneda_fac'].astype(str).str.strip()

        # 13. Valor DLLS
        df['Valor DLLS'] = pd.to_numeric(df_ship['valor_dls'], errors='coerce').fillna(0)

        # 14. Valor ME
        df['Valor ME'] = pd.to_numeric(df_ship['valor_me'], errors='coerce').fillna(0)

        # 15. Proveedor
        df['Proveedor'] = df_ship['proveedor'].astype(str).str.strip()

        # 16. Tax
        df['Tax'] = df_ship['tax_prov'].astype(str).str.strip().replace('nan', '')

        # 17. Calle
        df['Calle'] = df_ship['calle'].astype(str).str.strip().replace('nan', '')

        # 18. Número Interior
        df['Número Interior'] = df_ship['num_int'].astype(str).str.strip().replace('nan', '')

        # 19. Número Exterior
        df['Número Exterior'] = df_ship['num_ext'].astype(str).str.strip().replace('nan', '')

        # 20. Código Postal
        df['Código Postal'] = df_ship['cp_prov'].astype(str).str.strip().replace('nan', '')

        # 21. Municipio
        df['Municipio'] = df_ship['municipio_prov'].astype(str).str.strip().replace('nan', '')

        # 22. País
        df['País'] = df_ship['pais_fac'].astype(str).str.strip()

        # 23. E-Document (conservar COVE original)
        df['E-Document'] = df_ship['EDocument'].astype(str).str.strip().replace('nan', '')

        # 24. Vinculación
        df['Vinculación'] = df_ship['Vinculacion'].astype(str).str.strip().replace('nan', '')

        # 25. UUID-CFDI
        df['UUID-CFDI'] = df_ship['UUDI'].astype(str).str.strip().replace('nan', '')

        # === 26. Destino (sin blanks: fallback MIC → fallback aduana más frecuente) ===
        destino_clean = destino_raw.replace('nan', '')
        mic_destino = cove_raw.map(mic_loc_map).fillna('')
        df['Destino'] = destino_clean.where(destino_clean != '', mic_destino)

        # Tercer fallback: Destino más frecuente por aduana (de filas con Destino)
        aduana_s = df_ship['adua_sec_desp'].astype(str).str.strip()
        dest_still_blank = (df['Destino'].astype(str).str.strip() == '') | (df['Destino'].astype(str) == 'nan')
        if dest_still_blank.any():
            has_dest_mask = (destino_clean != '') & (destino_clean != 'nan')
            if has_dest_mask.any():
                dest_freq = (pd.DataFrame({'aduana': aduana_s[has_dest_mask], 'dest': destino_clean[has_dest_mask]})
                             .groupby('aduana')['dest']
                             .agg(lambda x: x.value_counts().index[0])
                             .to_dict())
                aduana_fallback = aduana_s.map(dest_freq).fillna('')
                df.loc[dest_still_blank, 'Destino'] = aduana_fallback[dest_still_blank]
                resolved = dest_still_blank.sum() - (df['Destino'].astype(str).str.strip().isin(['', 'nan'])).sum()
                logger.info(f"   Destino fallback por aduana: {resolved} resueltos")

        # === 27. Planta (sin blanks: derivar de Destino final) ===
        df['Planta'] = df['Destino'].apply(self._extraer_planta)

        # === Actualizar Unidad Negocio para blanks restantes (usando Destino final) ===
        un_blank = (df['Unidad Negocio'].astype(str).str.strip() == '') | (df['Unidad Negocio'].astype(str) == 'nan')
        if un_blank.any():
            un_from_dest = df.loc[un_blank, 'Destino'].apply(self._extraer_unidad_negocio)
            df.loc[un_blank, 'Unidad Negocio'] = un_from_dest

        # === Venta/No Venta basado en Tipo Operación, Factura y UUID ===
        factura_str = df['Factura'].astype(str).str.strip().str.upper()
        uuid_str = df['UUID-CFDI'].astype(str).str.strip()
        tipo_op = df['Tipo Operación'].astype(str).str.strip()

        condiciones_venta = [
            tipo_op == 'Importación',
            factura_str.str.startswith('FMMD'),
            factura_str.str.startswith('FMMT'),
            uuid_str != ''
        ]
        resultados_venta = [
            'Sin clasificación',
            'Venta',
            'No Venta',
            'Venta'
        ]
        df['Venta/No Venta'] = np.select(condiciones_venta, resultados_venta, default='Confirmar Venta/No Venta')

        # === 28. Fuente: solo MIC y NAD ===
        df['Fuente'] = df['Clave'].apply(self._inferir_fuente)

        # 29. Notas
        df['Notas'] = ''

        # Limpiar auxiliares
        df.drop(columns=[c for c in df.columns if c.startswith('_')], inplace=True, errors='ignore')

        # Stats
        imp_count = (df['Tipo Operación'] == 'Importación').sum()
        exp_count = (df['Tipo Operación'] == 'Exportación').sum()
        vin_count = (df['VIN'].astype(str).str.strip() != '').sum()
        cove_remaining = df['Factura'].astype(str).str.contains('COVE', case=False).sum()
        dest_blanks = (df['Destino'].astype(str).str.strip().isin(['', 'nan'])).sum()
        logger.info(f"✅ Reporte 4 generado: {len(df):,} facturas "
                     f"(IMP={imp_count:,}, EXP={exp_count:,}), "
                     f"VINs={vin_count:,}, COVE residual={cove_remaining}, "
                     f"Destino blanks={dest_blanks}")
        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _preparar_mic(self, sources: dict) -> tuple:
        """
        Prepara mapeos desde MIC InvoiceReport:
        - cove_map: COVE → INVOICE (factura real)
        - vin_map: COVE → VINs concatenados
        - loc_map: COVE → LOCATION (destino/planta)
        """
        empty = ({}, {}, {})
        if 'InvoiceReport' not in sources:
            logger.warning("   ⚠️ InvoiceReport no disponible — COVE/VIN/LOCATION sin resolver")
            return empty

        df_mic = sources['InvoiceReport'].copy()
        logger.info(f"   InvoiceReport (MIC): {len(df_mic):,} registros")

        cove_col = 'COVE' if 'COVE' in df_mic.columns else None
        inv_col = 'INVOICE' if 'INVOICE' in df_mic.columns else None
        vin_col = 'VIN_NUMBER' if 'VIN_NUMBER' in df_mic.columns else None
        loc_col = 'LOCATION' if 'LOCATION' in df_mic.columns else None

        if not cove_col:
            logger.warning("   ⚠️ InvoiceReport sin columna COVE")
            return empty

        df_mic['_cove'] = df_mic[cove_col].astype(str).str.strip()

        # 1. COVE → INVOICE
        cove_map = {}
        if inv_col:
            dedup = df_mic.drop_duplicates(subset='_cove', keep='first')
            cove_map = dict(zip(dedup['_cove'], dedup[inv_col].astype(str).str.strip()))
            logger.info(f"   MIC COVE→Invoice: {len(cove_map):,} mapeos")

        # 2. COVE → VINs concatenados (todos los VINs de la factura en un string)
        vin_map = {}
        if vin_col:
            df_vin = df_mic[df_mic[vin_col].notna()].copy()
            df_vin['_vin'] = df_vin[vin_col].astype(str).str.strip()
            df_vin = df_vin[df_vin['_vin'] != '']
            if len(df_vin) > 0:
                vin_groups = df_vin.groupby('_cove')['_vin'].apply(
                    lambda x: ','.join(sorted(set(x)))
                ).to_dict()
                vin_map = vin_groups
                logger.info(f"   MIC COVE→VINs: {len(vin_map):,} facturas con VINs")

        # 3. COVE → LOCATION (para resolver Destino/Planta)
        loc_map = {}
        if loc_col:
            dedup_loc = df_mic.drop_duplicates(subset='_cove', keep='first')
            loc_map = dict(zip(dedup_loc['_cove'], dedup_loc[loc_col].astype(str).str.strip()))

        return cove_map, vin_map, loc_map

    def _formatear_pedimento(self, df: pd.DataFrame, df_ship: pd.DataFrame,
                              sources: dict) -> pd.DataFrame:
        """
        Formato pedimento: AA-SS-PPPP-NNNNNNN
        AA = últimos 2 dígitos del año (de FechaPagoReal 501)
        SS = primeros 2 dígitos de SeccionAduanera (430→43, 800→80)
        PPPP = 4 dígitos de Patente
        NNNNNNN = 7 dígitos del Pedimento (folio)
        """
        # Obtener año del 501
        year_map = {}
        if 'DatosGenerales' in sources:
            df_501 = sources['DatosGenerales'].copy()
            from src.reportes.reporte_1_pedimentos import build_pedimento_key
            df_501['_key'] = build_pedimento_key(df_501)
            fecha = pd.to_datetime(df_501['FechaPagoReal'], format='mixed', errors='coerce')
            df_501['_year2'] = (fecha.dt.year % 100).astype('Int64').astype(str).str.zfill(2)
            year_dedup = df_501.drop_duplicates(subset='_key', keep='first')
            year_map = dict(zip(year_dedup['_key'], year_dedup['_year2']))

        ship_keys = _build_ship_key(df_ship)
        # Año: buscar en 501, fallback "25"
        anio = ship_keys.map(year_map).fillna('25')

        # Sección: primeros 2 dígitos (430→43, 800→80, 650→65)
        sec = pd.to_numeric(df_ship['adua_sec_desp'], errors='coerce').fillna(0).astype(int)
        sec_2d = (sec // 10).astype(str).str.zfill(2)

        # Patente: 4 dígitos
        pat = df_ship['patente'].astype(str).str.strip().str.zfill(4)

        # Pedimento: 7 dígitos
        ped = df_ship['pedimento'].astype(str).str.strip().str.zfill(7)

        df['Pedimento'] = anio + '-' + sec_2d + '-' + pat + '-' + ped
        return df

    def _resolver_unidad_negocio(self, destino: pd.Series, cove: pd.Series,
                                   mic_loc_map: dict) -> pd.Series:
        """
        Unidad Negocio: primero de Shippers Destino, fallback a MIC LOCATION.
        """
        # De Shippers Destino
        un_ship = destino.apply(self._extraer_unidad_negocio)

        # Fallback: MIC LOCATION → código planta → LOCALIDAD_MAP
        mic_loc_code = cove.map(mic_loc_map).fillna('')
        un_mic = mic_loc_code.map(MIC_LOCATION_MAP).fillna('')
        un_mic_resolved = un_mic.apply(
            lambda code: LOCALIDAD_MAP.get(code, code) if code else ''
        )

        # Combinar: Shippers primero, MIC como fallback
        result = un_ship.where(un_ship != '', un_mic_resolved)
        return result

    def _resolver_factura(self, factura: pd.Series, cove_map: dict) -> pd.Series:
        """
        Si la factura es COVE, reemplazar por la factura real del MIC InvoiceReport.
        """
        if not cove_map:
            return factura

        is_cove = factura.str.startswith('COVE')
        replaced = factura.map(cove_map)
        result = factura.copy()
        # Solo reemplazar donde hay COVE y encontramos match en MIC
        mask = is_cove & replaced.notna()
        result[mask] = replaced[mask]

        replaced_count = mask.sum()
        remaining = (result.str.startswith('COVE')).sum()
        logger.info(f"   Factura: {replaced_count:,} COVE reemplazados, {remaining} COVE residual")
        return result

    def _resolver_vin(self, cove: pd.Series, vin_map: dict) -> pd.Series:
        """
        VINs desde MIC InvoiceReport, concatenados por factura (COVE).
        """
        if not vin_map:
            return pd.Series('', index=cove.index)

        result = cove.map(vin_map).fillna('')
        filled = (result != '').sum()
        logger.info(f"   VIN: {filled:,}/{len(cove):,} facturas con VINs")
        return result

    @staticmethod
    def _extraer_unidad_negocio(destino: str) -> str:
        """Extrae la localidad Ford del campo Destino."""
        if not destino or destino == 'nan':
            return ''
        # Destino puede ser "IRAPUATO/NO PRODUCTIVO" → buscar antes del /
        parte = destino.split('/')[0].strip().upper()
        # Buscar en catálogo
        for key, val in LOCALIDAD_MAP.items():
            if key.upper() in parte:
                return val
        return parte  # retornar tal cual si no se encuentra

    @staticmethod
    def _extraer_planta(destino: str) -> str:
        """
        Calcula la Planta combinando Código Planta + Código Material.
        Ejemplo: HERMOSILLO/AUTOS -> 13 + 04 = 1304
        AP24A -> HSAP -> 13 + 01 (default) = 1301
        """
        if not destino or destino == 'nan':
            return ''
        
        partes = destino.split('/')
        planta_str = partes[0].strip().upper()
        
        # Interceptar códigos MIC internos que quedaron rezagados en Destino
        if planta_str in MIC_LOCATION_MAP:
            planta_str = MIC_LOCATION_MAP[planta_str]
        
        cod_planta = ''
        for key, val in PLANTA_CODES_MAP.items():
            if key in planta_str:
                cod_planta = val
                break
        
        if not cod_planta:
            return destino.strip()
            
        cod_mat = '01' # Default: Productivo si no se declara otra cosa
        if len(partes) > 1:
            mat_str = partes[1].strip().upper()
            for key, val in MATERIAL_CODES_MAP.items():
                if key in mat_str:
                    cod_mat = val
                    break
        
        return cod_planta + cod_mat

    @staticmethod
    def _inferir_fuente(clave: str) -> str:
        """Infiere la fuente: solo MIC y NAD."""
        if not clave:
            return ''
        c = clave.strip().upper()
        if c in ('A1', 'I1', 'H1', 'V1'):
            return 'MIC'
        elif c in ('F2', 'F3', 'R1'):
            return 'NAD'
        return 'NAD'  # default NAD

    def _cruzar_fecha_pago(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """Obtiene FechaPagoReal del 501."""
        df['Fecha Pago'] = ''
        if 'DatosGenerales' not in sources:
            return df

        df_501 = sources['DatosGenerales'].copy()
        from src.reportes.reporte_1_pedimentos import build_pedimento_key
        df_501['_key_501'] = build_pedimento_key(df_501)
        fecha_parsed = pd.to_datetime(df_501['FechaPagoReal'], format='mixed', errors='coerce')
        df_501['_fecha_pago'] = fecha_parsed.dt.strftime('%Y-%m-%d')

        pago_map = df_501.drop_duplicates(subset='_key_501', keep='first')[['_key_501', '_fecha_pago']]
        pago_map.columns = ['_ship_key', '_fp']

        df = df.merge(pago_map, on='_ship_key', how='left')
        df['Fecha Pago'] = df['_fp'].fillna('')
        df.drop(columns=['_fp'], inplace=True, errors='ignore')

        matched = (df['Fecha Pago'] != '').sum()
        logger.info(f"   Fecha Pago (501): {matched}/{len(df)} encontrados")
        return df

    def _cruzar_tipo_cambio(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """Obtiene TipoCambio del 501."""
        df['Tipo de Cambio'] = ''
        if 'DatosGenerales' not in sources:
            return df

        df_501 = sources['DatosGenerales'].copy()
        from src.reportes.reporte_1_pedimentos import build_pedimento_key
        df_501['_key_501'] = build_pedimento_key(df_501)

        tc_map = df_501.drop_duplicates(subset='_key_501', keep='first')[['_key_501', 'TipoCambio']]
        tc_map.columns = ['_ship_key', '_tc']

        df = df.merge(tc_map, on='_ship_key', how='left')
        df['Tipo de Cambio'] = df['_tc'].fillna('')
        df.drop(columns=['_tc'], inplace=True, errors='ignore')

        matched = (df['Tipo de Cambio'] != '').sum()
        logger.info(f"   Tipo de Cambio (501): {matched}/{len(df)} encontrados")
        return df

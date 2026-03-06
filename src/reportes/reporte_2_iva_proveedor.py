# ============================================================================
# src/reportes/reporte_2_iva_proveedor.py - PROYECTO_DIOT
# Reporte 2: IVA por Proveedor con Detalle de Importaciones
# ============================================================================
# Nivel: una fila por cada registro del 505
#
# Campos (26):
#   1.  TaxID Proveedor          <- 505.IndentFiscalProveedor
#   2.  Código Proveedor         <- SupplierReport.Partner (fallback: TaxID)
#   3.  Nombre del Proveedor     <- 505.ProveedorMercancia (o "CAAAREM")
#   4.  País Vendedor            <- 505.PaisFacturacion → SupplierReport → 551.PaisCompradorVendedor
#   5.  Nacionalidad             <- Traducción ISO 3166
#   6.  Base Gravable MXN        <- SUM(551.ValorAduana) + SUM(557.ImportePago DTA+IGI FP=0)
#   7.  Cálculo IVA Exceptuado   <- (BaseGravable×0.16) - IVA real pagado
#   8.  IVA al 16% MXN           <- SUM(557.ImportePago clave=6 FP=0) real pagado
#   9.  Prevalidación MXN        <- 510 clave=15 FP=0
#  10.  IVA Prevalidación MXN    <- 510 clave=23 FP=0
#  11.  Año
#  12.  Aduana
#  13.  Patente
#  14.  Pedimento
#  15.  Fecha de Pago (DS)
#  16.  Clave del Pedimento      <- 501.ClaveDocumento
#  17.  Fecha Recepción Ped.
#  18.  Fecha Real Pago (Bancario)
#  19.  Clave Forma de Pago      <- Solo FP=0
#  20.  TIPO CAMBIO MXP
#  21.  Valor Aduana DLLS
#  22.  Valor Comercial DLLS
#  23.  Total Pagado Imp MXP
#  24.  Total Pagado Imp DLLS
#  25.  Tipo de Material
#  26.  Tipo Operación            <- 501.TipoOperacion (1=IMP, 2=EXP)
# ============================================================================

import pandas as pd
import logging
from .base_reporte import BaseReporte
from .reporte_1_pedimentos import build_pedimento_key, build_pedimento_completo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Catálogo ISO 3166 (códigos más comunes en comercio exterior México)
# ---------------------------------------------------------------------------
PAISES_ISO = {
    'USA': 'ESTADOS UNIDOS DE AMÉRICA', 'US': 'ESTADOS UNIDOS DE AMÉRICA',
    'MEX': 'MÉXICO (ESTADOS UNIDOS MEXICANOS)', 'MX': 'MÉXICO (ESTADOS UNIDOS MEXICANOS)',
    'CAN': 'CANADÁ', 'CA': 'CANADÁ',
    'DEU': 'ALEMANIA (REPÚBLICA FEDERAL DE)', 'DE': 'ALEMANIA (REPÚBLICA FEDERAL DE)',
    'CHN': 'CHINA (REPÚBLICA POPULAR)', 'CN': 'CHINA (REPÚBLICA POPULAR)',
    'JPN': 'JAPÓN', 'JP': 'JAPÓN',
    'KOR': 'COREA (REPÚBLICA DE) (COREA DEL SUR)', 'KR': 'COREA (REPÚBLICA DE) (COREA DEL SUR)',
    'GBR': 'REINO UNIDO DE LA GRAN BRETAÑA E IRLANDA DEL NORTE', 'GB': 'REINO UNIDO DE LA GRAN BRETAÑA E IRLANDA DEL NORTE',
    'FRA': 'FRANCIA (REPÚBLICA FRANCESA)', 'FR': 'FRANCIA (REPÚBLICA FRANCESA)',
    'ITA': 'ITALIA', 'IT': 'ITALIA',
    'ESP': 'ESPAÑA (REINO DE)', 'ES': 'ESPAÑA (REINO DE)',
    'BRA': 'BRASIL (REPÚBLICA FEDERATIVA DE)', 'BR': 'BRASIL (REPÚBLICA FEDERATIVA DE)',
    'IND': 'INDIA', 'IN': 'INDIA',
    'TUR': 'TURQUÍA (REPÚBLICA DE)', 'TR': 'TURQUÍA (REPÚBLICA DE)',
    'TWN': 'TAIWÁN (REPÚBLICA DE CHINA)', 'TW': 'TAIWÁN (REPÚBLICA DE CHINA)',
    'THA': 'TAILANDIA (REINO DE)', 'TH': 'TAILANDIA (REINO DE)',
    'AUS': 'AUSTRALIA', 'AU': 'AUSTRALIA',
    'AUT': 'AUSTRIA', 'AT': 'AUSTRIA',
    'BEL': 'BÉLGICA', 'BE': 'BÉLGICA',
    'CZE': 'REPÚBLICA CHECA', 'CZ': 'REPÚBLICA CHECA',
    'POL': 'POLONIA', 'PL': 'POLONIA',
    'HUN': 'HUNGRÍA (REPÚBLICA DE)', 'HU': 'HUNGRÍA (REPÚBLICA DE)',
    'NLD': 'PAÍSES BAJOS', 'NL': 'PAÍSES BAJOS',
    'SWE': 'SUECIA (REINO DE)', 'SE': 'SUECIA (REINO DE)',
    'CHE': 'SUIZA', 'CH': 'SUIZA',
    'ROU': 'RUMANIA', 'RO': 'RUMANIA',
    'BGR': 'BULGARIA (REPÚBLICA DE)', 'BG': 'BULGARIA (REPÚBLICA DE)',
    'ARG': 'Argentina', 'AR': 'Argentina', 'COL': 'Colombia', 'CO': 'Colombia',
    'VNM': 'Vietnam', 'VN': 'Vietnam', 'MYS': 'Malasia', 'MY': 'Malasia',
    'IDN': 'Indonesia', 'ID': 'Indonesia', 'PHL': 'Filipinas', 'PH': 'Filipinas',
    'SGP': 'Singapur', 'SG': 'Singapur', 'ZAF': 'Sudáfrica', 'ZA': 'Sudáfrica',
    'PRT': 'Portugal', 'PT': 'Portugal', 'SVK': 'Eslovaquia', 'SK': 'Eslovaquia',
    'ISR': 'Israel', 'IL': 'Israel', 'RUS': 'Rusia', 'RU': 'Rusia',
    'DNK': 'Dinamarca', 'DK': 'Dinamarca', 'FIN': 'Finlandia', 'FI': 'Finlandia',
    'NOR': 'Noruega', 'NO': 'Noruega', 'IRL': 'Irlanda', 'IE': 'Irlanda',
    'CHL': 'Chile', 'CL': 'Chile', 'PER': 'Perú', 'PE': 'Perú',
    'GTM': 'Guatemala', 'GT': 'Guatemala', 'PAN': 'Panamá', 'PA': 'Panamá',
    'DOM': 'República Dominicana', 'DO': 'República Dominicana',
    'CRI': 'Costa Rica', 'CR': 'Costa Rica',
    'ECU': 'Ecuador', 'EC': 'Ecuador', 'URY': 'Uruguay', 'UY': 'Uruguay',
    'VEN': 'Venezuela', 'VE': 'Venezuela',
    'LUX': 'Luxemburgo', 'LU': 'Luxemburgo',
    'GRC': 'Grecia', 'GR': 'Grecia',
}


class Reporte2IvaProveedor(BaseReporte):
    """Reporte 2: IVA por Proveedor con Detalle de Importaciones."""

    nombre = "R2_IVA_Proveedor"
    fuentes_requeridas = ['ContribucionesPartida']  # 557 es la fuente base (IVA real pagado)

    def generar(self, sources: dict, mes_proceso: str = '') -> pd.DataFrame:
        if not self.validar_fuentes(sources):
            return pd.DataFrame()

        logger.info("🔧 Generando Reporte 2: IVA por Proveedor con Detalle...")

        # === BASE DESDE 557: Pedimentos con IVA real pagado (CC=3, FP=0) ===
        df_557_src = sources['ContribucionesPartida'].copy()
        df_557_src['_key'] = build_pedimento_key(df_557_src)
        df_557_src['ImportePago'] = pd.to_numeric(df_557_src['ImportePago'], errors='coerce').fillna(0)
        df_557_src['ClaveContribucion'] = df_557_src['ClaveContribucion'].astype(str).str.strip()
        df_557_src['FormaPago'] = df_557_src['FormaPago'].astype(str).str.strip()

        iva_557 = df_557_src[(df_557_src['FormaPago'] == '0') & (df_557_src['ClaveContribucion'] == '3')]
        iva_por_ped = iva_557.groupby('_key')['ImportePago'].sum().reset_index()
        iva_por_ped = iva_por_ped[iva_por_ped['ImportePago'] > 0]
        keys_base = set(iva_por_ped['_key'].unique())
        logger.info(f"   557 Base: {len(keys_base)} pedimentos con IVA (CC=3, FP=0)")

        # === RESOLVER PROVEEDORES: Shippers (primario) → 505 (fallback) → F3/FORD ===

        # --- Obtener ClaveDocumento del 501 para lógica F3 ---
        clave_doc_map = {}
        if 'DatosGenerales' in sources:
            df_501_src = sources['DatosGenerales'].copy()
            df_501_src['_key'] = build_pedimento_key(df_501_src)
            if 'ClaveDocumento' in df_501_src.columns:
                for _, row in df_501_src.drop_duplicates(subset='_key', keep='first').iterrows():
                    clave_doc_map[row['_key']] = str(row['ClaveDocumento']).strip()

        # --- Shippers: proveedor primario ---
        rows_base = []
        keys_resueltos = set()

        if 'Shippers' in sources:
            df_ship = sources['Shippers'].copy()
            df_ship['_key'] = build_pedimento_key(
                df_ship, col_patente='patente', col_pedimento='pedimento',
                col_seccion='adua_sec_desp')
            df_ship['_taxid'] = df_ship['tax_prov'].astype(str).str.strip().str.replace('-', '')

            # Solo filas de Shippers cuyos pedimentos están en la base 557
            df_ship_base = df_ship[df_ship['_key'].isin(keys_base)].copy()

            if not df_ship_base.empty:
                # NO deduplicar: Ford mantiene una fila por línea de factura
                df_ship_base = df_ship_base.rename(columns={
                    'proveedor': 'ProveedorMercancia', 'pais_fac': 'PaisFacturacion',
                    'fecha': 'FechaPagoReal', 'adua_sec_desp': 'SeccionAduanera',
                    'patente': 'Patente', 'pedimento': 'Pedimento',
                    'valor_dls': 'ValorDolares', 'valor_me': 'ValorMonedaExtranjera',
                    'cve_doc': 'ClaveDocumento',
                })
                df_ship_base['_source'] = 'Shippers'
                rows_base.append(df_ship_base)
                keys_resueltos = set(df_ship_base['_key'].unique())
                logger.info(f"   Shippers: {len(df_ship_base)} filas para {len(keys_resueltos)} pedimentos")

        # --- 505: fallback para pedimentos sin proveedor en Shippers ---
        keys_faltantes = keys_base - keys_resueltos

        if keys_faltantes and 'Proveedores' in sources:
            df_505 = sources['Proveedores'].copy()
            df_505['_key'] = build_pedimento_key(df_505)
            df_505['_taxid'] = df_505['IndentFiscalProveedor'].astype(str).str.strip().str.replace('-', '')
            df_505_match = df_505[df_505['_key'].isin(keys_faltantes)].copy()

            if not df_505_match.empty:
                agg_505 = {
                    'ProveedorMercancia': 'first', 'PaisFacturacion': 'first',
                    'FechaPagoReal': 'first', 'SeccionAduanera': 'first',
                    'Patente': 'first', 'Pedimento': 'first',
                    'ValorDolares': 'sum', 'ValorMonedaExtranjera': 'sum',
                }
                agg_505 = {k: v for k, v in agg_505.items() if k in df_505_match.columns}
                df_505_dedup = df_505_match.groupby(['_key', '_taxid'], sort=False).agg(agg_505).reset_index()
                df_505_dedup['_source'] = '505'
                rows_base.append(df_505_dedup)
                keys_from_505 = set(df_505_dedup['_key'].unique())
                keys_resueltos |= keys_from_505
                keys_faltantes -= keys_from_505
                logger.info(f"   505 fallback: {len(df_505_dedup)} filas para {len(keys_from_505)} pedimentos")

        # --- F3 → FORD MOTOR COMPANY / SIN ASIGNAR ---
        if keys_faltantes:
            falt_rows = []
            n_ford = 0
            n_sin = 0

            keys_in_invoice = set()
            if 'InvoiceReport' in sources:
                df_inv = sources['InvoiceReport'].copy()
                if 'PEDIMENTO' in df_inv.columns:
                    inv_parts = df_inv['PEDIMENTO'].astype(str).str.strip().str.split(r'\s+', expand=True)
                    if inv_parts.shape[1] >= 4:
                        inv_s = pd.to_numeric(inv_parts[1], errors='coerce').fillna(0).astype(int)
                        inv_p = pd.to_numeric(inv_parts[2], errors='coerce').fillna(0).astype(int)
                        inv_n = pd.to_numeric(inv_parts[3], errors='coerce').fillna(0).astype(int)
                        keys_in_invoice = set((inv_p.astype(str) + '|' + inv_n.astype(str) + '|' + inv_s.astype(str)).unique())

            for key in keys_faltantes:
                parts = key.split('|')
                clave_doc = clave_doc_map.get(key, '')
                if clave_doc == 'F3' and key not in keys_in_invoice:
                    taxid, nombre, pais = '380549190', 'FORD MOTOR COMPANY', 'USA'
                    n_ford += 1
                else:
                    taxid, nombre, pais = 'SIN ASIGNAR', 'SIN PROVEEDOR ASIGNADO', ''
                    n_sin += 1
                falt_rows.append({
                    '_key': key, '_taxid': taxid,
                    'ProveedorMercancia': nombre, 'PaisFacturacion': pais,
                    'SeccionAduanera': parts[2] if len(parts) >= 3 else '',
                    'Patente': parts[0] if len(parts) >= 1 else '',
                    'Pedimento': parts[1] if len(parts) >= 2 else '',
                    '_source': 'Fallback',
                })
            if falt_rows:
                rows_base.append(pd.DataFrame(falt_rows))
                logger.info(f"   Fallback: {n_ford} F3→FORD, {n_sin} SIN ASIGNAR")

        # === Combinar todas las fuentes ===
        if not rows_base:
            logger.error("   ❌ No se encontraron filas para la base de R2")
            return pd.DataFrame()

        df_base = pd.concat(rows_base, ignore_index=True)
        
        logger.info(f"   Base R2 total: {len(df_base)} filas ({len(df_base['_key'].unique())} pedimentos)")

        # Enriquecer con Destino de Shippers para filas que no lo tengan
        if 'Shippers' in sources:
            df_ship_full = sources['Shippers'].copy()
            df_ship_full['_key'] = build_pedimento_key(
                df_ship_full, col_patente='patente', col_pedimento='pedimento',
                col_seccion='adua_sec_desp')
            dest_map = df_ship_full[df_ship_full['Destino'].astype(str).str.strip().ne('')].drop_duplicates(
                subset='_key', keep='first')[['_key', 'Destino']]
            dest_map.columns = ['_key', '_destino_ship']
            df_base = df_base.merge(dest_map, on='_key', how='left')
            if 'Destino' in df_base.columns:
                mask_empty = df_base['Destino'].isna() | (df_base['Destino'].astype(str).str.strip() == '')
                df_base.loc[mask_empty, 'Destino'] = df_base.loc[mask_empty, '_destino_ship']
            else:
                df_base['Destino'] = df_base['_destino_ship']
            df_base.drop(columns=['_destino_ship'], inplace=True, errors='ignore')

        # Obtener FechaPagoReal del 501 para filas sin fecha
        if 'DatosGenerales' in sources:
            df_501_d = sources['DatosGenerales'].copy()
            df_501_d['_key'] = build_pedimento_key(df_501_d)
            fecha_map = df_501_d.drop_duplicates(subset='_key', keep='first')[['_key', 'FechaPagoReal']]
            fecha_map.columns = ['_key', '_fecha_501']
            df_base = df_base.merge(fecha_map, on='_key', how='left')
            if 'FechaPagoReal' not in df_base.columns:
                df_base['FechaPagoReal'] = df_base['_fecha_501']
            else:
                mask_nf = df_base['FechaPagoReal'].isna() | (df_base['FechaPagoReal'].astype(str).str.strip().isin(['', 'nan']))
                df_base.loc[mask_nf, 'FechaPagoReal'] = df_base.loc[mask_nf, '_fecha_501']
            df_base.drop(columns=['_fecha_501'], inplace=True, errors='ignore')

        # --- Contar proveedores ÚNICOS por pedimento (para prorrateo) ---
        rec_count = df_base.groupby('_key')['_key'].transform('count')
        df_base['_n_proveedores'] = rec_count

        # === Iniciar DataFrame de salida ===
        df = pd.DataFrame()

        # Extraemos la ClaveDocumento (necesaria internamente para cálculos de rectificaciones)
        # La renombramos de inmediato al nombre final para evitar colisiones
        df['Clave del Pedimento'] = df_base.get('ClaveDocumento', '')

        # 1. TaxID Proveedor
        df['TaxID Proveedor'] = df_base['_taxid'].values

        # 2-3. Código y Nombre Proveedor
        df['Código Proveedor'] = ''
        df['Nombre del Proveedor'] = df_base['ProveedorMercancia'].astype(str).str.strip().values
        


        # 4. País Vendedor — de 505, fallback SupplierReport → 551
        df['País Vendedor'] = df_base['PaisFacturacion'].astype(str).str.strip().values
        df.loc[df['País Vendedor'] == '', 'País Vendedor'] = pd.NA
        df.loc[df['País Vendedor'] == 'nan', 'País Vendedor'] = pd.NA

        # Guardar nombre normalizado para cruce SupplierReport
        df['_nombre_norm'] = df_base['ProveedorMercancia'].astype(str).str.strip().str.upper().values

        # Fallback 1: País desde SupplierReport
        if 'SupplierReport' in sources and df['País Vendedor'].isna().any():
            df_sup = sources['SupplierReport'].copy()
            df_sup['_nombre_norm'] = df_sup['Nombre'].astype(str).str.strip().str.upper()
            sup_pais = df_sup.drop_duplicates(subset='_nombre_norm', keep='first')[['_nombre_norm', 'Pais']]
            sup_pais.columns = ['_nombre_norm', '_pais_sup']
            df = df.merge(sup_pais, on='_nombre_norm', how='left')
            vacios_pais = df['País Vendedor'].isna()
            df.loc[vacios_pais, 'País Vendedor'] = df.loc[vacios_pais, '_pais_sup']
            filled_sup = vacios_pais.sum() - df['País Vendedor'].isna().sum()
            logger.info(f"   País Vendedor: fallback SupplierReport +{filled_sup} completados")
            df.drop(columns=['_pais_sup'], inplace=True, errors='ignore')

        # Fallback 2: País desde 551.PaisCompradorVendedor
        df['_key'] = df_base['_key'].values
        if 'Partidas' in sources and df['País Vendedor'].isna().any():
            df_551 = sources['Partidas'].copy()
            df_551['_key'] = build_pedimento_key(df_551)
            df_551['_pais_551'] = df_551['PaisCompradorVendedor'].astype(str).str.strip()
            pais_551 = df_551[df_551['_pais_551'].notna() & (df_551['_pais_551'] != '') & (df_551['_pais_551'] != 'nan')]
            pais_551_map = pais_551.drop_duplicates(subset='_key', keep='first')[['_key', '_pais_551']]
            df = df.merge(pais_551_map, on='_key', how='left')
            vacios_pais2 = df['País Vendedor'].isna()
            df.loc[vacios_pais2, 'País Vendedor'] = df.loc[vacios_pais2, '_pais_551']
            filled_551 = vacios_pais2.sum() - df['País Vendedor'].isna().sum()
            logger.info(f"   País Vendedor: fallback 551 +{filled_551} completados")
            df.drop(columns=['_pais_551'], inplace=True, errors='ignore')

        df['País Vendedor'] = df['País Vendedor'].fillna('')

        # 5. Nacionalidad (traducción ISO)
        df['Nacionalidad'] = df['País Vendedor'].map(PAISES_ISO).fillna(df['País Vendedor']).astype(str).str.upper()

        # Guardar metadata para cruces y prorrateo proporcional
        df['_n_proveedores'] = df_base['_n_proveedores'].values
        df['_valor_dls'] = pd.to_numeric(df_base.get('ValorDolares', 0), errors='coerce').fillna(0).values
        df['_valor_me'] = pd.to_numeric(df_base.get('ValorMonedaExtranjera', 0), errors='coerce').fillna(0).values

        # === 16-17. Clave Pedimento + Fecha Recepción + TIPO CAMBIO (del 501) ===
        # Se mueve antes de Base Gravable para tener el Tipo de Cambio disponible
        df = self._cruzar_501(df, sources)

        # === 6-8. Base Gravable, IVA Exceptuado, IVA 16% (SOLO FP=0) ===
        df = self._calcular_base_gravable(df, sources)

        # === 9-10. Prevalidación: para proveedores = 0, se asigna en CAAAREM ===
        df['Prevalidación MXN'] = 0.0
        df['IVA Prevalidación MXN'] = 0.0

        # 11. Año — 505 'FechaPagoReal' is typically 'YYYY-MM-DD' or 'DD/MM/YYYY'
        fecha_parsed = pd.to_datetime(df_base.get('FechaPagoReal', ''), format='mixed', errors='coerce')
        df['Año'] = fecha_parsed.dt.year.values

        # 12-14. Aduana, Patente, Pedimento
        df['Aduana'] = df_base['SeccionAduanera'].values
        df['Patente'] = df_base['Patente'].values
        df['Pedimento'] = df_base['Pedimento'].values

        # 15. Fecha de Pago (Data Stage)
        df['Fecha de Pago (Data Stage)'] = fecha_parsed.dt.strftime('%Y-%m-%d').values



        # === 18. Fecha Real de Pago Bancario (Póliza Contable) ===
        df = self._cruzar_fecha_pago_bancario(df, sources, fecha_parsed.values)

        # === 19. Clave Forma de Pago (solo considerar FP=0) ===
        df['Clave Forma de Pago'] = 'Forma de pago 0'

        # === 20. TIPO CAMBIO (del 501, ya cruzado arriba)

        # === 21-22. Valor Aduana DLLS y Valor Comercial DLLS ===
        df = self._calcular_valores_dlls(df, sources)

        # === 23-24. Total Pagado Impuestos (solo FP=0) ===
        df = self._calcular_total_impuestos(df, sources)

        # === 25. Tipo de Material (de Shippers Destino si existe, fallback aduana) ===
        if 'Destino' in df_base.columns:
            df['Tipo de Material'] = df_base['Destino'].astype(str).str.strip().values
        else:
            df['Tipo de Material'] = 'Sin clasificar'
        df.loc[df['Tipo de Material'].isin(['', 'nan', 'None', 'nan']), 'Tipo de Material'] = 'Sin clasificar'

        # Fallback: Destino más frecuente por aduana (de filas Shippers con Destino)
        sin_cls = df['Tipo de Material'] == 'Sin clasificar'
        if sin_cls.any() and 'Shippers' in sources:
            df_sh_full = sources['Shippers'].copy()
            dest_full = df_sh_full['Destino'].astype(str).str.strip()
            has_dest = (dest_full != '') & (dest_full != 'nan')
            if has_dest.any():
                aduana_full = pd.to_numeric(df_sh_full['adua_sec_desp'], errors='coerce').fillna(0).astype(int).astype(str)
                dest_freq = (pd.DataFrame({'aduana': aduana_full[has_dest], 'dest': dest_full[has_dest]})
                             .groupby('aduana')['dest']
                             .agg(lambda x: x.value_counts().index[0])
                             .to_dict())
                # Aduana de R2 = columna Aduana, normalizar como int
                aduana_r2 = pd.to_numeric(df['Aduana'], errors='coerce').fillna(0).astype(int).astype(str)
                fb = aduana_r2.map(dest_freq)
                resolved = sin_cls & fb.notna()
                df.loc[resolved, 'Tipo de Material'] = fb[resolved]
                logger.info(f"   Tipo de Material fallback aduana: {resolved.sum()}/{sin_cls.sum()} resueltos")

        # === 2. Código Proveedor (SupplierReport + fallback TaxID) ===
        df = self._cruzar_supplier_report(df, sources)

        # === 26. Tipo Operación (del 501) ===
        df = self._agregar_tipo_operacion(df, sources)

        # === Rectificaciones 701: se aplican DESPUÉS del filtro Póliza ===

        # === VALIDACIÓN: Completitud de Shippers (diagnóstico) ===
        self._validar_completitud_shippers(sources, df)

        # === CAAAREM: filas separadas para pedimentos con prevalidación ===
        df_caaarem = self._generar_filas_caaarem(sources, df.columns.tolist())
        if not df_caaarem.empty:
            df = pd.concat([df, df_caaarem], ignore_index=True)
            logger.info(f"   CAAAREM: +{len(df_caaarem)} filas añadidas")
        # === Último fallback Tipo de Material para CAAAREM/SIN PROVEEDOR ===
        sin_cls_final = df['Tipo de Material'].astype(str).str.strip() == 'Sin clasificar'
        if sin_cls_final.any() and 'Shippers' in sources:
            df_sh_full = sources['Shippers'].copy()
            dest_full = df_sh_full['Destino'].astype(str).str.strip()
            has_dest = (dest_full != '') & (dest_full != 'nan')
            if has_dest.any():
                aduana_full = pd.to_numeric(df_sh_full['adua_sec_desp'], errors='coerce').fillna(0).astype(int).astype(str)
                dest_freq = (pd.DataFrame({'aduana': aduana_full[has_dest], 'dest': dest_full[has_dest]})
                             .groupby('aduana')['dest']
                             .agg(lambda x: x.value_counts().index[0])
                             .to_dict())
                aduana_r2 = pd.to_numeric(df['Aduana'], errors='coerce').fillna(0).astype(int).astype(str)
                fb = aduana_r2.map(dest_freq)
                resolved = sin_cls_final & fb.notna()
                df.loc[resolved, 'Tipo de Material'] = fb[resolved]
                remaining = (df['Tipo de Material'].astype(str).str.strip() == 'Sin clasificar').sum()
                logger.info(f"   Tipo de Material fallback final: {resolved.sum()} resueltos, {remaining} restantes")

        # === FILTRO FINAL: SOLO IMPORTACIONES Y CAAAREM ===
        # CAAAREM se incluye SIEMPRE, incluso si el pedimento es de Exportación
        # Esto es porque Ford reporta el pago de prevalidación de exportaciones en el R2 (IVA por Proveedor)
        if 'Tipo Operación' in df.columns:
            n_antes = len(df)
            mask_type_1 = df['Tipo Operación'].astype(str).str.strip() == '1'
            mask_caaarem = df['TaxID Proveedor'].astype(str).str.strip() == 'CAAAREM'
            
            # Ford excluye en R2 todo lo que sea Exportación (Tipo Operación = 2) 
            # EXCEPTO las filas correspondientes al pago de CAAAREM (Prevalidación).
            # Entonces mantenemos todo lo que sea Importación (Tipo 1) O todo lo que sea CAAAREM.
            mask_keep = mask_type_1 | mask_caaarem
            df = df[mask_keep]
            n_despues = len(df)
            logger.info(f"   Filtro Importaciones (reteniendo CAAAREM): {n_antes} -> {n_despues} registros (se eliminaron {n_antes - n_despues} records)")

        # --- Limpiar columnas auxiliares ---
        cols_drop = [c for c in df.columns if c.startswith('_')]
        df.drop(columns=cols_drop, inplace=True)

        # --- Reordenar columnas al orden requerido ---
        col_order = [
            'TaxID Proveedor', 'Código Proveedor', 'Nombre del Proveedor',
            'País Vendedor', 'Nacionalidad',
            'Base Gravable MXN', 'Cálculo IVA Exceptuado', 'IVA al 16% MXN',
            'Prevalidación MXN', 'IVA Prevalidación MXN',
            'Año', 'Aduana', 'Patente', 'Pedimento',
            'Fecha de Pago (Data Stage)', 'Clave del Pedimento',
            'Fecha Recepción Pedimento', 'Fecha Real Pago (Bancario)',
            'Clave Forma de Pago', 'TIPO CAMBIO MXP',
            'Valor Aduana DLLS', 'Valor Comercial DLLS',
            'Total Pagado Impuestos MXP', 'Total Pagado Impuestos DLLS',
            'Tipo de Material', 'Tipo Operación', 'NOTAS',
        ]
        # Excluir ClaveDocumento del output
        cols_excluir = {'ClaveDocumento'}
        existing = [c for c in col_order if c in df.columns]
        extra = [c for c in df.columns if c not in col_order and c not in cols_excluir]
        df = df[existing + extra]

        # TIPO CAMBIO MXP: redondear a 4 decimales
        if 'TIPO CAMBIO MXP' in df.columns:
            df['TIPO CAMBIO MXP'] = pd.to_numeric(df['TIPO CAMBIO MXP'], errors='coerce').round(4)

        logger.info(f"✅ Reporte 2 pre-filtro: {len(df):,} registros, {len(df.columns)} columnas")

        # === FILTRO ESTADO DE CUENTA: Excluir pedimentos NO en Póliza Contable ===
        df_incluido, df_excluido = self._filtrar_por_poliza_contable(df, sources)

        # === PENDIENTES ANTERIORES: recuperar de BD y cruzar vs Póliza actual ===
        df_incluido = self._integrar_pendientes_anteriores(df_incluido, sources, mes_proceso)

        # === Resumen de cuadre CON exclusiones ===
        self._log_cuadre_con_exclusiones(df_incluido, df_excluido, sources)

        # === RECTIFICACIONES 701: marcar pedimentos rectificados (después de filtro) ===
        df_incluido = self._marcar_rectificaciones(df_incluido, sources)
        df_excluido = self._marcar_rectificaciones(df_excluido, sources)

        # === REGLAS ESPECÍFICAS DE FORD PARA EXCLUIDOS ===
        df_excluido = self._aplicar_reglas_ford_excluidos(df_excluido)

        # === RESUMEN FORD al final del R2 ===
        df_incluido = self._agregar_resumen_ford(df_incluido, df_excluido, sources)

        # === PERSISTIR nuevos pendientes en BD ===
        self._persistir_pendientes(df_excluido, mes_proceso)

        logger.info(f"✅ Reporte 2 FINAL: {len(df_incluido):,} filas (incluye resumen), {len(df_excluido):,} pendientes")

        return (df_incluido, df_excluido)

    # ------------------------------------------------------------------
    # Pendientes BD
    # ------------------------------------------------------------------

    def _integrar_pendientes_anteriores(self, df_incluido: pd.DataFrame,
                                         sources: dict, mes_proceso: str) -> pd.DataFrame:
        """
        Lee pendientes anteriores de BD (Utilizado=0), cruza sus pedimentos
        contra la Póliza Contable del mes actual, y agrega las filas completas
        al df_incluido si matchean.
        """
        if not mes_proceso:
            return df_incluido

        try:
            from src.db_pendientes import obtener_pendientes_anteriores, marcar_utilizados
        except Exception as e:
            logger.warning(f"   ⚠️ No se pudo importar db_pendientes: {e}")
            return df_incluido

        df_prev = obtener_pendientes_anteriores()
        if df_prev.empty:
            return df_incluido

        # Obtener keys de Póliza Contable (misma lógica que _filtrar_por_poliza_contable)
        if 'PolizaContable' not in sources:
            logger.warning("   ⚠️ PolizaContable no disponible, no se pueden resolver pendientes")
            return df_incluido

        df_pol = sources['PolizaContable'].copy()
        col_ped = None
        for c in df_pol.columns:
            if 'pedimento' in str(c).lower() and 'factura' in str(c).lower():
                col_ped = c
                break
        if col_ped is None:
            for c in df_pol.columns:
                if 'pedimento' in str(c).lower():
                    col_ped = c
                    break
        if col_ped is None:
            return df_incluido

        keys_poliza = set()
        for val in df_pol[col_ped].dropna().unique():
            parts = str(val).strip().split()
            if len(parts) >= 4:
                try:
                    patente = str(int(float(parts[2])))
                    pedimento = str(int(float(parts[3])))
                    keys_poliza.add(f"{patente}|{pedimento}")
                except (ValueError, IndexError):
                    continue

        # Cruzar pendientes vs Póliza actual
        pat_prev = pd.to_numeric(df_prev['Patente'], errors='coerce').fillna(0).astype(int).astype(str)
        ped_prev = pd.to_numeric(df_prev['Pedimento'], errors='coerce').fillna(0).astype(int).astype(str)
        df_prev['_key'] = pat_prev + '|' + ped_prev

        mask_match = df_prev['_key'].isin(keys_poliza)
        df_matched = df_prev[mask_match].copy()

        if df_matched.empty:
            logger.info("   Sin pendientes anteriores resueltos en este mes")
            return df_incluido

        # Obtener keys únicas para marcar en BD
        matched_keys = set()
        for _, row in df_matched.iterrows():
            matched_keys.add((
                str(row.get('Patente', '')).strip(),
                str(row.get('Pedimento', '')).strip(),
                str(row.get('Aduana', '')).strip()
            ))

        # Preparar filas para concat (quitar columnas auxiliares)
        df_matched = df_matched.drop(columns=['_key', 'MesOrigen'], errors='ignore')

        # Alinear columnas con df_incluido
        for col in df_incluido.columns:
            if col not in df_matched.columns:
                df_matched[col] = ''
        df_matched = df_matched[df_incluido.columns]

        # Convertir tipos numéricos
        for col in ['Base Gravable MXN', 'IVA al 16% MXN', 'TIPO CAMBIO MXP',
                     'Prevalidación MXN', 'IVA Prevalidación MXN',
                     'Valor Aduana DLLS', 'Valor Comercial DLLS',
                     'Total Pagado Impuestos MXP', 'Total Pagado Impuestos DLLS']:
            if col in df_matched.columns:
                df_matched[col] = pd.to_numeric(df_matched[col], errors='coerce').fillna(0)

        logger.info(f"   🔄 {len(df_matched)} filas de pendientes anteriores incluidas en R2 ({len(matched_keys)} pedimentos)")

        # Marcar como utilizados en BD
        marcar_utilizados(list(matched_keys), mes_proceso)

        # Agregar al incluido
        df_incluido = pd.concat([df_incluido, df_matched], ignore_index=True)
        return df_incluido

    def _persistir_pendientes(self, df_excluido: pd.DataFrame, mes_proceso: str):
        """Inserta los nuevos excluidos del mes actual en BD."""
        if not mes_proceso or df_excluido.empty:
            return

        try:
            from src.db_pendientes import insertar_pendientes
            insertar_pendientes(df_excluido, mes_proceso)
        except Exception as e:
            logger.warning(f"   ⚠️ No se pudieron persistir pendientes: {e}")

    # ------------------------------------------------------------------
    # Métodos helper
    # ------------------------------------------------------------------

    def _calcular_base_gravable(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        LÓGICA FORD (FOM-MEX-041):
        - Prorrateo PROPORCIONAL por Valor Comercial DLLS (no equitativo)
        - Base Gravable = (Valor Aduana DLLS × Tipo Cambio) + DTA
        - IVA al 16% = Base Gravable × 0.16
        - IVA Exceptuado = (Base Gravable × 0.16) - IVA real pagado
        """

        try:
            # === PASO 1: Calcular proporción de cada proveedor en el pedimento ===
            # Usar valor_dls (Valor Comercial DLLS), fallback a valor_me, fallback a equitativo

            # Total de valor comercial por pedimento
            df['_valor_comercial'] = df['_valor_dls'].copy()
            # Fallback a valor_me si valor_dls = 0
            sin_dls = df['_valor_comercial'] == 0
            if sin_dls.any():
                df.loc[sin_dls, '_valor_comercial'] = df.loc[sin_dls, '_valor_me']

            # Calcular total por pedimento
            total_por_ped = df.groupby('_key')['_valor_comercial'].transform('sum')

            # Calcular proporción de cada proveedor
            df['_proporcion'] = 0.0
            tiene_valor = total_por_ped > 0
            df.loc[tiene_valor, '_proporcion'] = df.loc[tiene_valor, '_valor_comercial'] / total_por_ped[tiene_valor]

            # Fallback equitativo si no hay valores comerciales
            sin_valor = ~tiene_valor
            if sin_valor.any():
                df.loc[sin_valor, '_proporcion'] = 1.0 / df.loc[sin_valor, '_n_proveedores']
                logger.info(f"   Prorrateo equitativo (sin valores): {sin_valor.sum()} registros")

            logger.info(f"   Prorrateo proporcional por Valor Comercial DLLS: {tiene_valor.sum()} registros")

            # === PASO 2: Valor Aduana MXN del 551 ===
            if 'Partidas' in sources:
                df_551 = sources['Partidas'].copy()
                df_551['_key'] = build_pedimento_key(df_551)

                # Valor Aduana en MXN (columna ValorAduana de 551)
                col_va_mxn = 'ValorAduana'
                if col_va_mxn not in df_551.columns:
                    for c in df_551.columns:
                        if 'aduana' in str(c).lower() and 'valor' in str(c).lower():
                            col_va_mxn = c
                            break

                df_551[col_va_mxn] = pd.to_numeric(df_551[col_va_mxn], errors='coerce').fillna(0)
                va_mxn_sum = df_551.groupby('_key')[col_va_mxn].sum().reset_index()
                va_mxn_sum.columns = ['_key', '_valor_aduana_mxn_ped']
                df = df.merge(va_mxn_sum, on='_key', how='left')
                df['_valor_aduana_mxn_ped'] = df['_valor_aduana_mxn_ped'].fillna(0)
                logger.info(f"   551 Valor Aduana MXN: total={df['_valor_aduana_mxn_ped'].sum():,.2f}")
            else:
                df['_valor_aduana_mxn_ped'] = 0
                logger.warning("   ⚠️ 551 no disponible para Valor Aduana")

            # === PASO 3: DTA del 510 y 702 (CC=1, SOLO FP=0) ===
            # Para pedimentos normales, DTA viene del 510. Para rectificaciones, las diferencias vienen del 702.
            # DTA de 510 (Normal)
            df_510_dta = pd.DataFrame()
            if 'ContribucionesPedimento' in sources:
                df_510 = sources['ContribucionesPedimento'].copy()
                df_510['_key'] = build_pedimento_key(df_510)
                df_510['ImportePago'] = pd.to_numeric(df_510['ImportePago'], errors='coerce').fillna(0)
                df_510['ClaveContribucion'] = df_510['ClaveContribucion'].astype(str).str.strip()
                df_510['FormaPago'] = df_510['FormaPago'].astype(str).str.strip()
                df_510_dta = df_510[(df_510['ClaveContribucion'] == '1') & (df_510['FormaPago'] == '0')]

            # DTA de 702 (Rectificación)
            df_702_dta = pd.DataFrame()
            if 'RectificacionesDetalle' in sources:
                df_702 = sources['RectificacionesDetalle'].copy()
                df_702['_key'] = build_pedimento_key(df_702)
                df_702['ImportePago'] = pd.to_numeric(df_702['ImportePago'], errors='coerce').fillna(0)
                df_702['ClaveContribucion'] = df_702['ClaveContribucion'].astype(str).str.strip()
                df_702['FormaPago'] = df_702['FormaPago'].astype(str).str.strip()
                df_702_dta = df_702[(df_702['ClaveContribucion'] == '1') & (df_702['FormaPago'] == '0')]

            # Mergear ambos por separado al df principal
            if not df_510_dta.empty:
                dta_510_sum = df_510_dta.groupby('_key')['ImportePago'].sum().reset_index()
                dta_510_sum.columns = ['_key', '_dta_510']
                df = df.merge(dta_510_sum, on='_key', how='left')
            
            if '_dta_510' not in df.columns:
                df['_dta_510'] = 0.0
            else:
                df['_dta_510'] = df['_dta_510'].fillna(0.0)

            if not df_702_dta.empty:
                dta_702_sum = df_702_dta.groupby('_key')['ImportePago'].sum().reset_index()
                dta_702_sum.columns = ['_key', '_dta_702']
                df = df.merge(dta_702_sum, on='_key', how='left')
            
            if '_dta_702' not in df.columns:
                df['_dta_702'] = 0.0
            else:
                df['_dta_702'] = df['_dta_702'].fillna(0.0)

            # Para compatibilidad con el resto del script, _dta_ped es el total
            df['_dta_ped'] = df['_dta_510'] + df['_dta_702']

            # ADV de 557 (Normal)
            df_557_adv_sum = pd.DataFrame()
            if 'ContribucionesPartida' in sources:
                df_557 = sources['ContribucionesPartida'].copy()
                df_557['_key'] = build_pedimento_key(df_557)
                df_557['ImportePago'] = pd.to_numeric(df_557['ImportePago'], errors='coerce').fillna(0)
                df_557['ClaveContribucion'] = df_557['ClaveContribucion'].astype(str).str.strip()
                df_557['FormaPago'] = df_557['FormaPago'].astype(str).str.strip()
                df_557_adv = df_557[(df_557['ClaveContribucion'] == '6') & (df_557['FormaPago'] == '0')]
                if not df_557_adv.empty:
                    df_557_adv_sum = df_557_adv.groupby('_key')['ImportePago'].sum().reset_index()
                    df_557_adv_sum.columns = ['_key', '_adv_557']
                    df = df.merge(df_557_adv_sum, on='_key', how='left')
            
            if '_adv_557' not in df.columns:
                df['_adv_557'] = 0.0
            else:
                df['_adv_557'] = df['_adv_557'].fillna(0.0)

            # ADV de 702 (Rectificación)
            df_702_adv_sum = pd.DataFrame()
            if 'RectificacionesDetalle' in sources:
                df_702 = sources['RectificacionesDetalle'].copy()
                df_702['_key'] = build_pedimento_key(df_702)
                df_702['ImportePago'] = pd.to_numeric(df_702['ImportePago'], errors='coerce').fillna(0)
                df_702['ClaveContribucion'] = df_702['ClaveContribucion'].astype(str).str.strip()
                df_702['FormaPago'] = df_702['FormaPago'].astype(str).str.strip()
                df_702_adv = df_702[(df_702['ClaveContribucion'] == '6') & (df_702['FormaPago'] == '0')]
                if not df_702_adv.empty:
                    df_702_adv_sum = df_702_adv.groupby('_key')['ImportePago'].sum().reset_index()
                    df_702_adv_sum.columns = ['_key', '_adv_702']
                    df = df.merge(df_702_adv_sum, on='_key', how='left')
            
            if '_adv_702' not in df.columns:
                df['_adv_702'] = 0.0
            else:
                df['_adv_702'] = df['_adv_702'].fillna(0.0)

            # Total ADV para compatibilidad
            df['_adv_ped'] = df['_adv_557'] + df['_adv_702']

            # IVA REAL (Necesario para Cálculo IVA Exceptuado)
            # Combinamos IVA de 557 y 702
            iva_dfs = []
            if 'ContribucionesPartida' in sources:
                iva_557 = df_557[(df_557['ClaveContribucion'] == '3') & (df_557['FormaPago'] == '0')]
                iva_dfs.append(iva_557)
            if 'RectificacionesDetalle' in sources:
                iva_702 = df_702[(df_702['ClaveContribucion'] == '3') & (df_702['FormaPago'] == '0')]
                iva_dfs.append(iva_702)

            if iva_dfs:
                iva_combined = pd.concat(iva_dfs, ignore_index=True)
                iva_sum = iva_combined.groupby('_key')['ImportePago'].sum().reset_index()
                iva_sum.columns = ['_key', '_iva_real_ped']
                df = df.merge(iva_sum, on='_key', how='left')
            
            if '_iva_real_ped' not in df.columns:
                df['_iva_real_ped'] = 0.0
            else:
                df['_iva_real_ped'] = df['_iva_real_ped'].fillna(0.0)

            # === PASO 5: Calcular Base Gravable según FORD (Ajuste Solicitado) ===
            # Fórmula: Base Gravable = Valor Aduana + DTA + ADV
            # donde ADV = ClaveContribucion 6 (IGI/Ajuste de Valoración) del 557
            
            tc = pd.to_numeric(df.get('TIPO CAMBIO MXP', 1), errors='coerce').fillna(1).replace(0, 1)
            
            # Aplicar proporción del proveedor a todos los componentes
            # Usar _valor_aduana_mxn_ped directamente (ya está en MXN del 551)
            # sin conversión MXN→USD→MXN que introduce error de punto flotante
            base_va = df['_valor_aduana_mxn_ped'] * df['_proporcion']
            base_dta = df['_dta_ped'] * df['_proporcion']
            base_adv = df['_adv_ped'] * df['_proporcion']

            # Base Gravable = VA + DTA + ADV
            df['Base Gravable MXN'] = base_va + base_dta + base_adv

            # === PASO 6: IVA al 16% = Base Gravable × 0.16 (según FORD) ===
            df['IVA al 16% MXN'] = df['Base Gravable MXN'] * 0.16

            # === FILTRO CRÍTICO DE CUADRE ===
            # Si NO hubo pago real de IVA (557 CC=3) en el pedimento, la Base Gravable para IVA 16% debe ser 0.
            # Esto elimina operaciones Tasa 0% o Exentas que tienen Valor Aduana pero no causan IVA 16%.
            mask_iva_real_zero = df['_iva_real_ped'] == 0
            n_filtered = mask_iva_real_zero.sum()
            if n_filtered > 0:
                 logger.info(f"   Filtro Tasa 0%/Exentos: {n_filtered} registros con IVA Real=0 -> Base=0")
                 df.loc[mask_iva_real_zero, 'Base Gravable MXN'] = 0
                 df.loc[mask_iva_real_zero, 'IVA al 16% MXN'] = 0

            # Filtro adicional: Si Base Gravable = 0, IVA debe ser 0 (limpieza)
            mask_bg_zero = df['Base Gravable MXN'] == 0
            df.loc[mask_bg_zero, 'IVA al 16% MXN'] = 0

            # === PASO 7: IVA Exceptuado a nivel PEDIMENTO (Ford) ===
            # Ford calcula BG_ped × 0.16 - IVA_ped a nivel pedimento.
            # Solo asigna el exceptuado cuando |Exc| > 100 MXN (excepciones reales,
            # no diferencias de redondeo). Lo asigna a la fila con mayor mismatch
            # BG×0.16 - IVA a nivel de fila.
            df['Cálculo IVA Exceptuado'] = 0.0

            # Calcular Exceptuado a nivel pedimento usando valores crudos (antes de prorrateo)
            # BG_ped = VA_ped + DTA_ped + ADV_ped (mismo por todas las filas del pedimento)
            # Solo para pedimentos CON IVA real (excluir Tasa 0%/Exentos)
            bg_ped_raw = df['_valor_aduana_mxn_ped'] + df['_dta_ped'] + df['_adv_ped']
            iva_ped_teorico = bg_ped_raw * 0.16
            exc_ped = pd.Series(0.0, index=df.index)
            mask_con_iva = df['_iva_real_ped'] > 0
            exc_ped[mask_con_iva] = iva_ped_teorico[mask_con_iva] - df.loc[mask_con_iva, '_iva_real_ped']

            # Calcular mismatch a nivel de fila (para saber a cuál asignar)
            iva_real_prorr = df['_iva_real_ped'] * df['_proporcion']
            row_mismatch = df['IVA al 16% MXN'] - iva_real_prorr

            # Solo pedimentos con Exc > 100 MXN (excepciones reales, positivas)
            # Negativo = IVA pagado > BG teórico, típico de rectificaciones → se excluye
            mask_significativo = exc_ped > 100

            if mask_significativo.any():
                # Para cada pedimento significativo, asignar a la fila con mayor mismatch
                for key in df.loc[mask_significativo, '_key'].unique():
                    mask_key = df['_key'] == key
                    ped_exc_val = exc_ped[mask_key].iloc[0]
                    # Encontrar la fila con mayor mismatch absoluto
                    mismatches = row_mismatch[mask_key].abs()
                    best_idx = mismatches.idxmax()
                    df.at[best_idx, 'Cálculo IVA Exceptuado'] = ped_exc_val

                n_exc = len(df.loc[mask_significativo, '_key'].unique())
                total_exc = df['Cálculo IVA Exceptuado'].sum()
                logger.info(f"   IVA Exceptuado: {n_exc} pedimentos con excepciones reales, total={total_exc:,.2f}")

            matched = (df['Base Gravable MXN'] > 0).sum()
            iva_ok = (df['IVA al 16% MXN'] > 0).sum()
            logger.info(f"   Base Gravable (FORD): {matched}/{len(df)} con valor > 0")
            logger.info(f"   IVA al 16% (BG×0.16): {iva_ok}/{len(df)} con valor > 0, total={df['IVA al 16% MXN'].sum():,.2f}")
            logger.info(f"   IVA real DS557 (ref): total={df['_iva_real_ped'].sum():,.2f}")
            
        except Exception as e:
            logger.exception(f"❌ Error CRÍTICO en _calcular_base_gravable: {e}")
            raise e
            
            # Trace 5003131 FINAL CHECK
            trace_mask = df['_key'].astype(str).str.contains('5003131')
            if trace_mask.any():
                row_trace = df[trace_mask].iloc[0]
                logger.info(f"   🕵️ TRACE 5003131 END: Key='{row_trace.get('_key')}', IVA={row_trace.get('_iva_real_ped')}, Base={row_trace.get('Base Gravable MXN')}, Exceptuado={row_trace.get('Cálculo IVA Exceptuado')}, CalcIVA={row_trace.get('IVA al 16% MXN')}")
            else:
                logger.warning("   🕵️ TRACE 5003131 END: NOT FOUND in df")

        return df

    def _generar_filas_caaarem(self, sources: dict, columnas: list) -> pd.DataFrame:
        """
        Genera filas CAAAREM separadas para cada pedimento con prevalidación
        en el 510 (ClaveContribucion=15/23, FormaPago=0).
        Una fila por pedimento con prevalidación.
        """
        if 'ContribucionesPedimento' not in sources:
            logger.warning("   ⚠️ 510 no disponible para CAAAREM")
            return pd.DataFrame(columns=columnas)

        df_510 = sources['ContribucionesPedimento'].copy()
        df_510['_key'] = build_pedimento_key(df_510)
        df_510['ImportePago'] = pd.to_numeric(df_510['ImportePago'], errors='coerce').fillna(0)
        df_510['ClaveContribucion'] = df_510['ClaveContribucion'].astype(str).str.strip()
        df_510['FormaPago'] = df_510['FormaPago'].astype(str).str.strip()

        # Prevalidación: Ford usa valor fijo de 330 MXN por pedimento
        # Identificar pedimentos que tienen prevalidación (CC=15 FP=0 en 510)
        prev = df_510[(df_510['ClaveContribucion'] == '15') & (df_510['FormaPago'] == '0')]
        prev_keys = prev.groupby('_key')['ImportePago'].sum().reset_index()
        prev_keys.columns = ['_key', '_prev_raw']
        # Ford siempre usa 330 para Prev y 330*0.16=52.80 para IVA Prev
        prev_keys['Prevalidación MXN'] = 330.0
        prev_keys['IVA Prevalidación MXN'] = 330.0 * 0.16  # = 52.80

        # Merge
        df_caa = prev_keys[['_key', 'Prevalidación MXN', 'IVA Prevalidación MXN']].copy()

        if df_caa.empty:
            logger.info("   CAAAREM: 0 pedimentos con prevalidación")
            return pd.DataFrame(columns=columnas)

        logger.info(f"   CAAAREM pedimentos: {len(df_caa)}, Prev total={df_caa['Prevalidación MXN'].sum():,.2f}")

        # Metadata del 501
        if 'DatosGenerales' in sources:
            df_501 = sources['DatosGenerales'].copy()
            df_501['_key'] = build_pedimento_key(df_501)
            meta_cols = {
                'SeccionAduanera': 'Aduana',
                'Patente': 'Patente',
                'Pedimento': 'Pedimento',
                'TipoCambio': 'TIPO CAMBIO MXP',
                'TipoOperacion': 'Tipo Operación',
            }
            cols_exist = {k: v for k, v in meta_cols.items() if k in df_501.columns}
            map_501 = df_501[['_key'] + list(cols_exist.keys())].drop_duplicates(subset='_key', keep='first')
            map_501.rename(columns=cols_exist, inplace=True)
            df_caa = df_caa.merge(map_501, on='_key', how='left')

            # Fecha de Pago
            if 'FechaPagoReal' in df_501.columns:
                fecha_map = df_501[['_key', 'FechaPagoReal']].drop_duplicates(subset='_key', keep='first')
                df_caa = df_caa.merge(fecha_map, on='_key', how='left')
                fecha_parsed = pd.to_datetime(df_caa['FechaPagoReal'], format='mixed', errors='coerce')
                df_caa['Fecha de Pago (Data Stage)'] = fecha_parsed.dt.strftime('%Y-%m-%d')
                df_caa['Año'] = fecha_parsed.dt.year
                df_caa.drop(columns=['FechaPagoReal'], inplace=True, errors='ignore')

            # Clave del Pedimento
            if 'ClaveDocumento' in df_501.columns:
                clave_map = df_501[['_key', 'ClaveDocumento']].drop_duplicates(subset='_key', keep='first')
                clave_map.rename(columns={'ClaveDocumento': 'Clave del Pedimento'}, inplace=True)
                df_caa = df_caa.merge(clave_map, on='_key', how='left')

            # Fecha Recepción
            if 'FechaRecepcionPedimento' in df_501.columns:
                frec_map = df_501[['_key', 'FechaRecepcionPedimento']].drop_duplicates(subset='_key', keep='first')
                frec_map.rename(columns={'FechaRecepcionPedimento': 'Fecha Recepción Pedimento'}, inplace=True)
                df_caa = df_caa.merge(frec_map, on='_key', how='left')

        # CAAAREM identifiers
        df_caa['TaxID Proveedor'] = 'CAAAREM'
        df_caa['Código Proveedor'] = 'CAAAREM'
        df_caa['Nombre del Proveedor'] = 'CAAAREM'
        df_caa['País Vendedor'] = 'CAAAREM'
        df_caa['Nacionalidad'] = 'CAAAREM'

        # Zero columns for CAAAREM
        df_caa['Base Gravable MXN'] = 0
        df_caa['Cálculo IVA Exceptuado'] = 0
        df_caa['IVA al 16% MXN'] = 0
        df_caa['Valor Aduana DLLS'] = 0
        df_caa['Valor Comercial DLLS'] = 0
        df_caa['Clave Forma de Pago'] = 'Forma de pago 0'
        df_caa['Fecha Real Pago (Bancario)'] = ''

        # Total Pagado = prevalidación + IVA prevalidación
        df_caa['Total Pagado Impuestos MXP'] = df_caa['Prevalidación MXN'] + df_caa['IVA Prevalidación MXN']
        tc = pd.to_numeric(df_caa.get('TIPO CAMBIO MXP', 1), errors='coerce').fillna(1).replace(0, 1)
        df_caa['Total Pagado Impuestos DLLS'] = df_caa['Total Pagado Impuestos MXP'] / tc

        # Tipo de Material desde Shippers
        if 'Shippers' in sources:
            df_ship = sources['Shippers'].copy()
            needed = ['patente', 'pedimento', 'adua_sec_desp', 'Destino']
            if all(c in df_ship.columns for c in needed):
                df_ship['_key'] = build_pedimento_key(
                    df_ship, col_patente='patente', col_pedimento='pedimento',
                    col_seccion='adua_sec_desp')
                ship_map = df_ship[df_ship['Destino'].astype(str).str.strip() != '']
                ship_map = ship_map.drop_duplicates(subset='_key', keep='first')[['_key', 'Destino']]
                ship_map.columns = ['_key', 'Tipo de Material']
                df_caa = df_caa.merge(ship_map, on='_key', how='left')
        if 'Tipo de Material' not in df_caa.columns:
            df_caa['Tipo de Material'] = 'Sin clasificar'
        df_caa['Tipo de Material'] = df_caa['Tipo de Material'].fillna('Sin clasificar')

        # Limpiar _key
        df_caa.drop(columns=['_key'], inplace=True, errors='ignore')

        # Asegurar todas las columnas finales
        for col in columnas:
            if col not in df_caa.columns:
                df_caa[col] = ''

        return df_caa[columnas]

    def _aplicar_reglas_ford_excluidos(self, df_exc: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica las lógicas atípicas que Ford emplea específicamente en su pestaña de excluidos:
        1. Para pedimentos normales (no R1), el Valor Aduana y Comercial se repite
           íntegramente en CAAAREM (duplicando el valor en vez de dejarlo en 0).
        2. El 'Total Pagado Impuestos MXP' para proveedores se calcula como Base Gravable + IVA,
           en vez de solo el IVA.
        """
        if df_exc.empty:
            return df_exc

        df_exc['Fecha Real Pago (Bancario)'] = 'NO SE ENCONTRO EN ESTADO DE CUENTA'

        is_caaarem = df_exc['TaxID Proveedor'].astype(str).str.strip() == 'CAAAREM'
        is_r1 = df_exc['Clave del Pedimento'].astype(str).str.strip() == 'R1'
        normal_mask = ~is_r1

        # 1. Ajustar 'Total Pagado Impuestos MXP' para filas de datos normales (no CAAAREM ni R1)
        bg = pd.to_numeric(df_exc['Base Gravable MXN'], errors='coerce').fillna(0)
        iva = pd.to_numeric(df_exc['IVA al 16% MXN'], errors='coerce').fillna(0)
        mask_ajuste = ~is_caaarem & normal_mask
        df_exc.loc[mask_ajuste, 'Total Pagado Impuestos MXP'] = bg[mask_ajuste] + iva[mask_ajuste]

        # 2. Duplicar VA y VC para los CAAAREM de pedimentos normales
        df_exc['_ped_str'] = pd.to_numeric(df_exc['Pedimento'], errors='coerce').fillna(0).astype(int).astype(str)
        va_por_ped = df_exc[normal_mask & ~is_caaarem].groupby('_ped_str')['Valor Aduana DLLS'].sum()
        vc_por_ped = df_exc[normal_mask & ~is_caaarem].groupby('_ped_str')['Valor Comercial DLLS'].sum()

        caaarem_normal_idx = df_exc[normal_mask & is_caaarem].index
        for idx in caaarem_normal_idx:
            ped = df_exc.loc[idx, '_ped_str']
            if ped in va_por_ped:
                df_exc.loc[idx, 'Valor Aduana DLLS'] = va_por_ped[ped]
            if ped in vc_por_ped:
                df_exc.loc[idx, 'Valor Comercial DLLS'] = vc_por_ped[ped]
        
        df_exc.drop(columns=['_ped_str'], inplace=True, errors='ignore')

        # Recalcular Total DLLS tras sumar Base Gravable
        tc = pd.to_numeric(df_exc['TIPO CAMBIO MXP'], errors='coerce').fillna(1).replace(0, 1)
        df_exc['Total Pagado Impuestos DLLS'] = (df_exc['Total Pagado Impuestos MXP'] / tc).round(2)

        return df_exc

    def _generar_filas_sin_proveedor(self, sources: dict, df_actual: pd.DataFrame) -> pd.DataFrame:
        """
        Para pedimentos que tienen IVA pagado en 557 (clave=6, FP=0)
        pero NO tienen proveedor en Shippers, genera una fila con
        'SIN PROVEEDOR ASIGNADO' para que el IVA total cuadre con DataStage.
        """
        if 'ContribucionesPartida' not in sources or 'DatosGenerales' not in sources:
            return pd.DataFrame()

        # Keys ya cubiertos en R2 (proveedores + CAAAREM)
        # Reconstruir _key en formato build_pedimento_key: Patente|Pedimento|SeccionAduanera
        df_tmp = df_actual.copy()
        seccion_norm = pd.to_numeric(df_tmp['Aduana'], errors='coerce').fillna(0).astype(int).astype(str)
        patente = pd.to_numeric(df_tmp['Patente'], errors='coerce').fillna(0).astype(int).astype(str)
        pedimento = pd.to_numeric(df_tmp['Pedimento'], errors='coerce').fillna(0).astype(int).astype(str)
        df_tmp['_key_check'] = (
            patente + '|' +
            pedimento + '|' +
            seccion_norm
        )
        keys_cubiertos = set(df_tmp['_key_check'].unique())

        # Todos los pedimentos con IVA (557 CC=3, FP=0)
        df_557 = sources['ContribucionesPartida'].copy()
        df_557['_key'] = build_pedimento_key(df_557)
        df_557['ImportePago'] = pd.to_numeric(df_557['ImportePago'], errors='coerce').fillna(0)
        df_557['ClaveContribucion'] = df_557['ClaveContribucion'].astype(str).str.strip()
        df_557['FormaPago'] = df_557['FormaPago'].astype(str).str.strip()
        iva_fp0 = df_557[(df_557['FormaPago'] == '0') & (df_557['ClaveContribucion'] == '3')]
        iva_por_ped = iva_fp0.groupby('_key')['ImportePago'].sum().reset_index()
        iva_por_ped.columns = ['_key', '_iva_total']
        iva_por_ped = iva_por_ped[iva_por_ped['_iva_total'] > 0]

        # Pedimentos con IVA pero NO cubiertos
        keys_con_iva = set(iva_por_ped['_key'])
        keys_faltantes = keys_con_iva - keys_cubiertos

        if not keys_faltantes:
            logger.info("   SIN PROVEEDOR: 0 pedimentos faltantes")
            return pd.DataFrame()

        # Calcular BG según FORD: (Valor Aduana DLLS × TC) + DTA = Valor Aduana MXN + DTA
        df_falt = iva_por_ped[iva_por_ped['_key'].isin(keys_faltantes)].copy()

        # Valor Aduana MXN y Valor Comercial DLLS del 551
        if 'Partidas' in sources:
            df_551 = sources['Partidas'].copy()
            df_551['_key'] = build_pedimento_key(df_551)

            # Valor Aduana en MXN
            col_va_mxn = 'ValorAduana'
            if col_va_mxn not in df_551.columns:
                for c in df_551.columns:
                    if 'aduana' in str(c).lower() and 'valor' in str(c).lower():
                        col_va_mxn = c
                        break
            df_551[col_va_mxn] = pd.to_numeric(df_551[col_va_mxn], errors='coerce').fillna(0)
            va_sum = df_551.groupby('_key')[col_va_mxn].sum().reset_index()
            va_sum.columns = ['_key', '_va_mxn']
            df_falt = df_falt.merge(va_sum, on='_key', how='left')
        df_falt['_va_mxn'] = df_falt.get('_va_mxn', pd.Series(dtype=float)).fillna(0)

        # Contribuciones
        if 'ContribucionesPedimento' in sources:
            df_510 = sources['ContribucionesPedimento'].copy()
            df_510['_key'] = build_pedimento_key(df_510)
            df_510['ImportePago'] = pd.to_numeric(df_510['ImportePago'], errors='coerce').fillna(0)
            dta = df_510[(df_510['ClaveContribucion'].astype(str) == '1') & (df_510['FormaPago'].astype(str) == '0')]
            dta_sum = dta.groupby('_key')['ImportePago'].sum().reset_index()
            dta_sum.columns = ['_key', '_dta']
            df_falt = df_falt.merge(dta_sum, on='_key', how='left')
        df_falt['_dta'] = df_falt.get('_dta', pd.Series(dtype=float)).fillna(0)

        df_falt['Base Gravable MXN'] = df_falt['_va_mxn'] + df_falt['_dta']
        df_falt['IVA al 16% MXN'] = df_falt['_iva_total']
        df_falt['TaxID Proveedor'] = 'SIN ASIGNAR'
        df_falt['Nombre del Proveedor'] = 'SIN PROVEEDOR ASIGNADO'
        df_falt['País Vendedor'] = ''
        df_falt['Nacionalidad'] = ''
        df_falt['Fecha Real Pago (Bancario)'] = 'NO SE ENCONTRO EN ESTADO DE CUENTA'
        
        # Metadata
        parts = df_falt['_key'].str.split('|', expand=True)
        df_falt['Patente'] = parts[0]
        df_falt['Pedimento'] = parts[1]
        df_falt['Aduana'] = parts[2]
        
        # Eliminar auxiliares
        cols_drop = [c for c in df_falt.columns if c.startswith('_')]
        df_falt.drop(columns=cols_drop, inplace=True, errors='ignore')
        
        # Alinear columnas
        for col in df_actual.columns:
            if col not in df_falt.columns:
                df_falt[col] = ''
        
        return df_falt[df_actual.columns]

    def _cruzar_501(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        if 'DatosGenerales' not in sources:
            return df
        df_501 = sources['DatosGenerales'].copy()
        df_501['_key'] = build_pedimento_key(df_501)
        meta = df_501.drop_duplicates(subset='_key', keep='first')
        cols = ['_key', 'ClaveDocumento', 'FechaRecepcionPedimento', 'TipoCambio']
        cols = [c for c in cols if c in meta.columns]
        df = df.merge(meta[cols], on='_key', how='left')
        if 'ClaveDocumento' in df.columns:
            if 'Clave del Pedimento' in df.columns:
                mask = df['Clave del Pedimento'].isna() | (df['Clave del Pedimento'] == '')
                df.loc[mask, 'Clave del Pedimento'] = df.loc[mask, 'ClaveDocumento']
            else:
                df.rename(columns={'ClaveDocumento': 'Clave del Pedimento'}, inplace=True)
        if 'FechaRecepcionPedimento' in df.columns:
            df.rename(columns={'FechaRecepcionPedimento': 'Fecha Recepción Pedimento'}, inplace=True)
        if 'TipoCambio' in df.columns:
            df.rename(columns={'TipoCambio': 'TIPO CAMBIO MXP'}, inplace=True)
        return df

    def _cruzar_fecha_pago_bancario(self, df: pd.DataFrame, sources: dict, fecha_values) -> pd.DataFrame:
        df['Fecha Real Pago (Bancario)'] = ''
        
        # Primero, intentar sacar la fecha de la Póliza Contable (hoja IMP)
        if 'PolizaContable' in sources:
            df_pol = sources['PolizaContable'].copy()
            if 'Pedimentos / Factura' in df_pol.columns and 'Fecha de Pago' in df_pol.columns:
                # Extraer pedimento de la cadena (ej: "26 16 3949 5004814" -> "5004814")
                df_pol['_ped'] = df_pol['Pedimentos / Factura'].astype(str).str.split().str[-1].str.lstrip('0')
                
                # Convertir fechas a str DD/MM/YYYY
                fechas = pd.to_datetime(df_pol['Fecha de Pago'], errors='coerce')
                df_pol['Fecha de Pago Fmt'] = fechas.dt.strftime('%d/%m/%Y').fillna('')
                
                mapa_fechas = df_pol.drop_duplicates(subset='_ped', keep='first').set_index('_ped')['Fecha de Pago Fmt'].to_dict()
                
                # Asignar a nuestro DataFrame
                df['_ped_str'] = df['Pedimento'].astype(str).str.lstrip('0')
                df['Fecha Real Pago (Bancario)'] = df['_ped_str'].map(mapa_fechas).fillna('')
                df.drop(columns=['_ped_str'], inplace=True, errors='ignore')

        # Fallback para los que quedaron vacíos: usar 'Fecha de Pago (Data Stage)' formateada
        mask_vacio = df['Fecha Real Pago (Bancario)'].astype(str).str.strip() == ''
        if 'Fecha de Pago (Data Stage)' in df.columns:
            # Convertir la de Data Stage que está en datetime a string DD/MM/YYYY
            fechas_ds = pd.to_datetime(df['Fecha de Pago (Data Stage)'], errors='coerce')
            df.loc[mask_vacio, 'Fecha Real Pago (Bancario)'] = fechas_ds.dt.strftime('%d/%m/%Y').fillna('')
            
        return df

    def _calcular_valores_dlls(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        tc = pd.to_numeric(df.get('TIPO CAMBIO MXP', 1), errors='coerce').fillna(1).replace(0, 1)
        df['Valor Aduana DLLS'] = (df.get('Base Gravable MXN', 0) / tc).round(2)
        df['Valor Comercial DLLS'] = df.get('_valor_dls', 0).round(2)
        return df

    def _calcular_total_impuestos(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        df['Total Pagado Impuestos MXP'] = df.get('IVA al 16% MXN', 0)
        tc = pd.to_numeric(df.get('TIPO CAMBIO MXP', 1), errors='coerce').fillna(1).replace(0, 1)
        df['Total Pagado Impuestos DLLS'] = (df['Total Pagado Impuestos MXP'] / tc).round(2)
        return df

    def _cruzar_supplier_report(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        if 'SupplierReport' not in sources:
            df['Código Proveedor'] = df['TaxID Proveedor']
            return df
        df_sup = sources['SupplierReport'].copy()
        df_sup['_nombre_norm'] = df_sup['Nombre'].astype(str).str.strip().str.upper()
        sup_map = df_sup.drop_duplicates(subset='_nombre_norm', keep='first')[['_nombre_norm', 'Partner']]
        df = df.merge(sup_map, left_on='Nombre del Proveedor', right_on='_nombre_norm', how='left')
        df['Código Proveedor'] = df['Partner'].fillna(df['TaxID Proveedor'])
        df.drop(columns=['_nombre_norm', 'Partner'], inplace=True, errors='ignore')
        return df

    def _agregar_tipo_operacion(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        if 'DatosGenerales' not in sources:
            return df
        df_501 = sources['DatosGenerales'].copy()
        df_501['_key'] = build_pedimento_key(df_501)
        if 'TipoOperacion' not in df_501.columns:
            return df
        tipo_map = df_501.drop_duplicates(subset='_key', keep='first')[['_key', 'TipoOperacion']]
        tipo_map.rename(columns={'TipoOperacion': 'Tipo Operación'}, inplace=True)
        df = df.merge(tipo_map, on='_key', how='left')
        return df

    def _marcar_rectificaciones(self, df: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """
        Identifica pedimentos rectificados en la 701 y marca en R2:
        - Si un pedimento R2 aparece como NUEVO en 701: se marca Clave del Pedimento = 'R1'
          y se anotan en NOTAS los datos del pedimento anterior.
        - Los pedimentos ANTERIORES mantienen sus valores originales sin cambio.
        - Para las filas de datos R1 (no CAAAREM), se aplican los valores del 702:
          * Si 702 tiene IVA (CC=3 FP=0) > 0: BG = IVA/0.16, IVA = 702_IVA
          * Si 702 solo tiene DTA (CC=1 FP=0): BG = 702_DTA, IVA = 0
        """
        if 'RectificacionesHeader' not in sources:
            return df

        df_701 = sources['RectificacionesHeader'].copy()
        if df_701.empty:
            return df

        # Construir keys para 701
        df_701['_key_nuevo'] = build_pedimento_key(df_701)
        df_701['_key_anterior'] = build_pedimento_key(
            df_701, col_patente='PatenteAnterior',
            col_pedimento='PedimentoAnterior',
            col_seccion='SeccionAduaneraAnterior'
        )

        # Mapeo nuevo → anterior
        map_nuevo_a_ant = {}
        for _, r in df_701.iterrows():
            k_nuevo = r['_key_nuevo']
            map_nuevo_a_ant[k_nuevo] = {
                'Patente': r['PatenteAnterior'].strip(),
                'Pedimento': r['PedimentoAnterior'].strip(),
                'Aduana': r['SeccionAduaneraAnterior'].strip(),
            }

        # Obtener valores del 702 por pedimento
        monto_702_dta = {}  # CC=1 FP=0
        monto_702_iva = {}  # CC=3 FP=0
        if 'RectificacionesDetalle' in sources:
            df_702 = sources['RectificacionesDetalle'].copy()
            df_702['ClaveContribucion'] = df_702['ClaveContribucion'].astype(str).str.strip()
            df_702['FormaPago'] = df_702['FormaPago'].astype(str).str.strip()
            df_702['ImportePago'] = pd.to_numeric(df_702['ImportePago'], errors='coerce').fillna(0)
            df_702['_key'] = build_pedimento_key(df_702)
            # DTA (CC=1 FP=0)
            cc1 = df_702[(df_702['ClaveContribucion'] == '1') & (df_702['FormaPago'] == '0')]
            if not cc1.empty:
                monto_702_dta = cc1.groupby('_key')['ImportePago'].sum().to_dict()
            # IVA (CC=3 FP=0)
            cc3 = df_702[(df_702['ClaveContribucion'] == '3') & (df_702['FormaPago'] == '0')]
            if not cc3.empty:
                monto_702_iva = cc3.groupby('_key')['ImportePago'].sum().to_dict()

        # Pre-computar VA y VC del 551 por pedimento key
        va_por_ped = {}  # key -> VA_MXN
        vc_por_ped = {}  # key -> VC_MXN
        if 'Partidas' in sources:
            df_551 = sources['Partidas'].copy()
            df_551['_key'] = build_pedimento_key(df_551)
            df_551['ValorAduana'] = pd.to_numeric(df_551['ValorAduana'], errors='coerce').fillna(0)
            df_551['ValorComercial'] = pd.to_numeric(df_551['ValorComercial'], errors='coerce').fillna(0)
            va_por_ped = df_551.groupby('_key')['ValorAduana'].sum().to_dict()
            vc_por_ped = df_551.groupby('_key')['ValorComercial'].sum().to_dict()

        # Pre-computar Total Impuestos (557 + 510 FP=0, excluyendo CC=15,23 Prevalidación)
        ti_por_ped = {}  # key -> TI_MXP (VA + 557 + 510 sin prev)
        imp_557 = {}
        if 'ContribucionesPartida' in sources:
            df_557_src = sources['ContribucionesPartida'].copy()
            df_557_src['_key'] = build_pedimento_key(df_557_src)
            df_557_src['ImportePago'] = pd.to_numeric(df_557_src['ImportePago'], errors='coerce').fillna(0)
            df_557_src['FormaPago'] = df_557_src['FormaPago'].astype(str).str.strip()
            fp0_557 = df_557_src[df_557_src['FormaPago'] == '0']
            imp_557 = fp0_557.groupby('_key')['ImportePago'].sum().to_dict()
        imp_510_no_prev = {}
        if 'ContribucionesPedimento' in sources:
            df_510_src = sources['ContribucionesPedimento'].copy()
            df_510_src['_key'] = build_pedimento_key(df_510_src)
            df_510_src['ImportePago'] = pd.to_numeric(df_510_src['ImportePago'], errors='coerce').fillna(0)
            df_510_src['FormaPago'] = df_510_src['FormaPago'].astype(str).str.strip()
            df_510_src['ClaveContribucion'] = df_510_src['ClaveContribucion'].astype(str).str.strip()
            # FP=0, excluyendo CC=15 (Prev) y CC=23 (IVA Prev)
            fp0_510 = df_510_src[
                (df_510_src['FormaPago'] == '0') &
                (~df_510_src['ClaveContribucion'].isin(['15', '23']))
            ]
            imp_510_no_prev = fp0_510.groupby('_key')['ImportePago'].sum().to_dict()

        # Calcular TI por pedimento: VA + 557 + 510 (sin prev)
        all_keys = set(list(va_por_ped.keys()) + list(imp_557.keys()) + list(imp_510_no_prev.keys()))
        for k in all_keys:
            ti_por_ped[k] = va_por_ped.get(k, 0) + imp_557.get(k, 0) + imp_510_no_prev.get(k, 0)

        # Inicializar columnas
        if 'NOTAS' not in df.columns:
            df['NOTAS'] = ''
        if 'Clave del Pedimento' not in df.columns:
            df['Clave del Pedimento'] = ''

        # Construir _key dinámica desde Patente/Pedimento/Aduana del df
        sec_norm = pd.to_numeric(df['Aduana'], errors='coerce').fillna(0).astype(int).astype(str)
        pat_norm = pd.to_numeric(df['Patente'], errors='coerce').fillna(0).astype(int).astype(str)
        ped_norm = pd.to_numeric(df['Pedimento'], errors='coerce').fillna(0).astype(int).astype(str)
        df_keys = pat_norm + '|' + ped_norm + '|' + sec_norm

        n_rectif = 0

        # Pre-contar filas de datos (no CAAAREM) por pedimento R1
        r1_data_count = {}  # key -> número de filas data para ese ped
        for idx in df.index:
            key = df_keys[idx]
            if key in map_nuevo_a_ant:
                is_caaarem = str(df.at[idx, 'TaxID Proveedor']).strip() == 'CAAAREM'
                if not is_caaarem:
                    r1_data_count[key] = r1_data_count.get(key, 0) + 1

        # Marcar pedimentos NUEVOS como R1 y aplicar 702 + valores completos DS
        for idx in df.index:
            key = df_keys[idx]
            if key in map_nuevo_a_ant:
                ant = map_nuevo_a_ant[key]
                nota = f"R1 de: {ant['Patente']}|{ant['Pedimento']}|{ant['Aduana']}"
                df.at[idx, 'NOTAS'] = nota
                df.at[idx, 'Clave del Pedimento'] = 'R1'

                tc = pd.to_numeric(df.at[idx, 'TIPO CAMBIO MXP'], errors='coerce')
                is_caaarem = str(df.at[idx, 'TaxID Proveedor']).strip() == 'CAAAREM'
                n_rows = r1_data_count.get(key, 1)
                
                # VA y VC: valor completo en CAAAREM, dividido entre filas de datos
                if pd.notna(tc) and tc > 0:
                    divisor = 1 if is_caaarem else n_rows
                    if key in va_por_ped:
                        df.at[idx, 'Valor Aduana DLLS'] = (va_por_ped[key] / tc) / divisor
                    if key in vc_por_ped:
                        df.at[idx, 'Valor Comercial DLLS'] = (vc_por_ped[key] / tc) / divisor

                # BG, IVA y Total Impuestos: solo filas de datos (no CAAAREM)
                if not is_caaarem:
                    # BG e IVA del 702, distribuido entre filas del pedimento
                    iva_702 = monto_702_iva.get(key, 0)
                    dta_702 = monto_702_dta.get(key, 0)
                    if iva_702 > 0:
                        df.at[idx, 'Base Gravable MXN'] = (iva_702 / 0.16) / n_rows
                        df.at[idx, 'IVA al 16% MXN'] = iva_702 / n_rows
                    elif dta_702 > 0:
                        df.at[idx, 'Base Gravable MXN'] = dta_702 / n_rows
                        df.at[idx, 'IVA al 16% MXN'] = 0
                    else:
                        # Ped R1 sin entrada en 702: BG=0, IVA=0
                        df.at[idx, 'Base Gravable MXN'] = 0
                        df.at[idx, 'IVA al 16% MXN'] = 0

                    # Total Impuestos: valores completos del pedimento
                    if pd.notna(tc) and tc > 0 and key in ti_por_ped:
                        ti_mxp = ti_por_ped[key]
                        df.at[idx, 'Total Pagado Impuestos MXP'] = ti_mxp / n_rows
                        df.at[idx, 'Total Pagado Impuestos DLLS'] = (ti_mxp / tc) / n_rows
                n_rectif += 1

        if n_rectif > 0:
            logger.info(f"   Rectificaciones R2: {n_rectif} filas marcadas como R1")
            # Log totales R1
            r1_mask = df['Clave del Pedimento'].astype(str).str.strip() == 'R1'
            total_r1_bg = df.loc[r1_mask, 'Base Gravable MXN'].sum()
            total_r1_iva = df.loc[r1_mask, 'IVA al 16% MXN'].sum()
            logger.info(f"   Total R1: BG={total_r1_bg:,.2f}, IVA={total_r1_iva:,.2f}")

        return df

    def _validar_completitud_shippers(self, sources: dict, df: pd.DataFrame):
        """Diagnóstico: detecta pedimentos con IVA que no tienen factura en Shippers."""
        if 'ContribucionesPartida' not in sources or 'Shippers' not in sources:
            return
        df_557 = sources['ContribucionesPartida'].copy()
        df_557['_key'] = build_pedimento_key(df_557)
        df_557['ClaveContribucion'] = df_557['ClaveContribucion'].astype(str).str.strip()
        df_557['FormaPago'] = df_557['FormaPago'].astype(str).str.strip()
        df_557['ImportePago'] = pd.to_numeric(df_557['ImportePago'], errors='coerce').fillna(0)
        iva_557 = df_557[(df_557['FormaPago'] == '0') & (df_557['ClaveContribucion'] == '3')]
        keys_iva = set(iva_557[iva_557['ImportePago'] > 0].groupby('_key')['ImportePago'].sum().reset_index()['_key'])

        df_ship = sources['Shippers'].copy()
        df_ship['_key'] = build_pedimento_key(
            df_ship, col_patente='patente', col_pedimento='pedimento',
            col_seccion='adua_sec_desp')
        keys_ship = set(df_ship['_key'].unique())

        keys_sin = keys_iva - keys_ship
        if keys_sin:
            logger.warning(f"   ⚠️ COMPLETITUD SHIPPERS: {len(keys_sin)} pedimentos con IVA NO tienen factura en Shippers")
            for k in sorted(keys_sin):
                logger.warning(f"       → Pedimento sin factura: {k}")

    def _filtrar_por_poliza_contable(self, df: pd.DataFrame, sources: dict) -> tuple:
        """Separa pedimentos en incluidos (en Póliza Contable) y excluidos (pendientes)."""
        if 'PolizaContable' not in sources:
            return (df, pd.DataFrame(columns=df.columns))

        df_pol = sources['PolizaContable'].copy()
        logger.info(f"   Póliza Contable (IMP): {len(df_pol)} registros")

        # Parsear columna 'Pedimentos / Factura' → Patente|Pedimento|SeccionAduanera
        col_ped = None
        for c in df_pol.columns:
            if 'pedimento' in str(c).lower() and 'factura' in str(c).lower():
                col_ped = c
                break
        if col_ped is None:
            for c in df_pol.columns:
                if 'pedimento' in str(c).lower():
                    col_ped = c
                    break

        if col_ped is None:
            logger.warning("   ⚠️ No se encontró columna de pedimentos en Póliza Contable")
            return (df, pd.DataFrame(columns=df.columns))

        # Formato Póliza: "AA SS PPPP NNNNNNN" → extraer Patente y Pedimento
        # Matching on Patente + Pedimento (ignoring SeccionAduanera/year for robustness)
        keys_poliza = set()
        for val in df_pol[col_ped].dropna().unique():
            parts = str(val).strip().split()
            if len(parts) >= 4:
                try:
                    patente = str(int(float(parts[2])))
                    pedimento = str(int(float(parts[3])))
                    keys_poliza.add(f"{patente}|{pedimento}")
                except (ValueError, IndexError):
                    continue

        logger.info(f"   Póliza Contable: {len(keys_poliza)} pedimentos únicos encontrados")

        # Construir clave Patente|Pedimento en R2
        patente_norm = pd.to_numeric(df['Patente'], errors='coerce').fillna(0).astype(int).astype(str)
        pedimento_norm = pd.to_numeric(df['Pedimento'], errors='coerce').fillna(0).astype(int).astype(str)
        df_ped_completo = patente_norm + '|' + pedimento_norm

        # Todas las filas (incluyendo CAAAREM) deben estar en la Póliza
        # Ford excluye CAAAREM de pedimentos no en estado de cuenta
        mask_in_poliza = df_ped_completo.isin(keys_poliza)
        mask_incluido = mask_in_poliza

        df_incluido = df[mask_incluido].copy()
        df_excluido = df[~mask_incluido].copy()

        logger.info(f"   Estado de Cuenta: {len(df_incluido)} incluidos, {len(df_excluido)} excluidos (pendientes)")

        # Log pedimentos excluidos
        keys_exc = set(df_ped_completo[~mask_incluido].unique())
        keys_exc = {k for k in keys_exc if k and k not in ('--0000-0000000', '-00-0000-0000000')}
        if keys_exc:
            logger.info(f"   Pedimentos excluidos (únicos): {len(keys_exc)}")
            for i, k in enumerate(sorted(keys_exc)):
                if i < 10:
                    logger.info(f"       → {k}")
            if len(keys_exc) > 10:
                logger.info(f"       ... y {len(keys_exc) - 10} más")

        return (df_incluido, df_excluido)

    def _log_cuadre_con_exclusiones(self, df_inc: pd.DataFrame, df_exc: pd.DataFrame, sources: dict):
        """Cuadre final: Incluido + Excluido vs DataStage."""
        try:
            base_inc = df_inc['Base Gravable MXN'].sum() if 'Base Gravable MXN' in df_inc.columns else 0
            iva_inc = df_inc['IVA al 16% MXN'].sum() if 'IVA al 16% MXN' in df_inc.columns else 0
            prev_inc = df_inc['Prevalidación MXN'].sum() if 'Prevalidación MXN' in df_inc.columns else 0
            iva_prev_inc = df_inc['IVA Prevalidación MXN'].sum() if 'IVA Prevalidación MXN' in df_inc.columns else 0

            base_exc = df_exc['Base Gravable MXN'].sum() if 'Base Gravable MXN' in df_exc.columns and len(df_exc) > 0 else 0
            iva_exc = df_exc['IVA al 16% MXN'].sum() if 'IVA al 16% MXN' in df_exc.columns and len(df_exc) > 0 else 0
            prev_exc = df_exc['Prevalidación MXN'].sum() if 'Prevalidación MXN' in df_exc.columns and len(df_exc) > 0 else 0
            iva_prev_exc = df_exc['IVA Prevalidación MXN'].sum() if 'IVA Prevalidación MXN' in df_exc.columns and len(df_exc) > 0 else 0

            iva_total = iva_inc + iva_exc

            # DS reference
            ds_iva = 0
            ds_igi = 0
            if 'ContribucionesPartida' in sources:
                df_557 = sources['ContribucionesPartida'].copy()
                df_557['ImportePago'] = pd.to_numeric(df_557['ImportePago'], errors='coerce').fillna(0)
                df_557['ClaveContribucion'] = df_557['ClaveContribucion'].astype(str).str.strip()
                df_557['FormaPago'] = df_557['FormaPago'].astype(str).str.strip()
                ds_iva = df_557[(df_557['ClaveContribucion'] == '3') & (df_557['FormaPago'] == '0')]['ImportePago'].sum()
                ds_igi = df_557[(df_557['ClaveContribucion'] == '6') & (df_557['FormaPago'] == '0')]['ImportePago'].sum()

            delta = abs(iva_total - ds_iva)
            delta_inc = abs(iva_inc - ds_iva)

            logger.info(f"   📊 CUADRE R2 CON EXCLUSIONES:")
            logger.info(f"       R2 Incluido  — Base: {base_inc:,.2f}, IVA: {iva_inc:,.2f}, Prev: {prev_inc:,.2f}, IVA Prev: {iva_prev_inc:,.2f}")
            logger.info(f"       R2 Excluido  — Base: {base_exc:,.2f}, IVA: {iva_exc:,.2f}, Prev: {prev_exc:,.2f}, IVA Prev: {iva_prev_exc:,.2f}")
            logger.info(f"       R2 TOTAL     — Base: {base_inc + base_exc:,.2f}, IVA: {iva_total:,.2f}")
            logger.info(f"       DS IVA CC=3 (557 FP=0): {ds_iva:,.2f}")
            logger.info(f"       DS IGI CC=6 (557 FP=0): {ds_igi:,.2f}")
            logger.info(f"       Δ IVA Total (Inc+Exc vs DS): {delta:,.2f} ⚠️")
            logger.info(f"       Δ IVA Incluido (vs DS): {delta_inc:,.2f}")
        except Exception as e:
            logger.warning(f"   ⚠️ Error en cuadre: {e}")

    def _agregar_resumen_ford(self, df_inc: pd.DataFrame, df_exc: pd.DataFrame, sources: dict) -> pd.DataFrame:
        """Agrega filas de resumen al estilo Ford al final del R2."""
        if 'Base Gravable MXN' not in df_inc.columns:
            return df_inc

        try:
            total_base = df_inc['Base Gravable MXN'].sum()
            total_iva = df_inc['IVA al 16% MXN'].sum()
            total_prev = df_inc['Prevalidación MXN'].sum() if 'Prevalidación MXN' in df_inc.columns else 0
            total_iva_prev = df_inc['IVA Prevalidación MXN'].sum() if 'IVA Prevalidación MXN' in df_inc.columns else 0
            total_iva_exceptuado = df_inc['Cálculo IVA Exceptuado'].sum() if 'Cálculo IVA Exceptuado' in df_inc.columns else 0
            total_iva_pagado = total_iva + total_iva_prev

            # === DataStage directo de archivos .asc ===
            # Identificar pedimentos con IVA (557 CC=3 FP=0) para filtrar
            iva_keys = set()
            if 'ContribucionesPartida' in sources:
                df_557 = sources['ContribucionesPartida'].copy()
                df_557['ImportePago'] = pd.to_numeric(df_557['ImportePago'], errors='coerce').fillna(0)
                df_557['ClaveContribucion'] = df_557['ClaveContribucion'].astype(str).str.strip()
                df_557['FormaPago'] = df_557['FormaPago'].astype(str).str.strip()
                df_557['_key'] = build_pedimento_key(df_557)
                iva_keys = set(df_557[(df_557['ClaveContribucion'] == '3') & (df_557['FormaPago'] == '0')]['_key'].unique())

            # IVA DataStage: 557 CC=3 FP=0
            ds_iva = 0
            if 'ContribucionesPartida' in sources:
                ds_iva = df_557[(df_557['ClaveContribucion'] == '3') & (df_557['FormaPago'] == '0')]['ImportePago'].sum()

            # Base Gravable DataStage: 551 ValorAduana (por pedimento, IVA peds) + 510 DTA + 557 ADV
            ds_valor_aduana = 0
            if 'Partidas' in sources:
                df_551 = sources['Partidas'].copy()
                df_551['_key'] = build_pedimento_key(df_551)
                df_551['ValorAduana'] = pd.to_numeric(df_551['ValorAduana'], errors='coerce').fillna(0)
                va_by_ped = df_551.groupby('_key')['ValorAduana'].sum()
                ds_valor_aduana = va_by_ped[va_by_ped.index.isin(iva_keys)].sum()

            ds_dta = 0
            if 'ContribucionesPedimento' in sources:
                df_510 = sources['ContribucionesPedimento'].copy()
                df_510['ImportePago'] = pd.to_numeric(df_510['ImportePago'], errors='coerce').fillna(0)
                df_510['ClaveContribucion'] = df_510['ClaveContribucion'].astype(str).str.strip()
                df_510['FormaPago'] = df_510['FormaPago'].astype(str).str.strip()
                df_510['_key'] = build_pedimento_key(df_510)
                ds_dta = df_510[(df_510['ClaveContribucion'] == '1') & (df_510['FormaPago'] == '0') & df_510['_key'].isin(iva_keys)]['ImportePago'].sum()

            ds_adv = 0
            if 'ContribucionesPartida' in sources:
                ds_adv = df_557[(df_557['ClaveContribucion'] == '6') & (df_557['FormaPago'] == '0') & df_557['_key'].isin(iva_keys)]['ImportePago'].sum()

            ds_base = ds_valor_aduana + ds_dta + ds_adv

            # Prevalidación DataStage: 510 CC=15 FP=0
            ds_prev = 0
            if 'ContribucionesPedimento' in sources:
                ds_prev = df_510[(df_510['ClaveContribucion'] == '15') & (df_510['FormaPago'] == '0')]['ImportePago'].sum()
            # IVA Prev DataStage: 510 CC=23 FP=0
            ds_iva_prev = 0
            if 'ContribucionesPedimento' in sources:
                ds_iva_prev = df_510[(df_510['ClaveContribucion'] == '23') & (df_510['FormaPago'] == '0')]['ImportePago'].sum()

            # Total IVA pagado DS
            ds_total_pagado = ds_iva + ds_iva_prev

            # IVA Exceptuado DS
            ds_iva_exceptuado = (ds_base * 0.16) - ds_iva

            # === Total de R1 (rectificaciones) ===
            # BG e IVA de los pedimentos NUEVOS en 701 que tienen IVA (557 CC=3 FP=0 > 0)
            total_r1_base = 0
            total_r1_iva = 0
            if 'RectificacionesHeader' in sources:
                df_701_r = sources['RectificacionesHeader'].copy()
                if not df_701_r.empty:
                    df_701_r['_key_nuevo'] = build_pedimento_key(df_701_r)
                    for _, r701 in df_701_r.iterrows():
                        k_nuevo = r701['_key_nuevo']
                        # IVA del pedimento nuevo (557 CC=3 FP=0)
                        iva_n = 0
                        if 'ContribucionesPartida' in sources:
                            iva_n = df_557[(df_557['_key'] == k_nuevo) & (df_557['ClaveContribucion'] == '3') & (df_557['FormaPago'] == '0')]['ImportePago'].sum()
                        if iva_n > 0:
                            # BG del pedimento nuevo = VA + DTA + ADV (DS completo)
                            va_n = va_by_ped.get(k_nuevo, 0) if 'Partidas' in sources else 0
                            dta_n = df_510[(df_510['_key'] == k_nuevo) & (df_510['ClaveContribucion'] == '1') & (df_510['FormaPago'] == '0')]['ImportePago'].sum() if 'ContribucionesPedimento' in sources else 0
                            adv_n = df_557[(df_557['_key'] == k_nuevo) & (df_557['ClaveContribucion'] == '6') & (df_557['FormaPago'] == '0')]['ImportePago'].sum() if 'ContribucionesPartida' in sources else 0
                            total_r1_base += va_n + dta_n + adv_n
                            total_r1_iva += iva_n

            # Base grabable R1 de diferencia de IVA = suma de R1 BG de Inc + Exc
            r1_mask_inc = df_inc['Clave del Pedimento'].astype(str).str.strip() == 'R1' if 'Clave del Pedimento' in df_inc.columns else pd.Series(False, index=df_inc.index)
            r1_mask_exc = df_exc['Clave del Pedimento'].astype(str).str.strip() == 'R1' if len(df_exc) > 0 and 'Clave del Pedimento' in df_exc.columns else pd.Series(False, index=df_exc.index)
            bg_r1_diff = df_inc.loc[r1_mask_inc, 'Base Gravable MXN'].sum() + (df_exc.loc[r1_mask_exc, 'Base Gravable MXN'].sum() if len(df_exc) > 0 else 0)
            iva_r1_diff = df_inc.loc[r1_mask_inc, 'IVA al 16% MXN'].sum() + (df_exc.loc[r1_mask_exc, 'IVA al 16% MXN'].sum() if len(df_exc) > 0 else 0)

            # === Excluidos ===
            exc_base = df_exc['Base Gravable MXN'].sum() if len(df_exc) > 0 else 0
            exc_iva = df_exc['IVA al 16% MXN'].sum() if len(df_exc) > 0 else 0
            exc_prev = df_exc['Prevalidación MXN'].sum() if len(df_exc) > 0 and 'Prevalidación MXN' in df_exc.columns else 0
            exc_iva_prev = df_exc['IVA Prevalidación MXN'].sum() if len(df_exc) > 0 and 'IVA Prevalidación MXN' in df_exc.columns else 0

            # Verificación total
            verif_base = total_base + total_r1_base + exc_base
            verif_iva = total_iva + total_r1_iva + exc_iva

            resumen_rows = [
                # Subtotal del R2 incluido
                {'Nacionalidad': '', 'Base Gravable MXN': total_base,
                 'Cálculo IVA Exceptuado': total_iva_exceptuado,
                 'IVA al 16% MXN': total_iva,
                 'Prevalidación MXN': total_prev, 'IVA Prevalidación MXN': total_iva_prev,
                 'TOTAL IVA PAGADO': total_iva_pagado},
                {'Nacionalidad': ''},
                # DataStage (valores reales del .asc)
                {'Nacionalidad': 'Data Stage', 'Base Gravable MXN': ds_base,
                 'Cálculo IVA Exceptuado': ds_iva_exceptuado,
                 'IVA al 16% MXN': ds_iva,
                 'Prevalidación MXN': ds_prev, 'IVA Prevalidación MXN': ds_iva_prev,
                 'TOTAL IVA PAGADO': ds_total_pagado},
                # Diferencia (Subtotal - DS)
                {'Nacionalidad': 'Diferencia', 'Base Gravable MXN': total_base - ds_base,
                 'IVA al 16% MXN': total_iva - ds_iva},
                {'Nacionalidad': ''},
                # Reporte de IVA
                {'Nacionalidad': 'Reporte de IVA', 'Base Gravable MXN': total_base,
                 'Cálculo IVA Exceptuado': total_iva_exceptuado,
                 'IVA al 16% MXN': total_iva,
                 'Prevalidación MXN': total_prev, 'IVA Prevalidación MXN': total_iva_prev},
                # Total de R1 (rectificaciones)
                {'Nacionalidad': 'Total de R1', 'Base Gravable MXN': total_r1_base,
                 'IVA al 16% MXN': total_r1_iva},
                # Base grabable R1 de diferencia de IVA
                {'Nacionalidad': 'Base grabable R1 de diferencia de IVA',
                 'Base Gravable MXN': bg_r1_diff, 'IVA al 16% MXN': iva_r1_diff},
                # Pedimentos no encontrados en estado de cuenta
                {'Nacionalidad': 'Pedimentos no encontrados en esdo. Cuenta',
                 'Base Gravable MXN': exc_base, 'Cálculo IVA Exceptuado': 0,
                 'IVA al 16% MXN': exc_iva,
                 'Prevalidación MXN': exc_prev, 'IVA Prevalidación MXN': exc_iva_prev},
                # Verificación total (Inc + R1 + Exc = DS)
                {'Nacionalidad': '', 'Base Gravable MXN': verif_base,
                 'Cálculo IVA Exceptuado': total_iva_exceptuado,
                 'IVA al 16% MXN': verif_iva,
                 'Prevalidación MXN': total_prev + exc_prev,
                 'IVA Prevalidación MXN': total_iva_prev + exc_iva_prev},
                # Delta (Verificación - DS ≈ 0)
                {'Nacionalidad': '', 'Base Gravable MXN': verif_base - ds_base,
                 'Cálculo IVA Exceptuado': total_iva_exceptuado - ds_iva_exceptuado,
                 'IVA al 16% MXN': verif_iva - ds_iva,
                 'Prevalidación MXN': (total_prev + exc_prev) - ds_prev,
                 'IVA Prevalidación MXN': (total_iva_prev + exc_iva_prev) - ds_iva_prev},
                {'Nacionalidad': ''},
                # Base Gravable total DS
                {'Nacionalidad': 'Base Gravable', 'Base Gravable MXN': ds_base,
                 'IVA al 16% MXN': ds_base * 0.16,
                 'Prevalidación MXN': ''},
                # Delta final (IVA calculado - IVA DS)
                {'Nacionalidad': '', 'IVA al 16% MXN': (ds_base * 0.16) - ds_iva},
            ]

            df_resumen = pd.DataFrame(resumen_rows)
            for col in df_inc.columns:
                if col not in df_resumen.columns:
                    df_resumen[col] = ''
            df_inc = pd.concat([df_inc, df_resumen[df_inc.columns]], ignore_index=True)
            logger.info(f"   Resumen Ford agregado: {len(resumen_rows)} filas de resumen")
            logger.info(f"   DS Base={ds_base:,.2f}, DS IVA={ds_iva:,.2f}, DS Prev={ds_prev:,.2f}")
        except Exception as e:
            logger.warning(f"   ⚠️ Error generando resumen Ford: {e}")
            import traceback
            traceback.print_exc()

        return df_inc



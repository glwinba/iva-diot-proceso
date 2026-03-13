# ============================================================================
# src/asc_parser.py - PROYECTO_DIOT
# Lector genérico de archivos .asc (glosa de pedimentos)
# ============================================================================

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class AscParser:
    """Lee archivos .asc delimitados por pipe y retorna DataFrames."""

    def __init__(self, delimiter='|', encoding='latin-1'):
        self.delimiter = delimiter
        self.encoding = encoding

    def parse_file(self, filepath: Path) -> pd.DataFrame:
        """
        Lee un archivo .asc y retorna un DataFrame.
        Primera línea = headers, separador = |
        """
        filepath = Path(filepath)
        if not filepath.exists():
            logger.warning(f"Archivo no encontrado: {filepath}")
            return pd.DataFrame()

        try:
            # Leer contenido crudo (los .asc pueden tener \r)
            with open(filepath, 'r', encoding=self.encoding) as f:
                content = f.read()

            # Limpiar caracteres de retorno de carro
            content = content.replace('\r', '')
            lines = content.strip().split('\n')

            if len(lines) < 2:
                logger.warning(f"Archivo vacío o solo headers: {filepath.name}")
                return pd.DataFrame()

            # Extraer headers
            headers = [h.strip() for h in lines[0].split(self.delimiter)]

            # Extraer datos
            data_rows = []
            for i, line in enumerate(lines[1:], start=2):
                if not line.strip():
                    continue
                fields = [f.strip() for f in line.split(self.delimiter)]

                # Normalizar longitud
                if len(fields) < len(headers):
                    fields += [''] * (len(headers) - len(fields))
                elif len(fields) > len(headers):
                    fields = fields[:len(headers)]

                data_rows.append(fields)

            df = pd.DataFrame(data_rows, columns=headers)

            logger.info(f"📄 {filepath.name}: {len(df):,} registros, {len(headers)} columnas")
            return df

        except Exception as e:
            logger.error(f"Error leyendo {filepath.name}: {e}")
            return pd.DataFrame()

    def load_all_sources(self, folder: Path, asc_mapping: dict) -> dict:
        """
        Carga todos los archivos .asc de una carpeta.
        
        Args:
            folder: Carpeta con los archivos .asc
            asc_mapping: Dict {sufijo: nombre} ej: {'501': 'DatosGenerales'}
        
        Returns:
            Dict {nombre: DataFrame} ej: {'DatosGenerales': df_501}
        """
        sources = {}

        # Auto-extraer archivos .zip (incluyendo ZIPs anidados dentro de ZIPs)
        import zipfile
        zip_queue = list(folder.glob('*.zip'))
        processed_zips = set()

        while zip_queue:
            zf_path = zip_queue.pop(0)
            if zf_path in processed_zips:
                continue
            processed_zips.add(zf_path)

            try:
                with zipfile.ZipFile(zf_path, 'r') as zf:
                    members = zf.namelist()

                    # Extraer .asc (preservando estructura de carpetas)
                    asc_members = [m for m in members if m.endswith('.asc')]
                    if asc_members:
                        zf.extractall(folder, members=asc_members)
                        logger.info(f"📦 {zf_path.name}: extraídos {len(asc_members)} archivos .asc")

                    # Extraer .xlsx plano en la carpeta raíz
                    xlsx_members = [
                        m for m in members
                        if m.endswith('.xlsx')
                        and '__MACOSX' not in m
                        and not Path(m).name.startswith('~$')
                    ]
                    extracted_xlsx = 0
                    for member in xlsx_members:
                        filename = Path(member).name
                        target = folder / filename
                        if not target.exists():
                            with zf.open(member) as src, open(target, 'wb') as dst:
                                dst.write(src.read())
                            extracted_xlsx += 1
                    if extracted_xlsx:
                        logger.info(f"📦 {zf_path.name}: extraídos {extracted_xlsx} archivos .xlsx")

                    # Extraer ZIPs anidados y agregarlos a la cola
                    nested_zips = [
                        m for m in members
                        if m.endswith('.zip') and '__MACOSX' not in m
                    ]
                    for member in nested_zips:
                        filename = Path(member).name
                        target = folder / filename
                        if not target.exists():
                            with zf.open(member) as src, open(target, 'wb') as dst:
                                dst.write(src.read())
                        if target not in processed_zips:
                            zip_queue.append(target)
                            logger.info(f"📦 ZIP anidado encontrado: {filename} — se procesará")

            except Exception as e:
                logger.error(f"Error descomprimiendo {zf_path.name}: {e}")

        asc_files = list(folder.rglob('*.asc'))

        if not asc_files:
            logger.error(f"No se encontraron archivos .asc en {folder}")
            return sources

        logger.info(f"📂 {folder}: {len(asc_files)} archivos .asc encontrados")

        for suffix, name in asc_mapping.items():
            # Buscar archivo que termine en _{suffix}.asc
            matching = [f for f in asc_files if f.name.endswith(f'_{suffix}.asc')]

            if matching:
                filepath = matching[0]
                df = self.parse_file(filepath)
                if not df.empty:
                    sources[name] = df
                else:
                    logger.warning(f"⚠️ Archivo {suffix} vacío o sin datos")
            else:
                logger.debug(f"Archivo _{suffix}.asc no encontrado en {folder}")

        logger.info(f"✅ {len(sources)} fuentes .asc cargadas de {len(asc_mapping)} esperadas")
        return sources

    def load_excel_sources(self, folder: Path, excel_mapping: dict) -> dict:
        """
        Carga archivos Excel (.xlsx) buscando por keyword en el nombre.
        Busca en la carpeta dada y en su padre.

        Args:
            folder: Carpeta de referencia (donde están los .asc)
            excel_mapping: Dict {keyword: config}
                config puede ser str (nombre) o dict con:
                  name (str), sheet_name (str, opcional), header_row (int, opcional)

        Returns:
            Dict {nombre: DataFrame}
        """
        sources = {}
        search_dirs = [folder, folder.parent]

        for keyword, config in excel_mapping.items():
            # Soportar config como str o dict
            if isinstance(config, str):
                config = {'name': config}
            name = config['name']
            sheet_name = config.get('sheet_name', 0)  # 0 = primera hoja
            header_row = config.get('header_row', 0)  # 0 = primera fila
            use_str = config.get('dtype_str', False)   # leer como str para ceros iniciales

            found = False
            for search_dir in search_dirs:
                if not search_dir.exists():
                    continue
                # Buscar archivos que contengan el keyword (ignorar temporales ~$)
                matching = sorted([
                    f for f in search_dir.glob('*.xlsx')
                    if keyword in f.name and not f.name.startswith('~$')
                ])
                if matching:
                    password = config.get('password')
                    all_dfs = []
                    for filepath in matching:
                        try:
                            if password:
                                import msoffcrypto
                                import io
                                with open(filepath, 'rb') as f_enc:
                                    office_file = msoffcrypto.OfficeFile(f_enc)
                                    office_file.load_key(password=password)
                                    decrypted = io.BytesIO()
                                    office_file.decrypt(decrypted)
                                    decrypted.seek(0)
                                df = pd.read_excel(
                                    decrypted, engine='openpyxl',
                                    sheet_name=sheet_name, header=header_row,
                                    dtype=str if use_str else None
                                )
                            else:
                                df = pd.read_excel(
                                    filepath, engine='openpyxl',
                                    sheet_name=sheet_name, header=header_row,
                                    dtype=str if use_str else None
                                )
                            logger.info(f"📊 {filepath.name}"
                                        f"{'[' + str(sheet_name) + ']' if sheet_name != 0 else ''}"
                                        f": {len(df):,} registros, {len(df.columns)} columnas")
                            all_dfs.append(df)
                        except Exception as e:
                            logger.error(f"Error leyendo {filepath.name}: {e}")

                    if all_dfs:
                        if len(all_dfs) > 1:
                            combined = pd.concat(all_dfs, ignore_index=True)
                            logger.info(f"   🔗 '{keyword}': {len(all_dfs)} archivos combinados → {len(combined):,} registros totales")
                            sources[name] = combined
                        else:
                            sources[name] = all_dfs[0]
                        found = True
                    break

            if not found:
                logger.debug(f"Excel con keyword '{keyword}' no encontrado")

        if sources:
            logger.info(f"✅ {len(sources)} fuentes Excel cargadas")
        return sources

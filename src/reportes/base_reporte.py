# ============================================================================
# src/reportes/base_reporte.py - PROYECTO_DIOT
# Clase abstracta base para generadores de reportes
# ============================================================================

from abc import ABC, abstractmethod
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseReporte(ABC):
    """Clase base para todos los reportes DIOT."""

    # Cada subclase define su nombre y las fuentes que necesita
    nombre = ""
    fuentes_requeridas = []

    def validar_fuentes(self, sources: dict) -> bool:
        """Verifica que las fuentes necesarias estén cargadas."""
        faltantes = [f for f in self.fuentes_requeridas if f not in sources]
        if faltantes:
            logger.error(
                f"❌ {self.nombre}: faltan fuentes requeridas: {faltantes}"
            )
            return False
        return True

    @abstractmethod
    def generar(self, sources: dict) -> pd.DataFrame:
        """
        Genera el reporte a partir de las fuentes de datos.
        
        Args:
            sources: Dict {nombre_fuente: DataFrame}
        
        Returns:
            DataFrame con el reporte generado.
        """
        pass

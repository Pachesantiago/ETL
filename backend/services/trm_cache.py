"""
Sistema de cache para TRM (Tasa Representativa del Mercado)
Optimiza el rendimiento evitando consultas API repetidas
"""
import json
import os
from datetime import datetime, timedelta
from decimal import Decimal
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class TRMCache:
    """Sistema de cache para valores de TRM"""

    def __init__(self, cache_file="data/trm_cache.json", cache_duration_hours=24):
        self.cache_file = Path(cache_file)
        self.cache_duration = timedelta(hours=cache_duration_hours)
        self.cache_dir = self.cache_file.parent
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cached_trm(self):
        """
        Obtiene TRM del cache si está disponible y no expirado

        Returns:
            Decimal or None: Valor de TRM cacheado o None si no disponible
        """
        try:
            if not self.cache_file.exists():
                return None

            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            # Verificar si el cache no ha expirado
            cached_time = datetime.fromisoformat(cache_data['timestamp'])
            if datetime.now() - cached_time < self.cache_duration:
                trm_value = Decimal(str(cache_data['trm_value']))
                logger.info(f"TRM obtenida del cache: {trm_value}")
                return trm_value
            else:
                logger.info("Cache de TRM expirado")
                return None

        except Exception as e:
            logger.warning(f"Error leyendo cache de TRM: {e}")
            return None

    def save_trm_to_cache(self, trm_value):
        """
        Guarda valor de TRM en el cache

        Args:
            trm_value (Decimal): Valor de TRM a cachear
        """
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'trm_value': float(trm_value)
            }

            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)

            logger.info(f"TRM guardada en cache: {trm_value}")

        except Exception as e:
            logger.error(f"Error guardando TRM en cache: {e}")

    def is_cache_valid(self):
        """
        Verifica si el cache actual es válido

        Returns:
            bool: True si el cache es válido
        """
        cached_trm = self.get_cached_trm()
        return cached_trm is not None

# Instancia global del cache
_trm_cache = None

def get_trm_cache():
    """Obtiene la instancia global del cache de TRM"""
    global _trm_cache
    if _trm_cache is None:
        _trm_cache = TRMCache()
    return _trm_cache

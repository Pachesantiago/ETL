"""
Servicio para obtener la Tasa Representativa del Mercado (TRM)
"""
import requests
import json
from datetime import datetime, date
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

class TRMService:
    """Servicio para obtener la TRM del Banco de la República de Colombia"""
    
    def __init__(self, api_url=None):
        self.api_url = api_url or 'https://www.datos.gov.co/resource/32sa-8pi3.json?$limit=1&$order=vigenciadesde%20DESC'
        self.fallback_trm = Decimal('4200.00')  # TRM de respaldo en caso de error
    
    def obtener_trm_actual(self):
        """
        Obtiene la TRM actual desde la API del Banco de la República
        
        Returns:
            Decimal: Valor de la TRM en COP por USD
        """
        try:
            logger.info("Consultando TRM actual desde API del Banco de la República")
            
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data and len(data) > 0:
                trm_value = data[0].get('valor', self.fallback_trm)
                trm_decimal = Decimal(str(trm_value))
                
                logger.info(f"TRM obtenida exitosamente: {trm_decimal}")
                return trm_decimal
            else:
                logger.warning("No se encontraron datos de TRM, usando valor de respaldo")
                return self.fallback_trm
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al consultar API de TRM: {e}")
            return self._obtener_trm_respaldo()
        except (ValueError, KeyError, TypeError) as e:
            logger.error(f"Error al procesar respuesta de TRM: {e}")
            return self._obtener_trm_respaldo()
        except Exception as e:
            logger.error(f"Error inesperado al obtener TRM: {e}")
            return self._obtener_trm_respaldo()
    
    def _obtener_trm_respaldo(self):
        """
        Obtiene TRM de respaldo cuando falla la API principal
        
        Returns:
            Decimal: Valor de TRM de respaldo
        """
        logger.warning(f"Usando TRM de respaldo: {self.fallback_trm}")
        return self.fallback_trm
    
    def convertir_usd_a_cop(self, monto_usd, trm=None):
        """
        Convierte un monto de USD a COP usando la TRM
        
        Args:
            monto_usd (float|Decimal): Monto en dólares estadounidenses
            trm (Decimal, optional): TRM específica a usar. Si no se proporciona, obtiene la actual
        
        Returns:
            tuple: (monto_cop, trm_utilizada)
        """
        try:
            if trm is None:
                trm = self.obtener_trm_actual()
            
            monto_usd_decimal = Decimal(str(monto_usd))
            monto_cop = monto_usd_decimal * trm
            
            # Redondear a 2 decimales
            monto_cop = monto_cop.quantize(Decimal('0.01'))
            
            logger.debug(f"Conversión: ${monto_usd} USD = ${monto_cop} COP (TRM: {trm})")
            
            return monto_cop, trm
            
        except Exception as e:
            logger.error(f"Error en conversión USD a COP: {e}")
            raise
    
    def validar_trm(self, trm_value):
        """
        Valida que el valor de TRM esté en un rango razonable
        
        Args:
            trm_value (Decimal): Valor de TRM a validar
        
        Returns:
            bool: True si es válida, False en caso contrario
        """
        try:
            trm_decimal = Decimal(str(trm_value))
            # Rango razonable para TRM COP/USD (entre 3000 y 6000)
            return Decimal('3000') <= trm_decimal <= Decimal('6000')
        except:
            return False
    
    def obtener_trm_fecha(self, fecha_consulta):
        """
        Obtiene la TRM para una fecha específica (funcionalidad futura)
        
        Args:
            fecha_consulta (date): Fecha para consultar TRM
        
        Returns:
            Decimal: Valor de TRM para la fecha especificada
        """
        # Por ahora retorna la TRM actual
        # En una implementación completa, consultaría el histórico
        logger.info(f"Consultando TRM para fecha {fecha_consulta} (usando TRM actual)")
        return self.obtener_trm_actual()

# Función de conveniencia para uso directo
def obtener_trm():
    """
    Función de conveniencia para obtener la TRM actual
    
    Returns:
        Decimal: Valor actual de la TRM
    """
    service = TRMService()
    return service.obtener_trm_actual()

def convertir_usd_cop(monto_usd):
    """
    Función de conveniencia para convertir USD a COP
    
    Args:
        monto_usd (float): Monto en USD
    
    Returns:
        tuple: (monto_cop, trm_utilizada)
    """
    service = TRMService()
    return service.convertir_usd_a_cop(monto_usd)

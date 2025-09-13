"""
Utilidades para manejo de fechas en el ETL OtakuLATAM
Proporciona funciones centralizadas para formateo consistente de fechas
"""
from datetime import datetime
from typing import Union, Optional
import logging

logger = logging.getLogger(__name__)

# Formatos de fecha estándar
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
MYSQL_DATE_FORMAT = '%Y-%m-%d'
ISO_FORMAT = '%Y-%m-%dT%H:%M:%S'
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'

def get_mysql_datetime_now() -> str:
    """
    Obtiene la fecha y hora actual en formato MySQL DATETIME
    
    Returns:
        str: Fecha en formato 'YYYY-MM-DD HH:MM:SS'
    """
    return datetime.now().strftime(MYSQL_DATETIME_FORMAT)

def get_mysql_date_now() -> str:
    """
    Obtiene la fecha actual en formato MySQL DATE
    
    Returns:
        str: Fecha en formato 'YYYY-MM-DD'
    """
    return datetime.now().strftime(MYSQL_DATE_FORMAT)

def datetime_to_mysql_string(dt: Union[datetime, str]) -> str:
    """
    Convierte un objeto datetime o string a formato MySQL
    
    Args:
        dt: Objeto datetime o string de fecha
        
    Returns:
        str: Fecha en formato MySQL 'YYYY-MM-DD HH:MM:SS'
        
    Raises:
        ValueError: Si el formato de entrada no es válido
    """
    try:
        if isinstance(dt, datetime):
            return dt.strftime(MYSQL_DATETIME_FORMAT)
        elif isinstance(dt, str):
            # Intentar parsear diferentes formatos comunes
            formats_to_try = [
                MYSQL_DATETIME_FORMAT,
                ISO_FORMAT,
                '%Y-%m-%dT%H:%M:%S.%f',  # ISO con microsegundos
                '%a, %d %b %Y %H:%M:%S %Z',  # RFC 2822 (el formato problemático)
                '%Y-%m-%d %H:%M:%S.%f',  # MySQL con microsegundos
            ]
            
            for fmt in formats_to_try:
                try:
                    parsed_dt = datetime.strptime(dt, fmt)
                    return parsed_dt.strftime(MYSQL_DATETIME_FORMAT)
                except ValueError:
                    continue
            
            # Si ningún formato funciona, intentar con pandas
            try:
                import pandas as pd
                parsed_dt = pd.to_datetime(dt)
                return parsed_dt.strftime(MYSQL_DATETIME_FORMAT)
            except:
                pass
                
            raise ValueError(f"No se pudo parsear la fecha: {dt}")
        else:
            raise ValueError(f"Tipo de dato no soportado: {type(dt)}")
            
    except Exception as e:
        logger.error(f"Error convirtiendo fecha a MySQL: {e}")
        # En caso de error, devolver fecha actual
        logger.warning("Usando fecha actual como fallback")
        return get_mysql_datetime_now()

def validate_mysql_datetime(date_string: str) -> bool:
    """
    Valida si una cadena está en formato MySQL DATETIME válido
    
    Args:
        date_string: Cadena a validar
        
    Returns:
        bool: True si es válida, False en caso contrario
    """
    try:
        datetime.strptime(date_string, MYSQL_DATETIME_FORMAT)
        return True
    except ValueError:
        return False

def get_timestamp_for_filename() -> str:
    """
    Obtiene timestamp para usar en nombres de archivo
    
    Returns:
        str: Timestamp en formato 'YYYYMMDD_HHMMSS'
    """
    return datetime.now().strftime(TIMESTAMP_FORMAT)

def format_datetime_for_json(dt: Union[datetime, str]) -> str:
    """
    Formatea datetime para exportación JSON (ISO format)
    
    Args:
        dt: Objeto datetime o string
        
    Returns:
        str: Fecha en formato ISO
    """
    try:
        if isinstance(dt, datetime):
            return dt.isoformat()
        elif isinstance(dt, str):
            # Convertir primero a MySQL y luego a datetime para ISO
            mysql_str = datetime_to_mysql_string(dt)
            dt_obj = datetime.strptime(mysql_str, MYSQL_DATETIME_FORMAT)
            return dt_obj.isoformat()
        else:
            return datetime.now().isoformat()
    except Exception as e:
        logger.error(f"Error formateando fecha para JSON: {e}")
        return datetime.now().isoformat()

def ensure_mysql_datetime_format(value: Union[datetime, str, None]) -> str:
    """
    Asegura que un valor de fecha esté en formato MySQL DATETIME
    Función principal para usar en el ETL
    
    Args:
        value: Valor de fecha en cualquier formato
        
    Returns:
        str: Fecha garantizada en formato MySQL 'YYYY-MM-DD HH:MM:SS'
    """
    if value is None:
        return get_mysql_datetime_now()
    
    try:
        return datetime_to_mysql_string(value)
    except Exception as e:
        logger.error(f"Error asegurando formato MySQL: {e}")
        return get_mysql_datetime_now()

# Función de conveniencia para el ETL
def get_processing_timestamp() -> str:
    """
    Obtiene timestamp de procesamiento para el ETL
    Función específica para usar en transformer.py
    
    Returns:
        str: Fecha de procesamiento en formato MySQL
    """
    return get_mysql_datetime_now()

def convert_dataframe_datetime_columns(df, datetime_columns: list = None) -> 'pandas.DataFrame':
    """
    Convierte columnas datetime de un DataFrame a formato MySQL string
    
    Args:
        df: DataFrame de pandas
        datetime_columns: Lista de columnas datetime. Si None, detecta automáticamente
        
    Returns:
        DataFrame con columnas datetime convertidas a string MySQL
    """
    try:
        import pandas as pd
        
        df_copy = df.copy()
        
        if datetime_columns is None:
            # Detectar columnas datetime automáticamente
            datetime_columns = []
            for col in df_copy.columns:
                if df_copy[col].dtype == 'datetime64[ns]':
                    datetime_columns.append(col)
                elif col in ['fecha_procesamiento', 'Processing Date', 'timestamp']:
                    datetime_columns.append(col)
        
        # Convertir cada columna datetime
        for col in datetime_columns:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(ensure_mysql_datetime_format)
                logger.info(f"Convertida columna '{col}' a formato MySQL")
        
        return df_copy
        
    except Exception as e:
        logger.error(f"Error convirtiendo columnas datetime: {e}")
        return df

# Constantes útiles
MYSQL_NULL_DATE = '1970-01-01 00:00:00'
MYSQL_MAX_DATE = '2038-01-19 03:14:07'

def is_valid_mysql_date_range(date_string: str) -> bool:
    """
    Verifica si una fecha está en el rango válido de MySQL
    
    Args:
        date_string: Fecha en formato MySQL
        
    Returns:
        bool: True si está en rango válido
    """
    try:
        dt = datetime.strptime(date_string, MYSQL_DATETIME_FORMAT)
        min_dt = datetime.strptime(MYSQL_NULL_DATE, MYSQL_DATETIME_FORMAT)
        max_dt = datetime.strptime(MYSQL_MAX_DATE, MYSQL_DATETIME_FORMAT)
        
        return min_dt <= dt <= max_dt
    except:
        return False

# Función de diagnóstico
def diagnose_date_format(value: Union[datetime, str]) -> dict:
    """
    Diagnostica el formato de una fecha y proporciona información detallada
    
    Args:
        value: Valor de fecha a diagnosticar
        
    Returns:
        dict: Información de diagnóstico
    """
    diagnosis = {
        'original_value': str(value),
        'original_type': type(value).__name__,
        'is_mysql_format': False,
        'converted_mysql': None,
        'is_valid_range': False,
        'errors': []
    }
    
    try:
        # Verificar si ya está en formato MySQL
        if isinstance(value, str):
            diagnosis['is_mysql_format'] = validate_mysql_datetime(value)
        
        # Intentar conversión
        mysql_format = datetime_to_mysql_string(value)
        diagnosis['converted_mysql'] = mysql_format
        
        # Verificar rango válido
        diagnosis['is_valid_range'] = is_valid_mysql_date_range(mysql_format)
        
    except Exception as e:
        diagnosis['errors'].append(str(e))
    
    return diagnosis

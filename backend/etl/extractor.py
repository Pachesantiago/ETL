"""
Módulo Extractor del ETL OtakuLATAM
Responsable de extraer datos de diferentes fuentes
"""
import pandas as pd
import os
import logging
from pathlib import Path
import json
import csv

logger = logging.getLogger(__name__)

class DataExtractor:
    """Extractor de datos para el ETL"""
    
    def __init__(self):
        self.supported_formats = ['.csv', '.xlsx', '.xls', '.json', '.txt']
    
    def extract_from_csv(self, file_path, encoding='utf-8', delimiter=','):
        """
        Extrae datos de un archivo CSV
        
        Args:
            file_path (str): Ruta del archivo CSV
            encoding (str): Codificación del archivo
            delimiter (str): Delimitador del CSV
        
        Returns:
            pd.DataFrame: DataFrame con los datos extraídos
        """
        try:
            logger.info(f"Extrayendo datos de CSV: {file_path}")
            
            # Verificar que el archivo existe
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
            
            # Leer el archivo CSV
            df = pd.read_csv(
                file_path, 
                encoding=encoding, 
                delimiter=delimiter,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a']
            )
            
            logger.info(f"Datos extraídos exitosamente: {len(df)} registros, {len(df.columns)} columnas")
            logger.info(f"Columnas encontradas: {list(df.columns)}")
            
            return df
            
        except pd.errors.EmptyDataError:
            logger.error(f"El archivo CSV está vacío: {file_path}")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Error parseando CSV: {e}")
            raise
        except Exception as e:
            logger.error(f"Error extrayendo datos de CSV: {e}")
            raise
    
    def extract_from_excel(self, file_path, sheet_name=0):
        """
        Extrae datos de un archivo Excel
        
        Args:
            file_path (str): Ruta del archivo Excel
            sheet_name (str|int): Nombre o índice de la hoja
        
        Returns:
            pd.DataFrame: DataFrame con los datos extraídos
        """
        try:
            logger.info(f"Extrayendo datos de Excel: {file_path}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
            
            df = pd.read_excel(
                file_path, 
                sheet_name=sheet_name,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a']
            )
            
            logger.info(f"Datos extraídos exitosamente: {len(df)} registros, {len(df.columns)} columnas")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de Excel: {e}")
            raise
    
    def extract_from_json(self, file_path):
        """
        Extrae datos de un archivo JSON
        
        Args:
            file_path (str): Ruta del archivo JSON
        
        Returns:
            pd.DataFrame: DataFrame con los datos extraídos
        """
        try:
            logger.info(f"Extrayendo datos de JSON: {file_path}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Convertir a DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Si es un diccionario, intentar extraer la lista de datos
                if 'data' in data:
                    df = pd.DataFrame(data['data'])
                else:
                    df = pd.DataFrame([data])
            else:
                raise ValueError("Formato JSON no soportado")
            
            logger.info(f"Datos extraídos exitosamente: {len(df)} registros, {len(df.columns)} columnas")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de JSON: {e}")
            raise
    
    def extract_data(self, file_path, **kwargs):
        """
        Extrae datos automáticamente según la extensión del archivo
        
        Args:
            file_path (str): Ruta del archivo
            **kwargs: Argumentos adicionales para los extractores específicos
        
        Returns:
            pd.DataFrame: DataFrame con los datos extraídos
        """
        try:
            file_path = Path(file_path)
            extension = file_path.suffix.lower()
            
            logger.info(f"Iniciando extracción de datos desde: {file_path}")
            logger.info(f"Formato detectado: {extension}")
            
            if extension not in self.supported_formats:
                raise ValueError(f"Formato no soportado: {extension}. Formatos soportados: {self.supported_formats}")
            
            # Extraer según el formato
            if extension == '.csv':
                df = self.extract_from_csv(str(file_path), **kwargs)
            elif extension in ['.xlsx', '.xls']:
                df = self.extract_from_excel(str(file_path), **kwargs)
            elif extension == '.json':
                df = self.extract_from_json(str(file_path))
            else:
                raise ValueError(f"Extractor no implementado para: {extension}")
            
            # Validaciones básicas
            self._validate_extracted_data(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error en extracción de datos: {e}")
            raise
    
    def _validate_extracted_data(self, df):
        """
        Valida los datos extraídos
        
        Args:
            df (pd.DataFrame): DataFrame a validar
        """
        if df is None or df.empty:
            raise ValueError("No se extrajeron datos o el DataFrame está vacío")
        
        logger.info(f"Validación exitosa: {len(df)} registros extraídos")
        
        # Mostrar información básica del DataFrame
        logger.info(f"Columnas: {list(df.columns)}")
        logger.info(f"Tipos de datos: {df.dtypes.to_dict()}")
        
        # Verificar valores nulos
        null_counts = df.isnull().sum()
        if null_counts.any():
            logger.warning(f"Valores nulos encontrados: {null_counts[null_counts > 0].to_dict()}")
    
    def validate_required_columns(self, df, required_columns):
        """
        Valida que el DataFrame contenga las columnas requeridas
        
        Args:
            df (pd.DataFrame): DataFrame a validar
            required_columns (list): Lista de columnas requeridas
        
        Returns:
            bool: True si todas las columnas están presentes
        """
        try:
            missing_columns = []
            df_columns = [col.lower().strip() for col in df.columns]
            
            for required_col in required_columns:
                required_col_lower = required_col.lower().strip()
                if required_col_lower not in df_columns:
                    missing_columns.append(required_col)
            
            if missing_columns:
                logger.error(f"Columnas requeridas faltantes: {missing_columns}")
                logger.error(f"Columnas disponibles: {list(df.columns)}")
                return False
            
            logger.info("Todas las columnas requeridas están presentes")
            return True
            
        except Exception as e:
            logger.error(f"Error validando columnas: {e}")
            return False
    
    def get_data_sample(self, df, n_rows=5):
        """
        Obtiene una muestra de los datos para inspección
        
        Args:
            df (pd.DataFrame): DataFrame
            n_rows (int): Número de filas a mostrar
        
        Returns:
            dict: Información de muestra de los datos
        """
        try:
            sample_info = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'columns': list(df.columns),
                'data_types': df.dtypes.to_dict(),
                'sample_data': df.head(n_rows).to_dict('records'),
                'null_counts': df.isnull().sum().to_dict(),
                'memory_usage': df.memory_usage(deep=True).sum()
            }
            
            return sample_info
            
        except Exception as e:
            logger.error(f"Error obteniendo muestra de datos: {e}")
            return {}
    
    def clean_column_names(self, df):
        """
        Limpia los nombres de las columnas
        
        Args:
            df (pd.DataFrame): DataFrame con columnas a limpiar
        
        Returns:
            pd.DataFrame: DataFrame con columnas limpias
        """
        try:
            # Crear copia para no modificar el original
            df_clean = df.copy()
            
            # Limpiar nombres de columnas
            df_clean.columns = df_clean.columns.str.strip()  # Quitar espacios
            df_clean.columns = df_clean.columns.str.lower()  # Convertir a minúsculas
            df_clean.columns = df_clean.columns.str.replace(' ', '_')  # Reemplazar espacios con _
            df_clean.columns = df_clean.columns.str.replace('[^a-zA-Z0-9_]', '', regex=True)  # Quitar caracteres especiales
            
            logger.info(f"Nombres de columnas limpiados: {list(df_clean.columns)}")
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Error limpiando nombres de columnas: {e}")
            return df

class OtakuDataExtractor(DataExtractor):
    """Extractor específico para datos de OtakuLATAM"""
    
    def __init__(self):
        super().__init__()
        self.required_columns = ['name', 'age', 'gender', 'income', 'illness']
    
    def extract_otaku_data(self, file_path):
        """
        Extrae y valida datos específicos de OtakuLATAM
        
        Args:
            file_path (str): Ruta del archivo de datos
        
        Returns:
            pd.DataFrame: DataFrame con datos validados
        """
        try:
            # Extraer datos
            df = self.extract_data(file_path)
            
            # Limpiar nombres de columnas
            df = self.clean_column_names(df)
            
            # Validar columnas requeridas
            if not self.validate_required_columns(df, self.required_columns):
                raise ValueError("El archivo no contiene todas las columnas requeridas")
            
            # Validaciones específicas de OtakuLATAM
            self._validate_otaku_data(df)
            
            logger.info("Datos de OtakuLATAM extraídos y validados exitosamente")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de OtakuLATAM: {e}")
            raise
    
    def _validate_otaku_data(self, df):
        """
        Validaciones específicas para datos de OtakuLATAM
        
        Args:
            df (pd.DataFrame): DataFrame a validar
        """
        try:
            # Validar que no haya filas completamente vacías
            empty_rows = df.isnull().all(axis=1).sum()
            if empty_rows > 0:
                logger.warning(f"Se encontraron {empty_rows} filas completamente vacías")
            
            # Validar rangos de edad
            if 'age' in df.columns:
                invalid_ages = df[(df['age'] < 0) | (df['age'] > 120)]
                if not invalid_ages.empty:
                    logger.warning(f"Se encontraron {len(invalid_ages)} registros con edades inválidas")
            
            # Validar valores de género
            if 'gender' in df.columns:
                valid_genders = ['male', 'female', 'm', 'f']
                invalid_genders = df[~df['gender'].str.lower().isin(valid_genders)]
                if not invalid_genders.empty:
                    logger.warning(f"Se encontraron {len(invalid_genders)} registros con géneros inválidos")
            
            # Validar valores de ingresos
            if 'income' in df.columns:
                invalid_incomes = df[(df['income'] < 0) | (df['income'] > 1000000)]
                if not invalid_incomes.empty:
                    logger.warning(f"Se encontraron {len(invalid_incomes)} registros con ingresos fuera de rango")
            
            # Validar valores de enfermedad
            if 'illness' in df.columns:
                valid_illness = ['yes', 'no', 'y', 'n', 'true', 'false', '1', '0']
                invalid_illness = df[~df['illness'].astype(str).str.lower().isin(valid_illness)]
                if not invalid_illness.empty:
                    logger.warning(f"Se encontraron {len(invalid_illness)} registros con valores de enfermedad inválidos")
            
            logger.info("Validaciones específicas de OtakuLATAM completadas")
            
        except Exception as e:
            logger.error(f"Error en validaciones específicas: {e}")
            raise

# Función de conveniencia
def extract_otaku_data(file_path):
    """
    Función de conveniencia para extraer datos de OtakuLATAM
    
    Args:
        file_path (str): Ruta del archivo
    
    Returns:
        pd.DataFrame: DataFrame con datos extraídos
    """
    extractor = OtakuDataExtractor()
    return extractor.extract_otaku_data(file_path)

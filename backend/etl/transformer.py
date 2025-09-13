"""
Módulo Transformador del ETL OtakuLATAM
Responsable de transformar los datos según los requerimientos del negocio
"""
import pandas as pd
import logging
from decimal import Decimal
from datetime import datetime
import re
import sys
import os

# Agregar el directorio backend al path para importar utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.date_utils import get_processing_timestamp, ensure_mysql_datetime_format

logger = logging.getLogger(__name__)

class DataTransformer:
    """Transformador de datos para el ETL"""
    
    def __init__(self, trm_service=None):
        self.trm_service = trm_service
        self.current_trm = None
        
        # Mapeos de traducción
        self.gender_translation = {
            'male': 'Masculino',
            'female': 'Femenino',
            'm': 'Masculino',
            'f': 'Femenino'
        }
        
        self.illness_translation = {
            'yes': 'Sí',
            'no': 'No',
            'y': 'Sí',
            'n': 'No',
            'true': 'Sí',
            'false': 'No',
            '1': 'Sí',
            '0': 'No'
        }
    
    def transform_data(self, df, trm_value=None):
        """
        Aplica todas las transformaciones requeridas a los datos
        
        Args:
            df (pd.DataFrame): DataFrame con datos originales
            trm_value (Decimal, optional): Valor específico de TRM a usar
        
        Returns:
            pd.DataFrame: DataFrame con datos transformados
        """
        try:
            logger.info("Iniciando transformación de datos")
            
            # Crear copia para no modificar el original
            df_transformed = df.copy()
            
            # Limpiar y preparar datos
            df_transformed = self._clean_data(df_transformed)
            
            # Obtener TRM si no se proporciona
            if trm_value is None and self.trm_service:
                self.current_trm = self.trm_service.obtener_trm_actual()
            else:
                self.current_trm = trm_value or Decimal('4200.00')
            
            logger.info(f"TRM utilizada para conversiones: {self.current_trm}")
            
            # Aplicar transformaciones específicas
            df_transformed = self._transform_gender(df_transformed)
            df_transformed = self._transform_illness(df_transformed)
            df_transformed = self._transform_age_to_lustros(df_transformed)
            df_transformed = self._transform_income_to_cop(df_transformed)
            
            # Agregar metadatos de transformación
            df_transformed = self._add_transformation_metadata(df_transformed)
            
            logger.info(f"Transformación completada: {len(df_transformed)} registros procesados")
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error en transformación de datos: {e}")
            raise
    
    def _clean_data(self, df):
        """
        Limpia y prepara los datos para transformación
        
        Args:
            df (pd.DataFrame): DataFrame a limpiar
        
        Returns:
            pd.DataFrame: DataFrame limpio
        """
        try:
            logger.info("Limpiando datos")
            
            # Crear copia
            df_clean = df.copy()
            
            # Limpiar espacios en blanco en columnas de texto
            text_columns = df_clean.select_dtypes(include=['object']).columns
            for col in text_columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
            
            # Convertir columnas a tipos apropiados
            if 'age' in df_clean.columns:
                df_clean['age'] = pd.to_numeric(df_clean['age'], errors='coerce')
            
            if 'income' in df_clean.columns:
                df_clean['income'] = pd.to_numeric(df_clean['income'], errors='coerce')
            
            # Normalizar valores de texto
            if 'gender' in df_clean.columns:
                df_clean['gender'] = df_clean['gender'].str.lower().str.strip()
            
            if 'illness' in df_clean.columns:
                df_clean['illness'] = df_clean['illness'].str.lower().str.strip()
            
            # Eliminar filas con valores críticos nulos
            initial_count = len(df_clean)
            df_clean = df_clean.dropna(subset=['name', 'age', 'gender', 'income', 'illness'])
            final_count = len(df_clean)
            
            if initial_count != final_count:
                logger.warning(f"Se eliminaron {initial_count - final_count} filas con valores nulos críticos")
            
            logger.info("Limpieza de datos completada")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error limpiando datos: {e}")
            raise
    
    def _transform_gender(self, df):
        """
        Transforma el campo Gender de inglés a español
        
        Args:
            df (pd.DataFrame): DataFrame con datos
        
        Returns:
            pd.DataFrame: DataFrame con género transformado
        """
        try:
            logger.info("Transformando campo Gender")
            
            # Solo crear columna transformada (eliminamos genero_original)
            df['genero_es'] = df['gender'].map(self.gender_translation)
            
            # Manejar valores no mapeados
            unmapped_genders = df[df['genero_es'].isnull()]['gender'].unique()
            if len(unmapped_genders) > 0:
                logger.warning(f"Géneros no mapeados encontrados: {unmapped_genders}")
                # Asignar valor por defecto o mantener original
                df['genero_es'] = df['genero_es'].fillna(df['gender'])
            
            logger.info("Transformación de Gender completada")
            return df
            
        except Exception as e:
            logger.error(f"Error transformando Gender: {e}")
            raise
    
    def _transform_illness(self, df):
        """
        Transforma el campo Illness de inglés a español
        
        Args:
            df (pd.DataFrame): DataFrame con datos
        
        Returns:
            pd.DataFrame: DataFrame con enfermedad transformada
        """
        try:
            logger.info("Transformando campo Illness")
            
            # Renombrar enfermedad_original por Illness (como solicitado)
            df['Illness'] = df['illness']
            df['enfermedad_es'] = df['illness'].map(self.illness_translation)
            
            # Manejar valores no mapeados
            unmapped_illness = df[df['enfermedad_es'].isnull()]['illness'].unique()
            if len(unmapped_illness) > 0:
                logger.warning(f"Valores de enfermedad no mapeados: {unmapped_illness}")
                # Asignar valor por defecto
                df['enfermedad_es'] = df['enfermedad_es'].fillna('No')
            
            logger.info("Transformación de Illness completada")
            return df
            
        except Exception as e:
            logger.error(f"Error transformando Illness: {e}")
            raise
    
    def _transform_age_to_lustros(self, df):
        """
        Transforma la edad de años a lustros
        
        Args:
            df (pd.DataFrame): DataFrame con datos
        
        Returns:
            pd.DataFrame: DataFrame con edad en lustros
        """
        try:
            logger.info("Transformando edad a lustros")
            
            # Solo convertir a lustros (eliminamos edad_anos)
            df['edad_lustros'] = df['age'] / 5
            
            # Redondear a 2 decimales
            df['edad_lustros'] = df['edad_lustros'].round(2)
            
            logger.info("Transformación de edad a lustros completada")
            return df
            
        except Exception as e:
            logger.error(f"Error transformando edad a lustros: {e}")
            raise
    
    def _transform_income_to_cop(self, df):
        """
        Transforma los ingresos de USD a COP
        
        Args:
            df (pd.DataFrame): DataFrame con datos
        
        Returns:
            pd.DataFrame: DataFrame con ingresos en COP
        """
        try:
            logger.info("Transformando ingresos de USD a COP")
            
            # Solo convertir a COP (eliminamos ingreso_usd)
            df['ingreso_cop'] = df['income'] * float(self.current_trm)
            
            # Redondear a 2 decimales
            df['ingreso_cop'] = df['ingreso_cop'].round(2)
            
            # Agregar TRM utilizada
            df['trm_utilizada'] = float(self.current_trm)
            
            logger.info(f"Transformación de ingresos completada usando TRM: {self.current_trm}")
            return df
            
        except Exception as e:
            logger.error(f"Error transformando ingresos: {e}")
            raise
    
    def _add_transformation_metadata(self, df):
        """
        Agrega metadatos de la transformación y reorganiza columnas
        
        Args:
            df (pd.DataFrame): DataFrame transformado
        
        Returns:
            pd.DataFrame: DataFrame con metadatos y estructura final
        """
        try:
            # Agregar timestamp de procesamiento en formato MySQL
            mysql_timestamp = get_processing_timestamp()
            df['fecha_procesamiento'] = mysql_timestamp
            logger.info(f"Agregado timestamp de procesamiento: {mysql_timestamp}")
            
            # Eliminar columna illness original (ya tenemos Illness)
            if 'illness' in df.columns:
                df = df.drop(columns=['illness'])
                logger.info("Eliminada columna 'illness' original")
            
            # Crear columna nombre desde name
            if 'name' in df.columns:
                df['nombre'] = df['name']
                # Eliminar la columna name original
                df = df.drop(columns=['name'])
                logger.info("Creada columna 'nombre' y eliminada 'name'")
            
            # Mantener nombres de columnas en español para compatibilidad con frontend
            # Agregar campos faltantes que espera el frontend
            if 'age' in df.columns:
                df['edad_anos'] = df['age']
            if 'income' in df.columns:
                df['ingreso_usd'] = df['income']
            if 'gender' in df.columns:
                df['genero_original'] = df['gender']
            if 'Illness' in df.columns:
                df['enfermedad_original'] = df['Illness']
            
            # Reordenar columnas según el orden esperado por el frontend
            desired_order = [
                'nombre', 'edad_anos', 'edad_lustros', 'genero_original', 'genero_es',
                'ingreso_usd', 'ingreso_cop', 'trm_utilizada', 'enfermedad_original', 
                'enfermedad_es', 'fecha_procesamiento'
            ]
            
            # Verificar que todas las columnas deseadas existen
            available_columns = [col for col in desired_order if col in df.columns]
            df = df[available_columns]
            logger.info(f"Columnas reordenadas para frontend: {available_columns}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error agregando metadatos: {e}")
            raise
    
    def validate_transformed_data(self, df):
        """
        Valida los datos transformados
        
        Args:
            df (pd.DataFrame): DataFrame transformado
        
        Returns:
            dict: Resultado de la validación
        """
        try:
            logger.info("Validando datos transformados")
            
            validation_result = {
                'is_valid': True,
                'errors': [],
                'warnings': [],
                'statistics': {}
            }
            
            # Verificar columnas requeridas (actualizadas para frontend)
            required_columns = [
                'nombre', 'edad_lustros', 'genero_es',
                'ingreso_cop', 'trm_utilizada', 'enfermedad_es'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                validation_result['errors'].append(f"Columnas faltantes: {missing_columns}")
                validation_result['is_valid'] = False
            
            # Validar rangos de datos
            if 'edad_lustros' in df.columns:
                invalid_lustros = df[(df['edad_lustros'] < 0) | (df['edad_lustros'] > 24)]
                if not invalid_lustros.empty:
                    validation_result['warnings'].append(f"{len(invalid_lustros)} registros con lustros fuera de rango")
            
            if 'ingreso_cop' in df.columns:
                invalid_cop = df[df['ingreso_cop'] < 0]
                if not invalid_cop.empty:
                    validation_result['errors'].append(f"{len(invalid_cop)} registros con ingresos COP negativos")
                    validation_result['is_valid'] = False
            
            # Estadísticas
            validation_result['statistics'] = {
                'total_records': len(df),
                'avg_age_lustros': df['edad_lustros'].mean() if 'edad_lustros' in df.columns else 0,
                'avg_income_cop': df['ingreso_cop'].mean() if 'ingreso_cop' in df.columns else 0,
                'gender_distribution': df['genero_es'].value_counts().to_dict() if 'genero_es' in df.columns else {},
                'illness_distribution': df['enfermedad_es'].value_counts().to_dict() if 'enfermedad_es' in df.columns else {}
            }
            
            logger.info(f"Validación completada. Válido: {validation_result['is_valid']}")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validando datos transformados: {e}")
            return {
                'is_valid': False,
                'errors': [f"Error en validación: {str(e)}"],
                'warnings': [],
                'statistics': {}
            }
    
    def get_transformation_summary(self, original_df, transformed_df):
        """
        Genera un resumen de las transformaciones aplicadas
        
        Args:
            original_df (pd.DataFrame): DataFrame original
            transformed_df (pd.DataFrame): DataFrame transformado
        
        Returns:
            dict: Resumen de transformaciones
        """
        try:
            summary = {
                'original_records': len(original_df),
                'transformed_records': len(transformed_df),
                'records_lost': len(original_df) - len(transformed_df),
                'trm_used': float(self.current_trm) if self.current_trm else None,
                'transformations_applied': [
                    'Gender: English → Spanish',
                    'Illness: English → Spanish',
                    'Age: Years → Lustros',
                    'Income: USD → COP',
                    'Added processing metadata'
                ],
                'new_columns_created': [
                    'genero_es', 'enfermedad_es', 'edad_lustros', 
                    'ingreso_cop', 'trm_utilizada', 'fecha_procesamiento'
                ]
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generando resumen de transformaciones: {e}")
            return {}

class OtakuDataTransformer(DataTransformer):
    """Transformador específico para datos de OtakuLATAM"""
    
    def __init__(self, trm_service=None):
        super().__init__(trm_service)
    
    def transform_otaku_data(self, df, trm_value=None):
        """
        Aplica transformaciones específicas para OtakuLATAM
        
        Args:
            df (pd.DataFrame): DataFrame con datos originales
            trm_value (Decimal, optional): TRM específica
        
        Returns:
            tuple: (DataFrame transformado, resumen de validación)
        """
        try:
            logger.info("Iniciando transformación específica de OtakuLATAM")
            
            # Aplicar transformaciones base
            df_transformed = self.transform_data(df, trm_value)
            
            # Validar datos transformados
            validation_result = self.validate_transformed_data(df_transformed)
            
            if not validation_result['is_valid']:
                logger.error("Datos transformados no pasaron validación")
                for error in validation_result['errors']:
                    logger.error(f"Error de validación: {error}")
            
            # Generar resumen
            transformation_summary = self.get_transformation_summary(df, df_transformed)
            
            logger.info("Transformación específica de OtakuLATAM completada")
            
            return df_transformed, {
                'validation': validation_result,
                'summary': transformation_summary
            }
            
        except Exception as e:
            logger.error(f"Error en transformación específica de OtakuLATAM: {e}")
            raise

# Función de conveniencia
def transform_otaku_data(df, trm_service=None, trm_value=None):
    """
    Función de conveniencia para transformar datos de OtakuLATAM
    
    Args:
        df (pd.DataFrame): DataFrame con datos originales
        trm_service: Servicio de TRM
        trm_value (Decimal, optional): TRM específica
    
    Returns:
        tuple: (DataFrame transformado, resultado de validación)
    """
    transformer = OtakuDataTransformer(trm_service)
    return transformer.transform_otaku_data(df, trm_value)

"""
Módulo Transformador Optimizado del ETL OtakuLATAM
Versión optimizada con operaciones vectorizadas y mejor rendimiento
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
from services.trm_service_optimized import TRMService

logger = logging.getLogger(__name__)

class DataTransformerOptimized:
    """Transformador optimizado de datos para el ETL"""

    def __init__(self, trm_service=None):
        self.trm_service = trm_service
        self.current_trm = None

        # Mapeos de traducción como diccionarios para operaciones vectorizadas
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
        Aplica todas las transformaciones requeridas usando operaciones vectorizadas

        Args:
            df (pd.DataFrame): DataFrame con datos originales
            trm_value (Decimal, optional): Valor específico de TRM a usar

        Returns:
            pd.DataFrame: DataFrame con datos transformados
        """
        try:
            logger.info("Iniciando transformación optimizada de datos")

            # Crear copia para no modificar el original
            df_transformed = df.copy()

            # Limpiar y preparar datos (optimizado)
            df_transformed = self._clean_data_optimized(df_transformed)

            # Obtener TRM si no se proporciona
            if trm_value is None and self.trm_service:
                self.current_trm = self.trm_service.obtener_trm_actual()
            else:
                self.current_trm = trm_value or Decimal('4200.00')

            logger.info(f"TRM utilizada para conversiones: {self.current_trm}")

            # Aplicar transformaciones vectorizadas
            df_transformed = self._apply_vectorized_transformations(df_transformed)

            # Agregar metadatos de transformación
            df_transformed = self._add_transformation_metadata_optimized(df_transformed)

            logger.info(f"Transformación completada: {len(df_transformed)} registros procesados")

            return df_transformed

        except Exception as e:
            logger.error(f"Error en transformación de datos: {e}")
            raise

    def _clean_data_optimized(self, df):
        """
        Limpia y prepara los datos de forma optimizada

        Args:
            df (pd.DataFrame): DataFrame a limpiar

        Returns:
            pd.DataFrame: DataFrame limpio
        """
        try:
            logger.info("Limpiando datos de forma optimizada")

            # Crear copia
            df_clean = df.copy()

            # Limpiar espacios en blanco en columnas de texto de forma vectorizada
            text_columns = df_clean.select_dtypes(include=['object']).columns
            if len(text_columns) > 0:
                df_clean[text_columns] = df_clean[text_columns].astype(str).apply(lambda x: x.str.strip())

            # Convertir columnas numéricas de forma vectorizada
            if 'age' in df_clean.columns:
                df_clean['age'] = pd.to_numeric(df_clean['age'], errors='coerce')

            if 'income' in df_clean.columns:
                df_clean['income'] = pd.to_numeric(df_clean['income'], errors='coerce')

            # Normalizar valores de texto de forma vectorizada
            if 'gender' in df_clean.columns:
                df_clean['gender'] = df_clean['gender'].str.lower().str.strip()

            if 'illness' in df_clean.columns:
                df_clean['illness'] = df_clean['illness'].str.lower().str.strip()

            # Eliminar filas con valores críticos nulos de forma vectorizada
            initial_count = len(df_clean)
            critical_nulls = df_clean[['name', 'age', 'gender', 'income', 'illness']].isnull().any(axis=1)
            df_clean = df_clean[~critical_nulls]
            final_count = len(df_clean)

            if initial_count != final_count:
                logger.warning(f"Se eliminaron {initial_count - final_count} filas con valores nulos críticos")

            logger.info("Limpieza de datos completada")
            return df_clean

        except Exception as e:
            logger.error(f"Error limpiando datos: {e}")
            raise

    def _apply_vectorized_transformations(self, df):
        """
        Aplica todas las transformaciones usando operaciones vectorizadas

        Args:
            df (pd.DataFrame): DataFrame a transformar

        Returns:
            pd.DataFrame: DataFrame transformado
        """
        try:
            logger.info("Aplicando transformaciones vectorizadas")

            # Transformaciones vectorizadas usando map
            df = self._transform_gender_vectorized(df)
            df = self._transform_illness_vectorized(df)
            df = self._transform_age_to_lustros_vectorized(df)
            df = self._transform_income_to_cop_vectorized(df)

            return df

        except Exception as e:
            logger.error(f"Error en transformaciones vectorizadas: {e}")
            raise

    def _transform_gender_vectorized(self, df):
        """
        Transforma el campo Gender usando operaciones vectorizadas

        Args:
            df (pd.DataFrame): DataFrame con datos

        Returns:
            pd.DataFrame: DataFrame con género transformado
        """
        try:
            logger.info("Transformando campo Gender (vectorizado)")

            # Usar map para transformación vectorizada
            df['genero_es'] = df['gender'].map(self.gender_translation)

            # Manejar valores no mapeados
            unmapped_mask = df['genero_es'].isnull()
            if unmapped_mask.any():
                logger.warning(f"Géneros no mapeados encontrados: {df.loc[unmapped_mask, 'gender'].unique()}")
                # Asignar valor por defecto o mantener original
                df.loc[unmapped_mask, 'genero_es'] = df.loc[unmapped_mask, 'gender']

            logger.info("Transformación de Gender completada")
            return df

        except Exception as e:
            logger.error(f"Error transformando Gender: {e}")
            raise

    def _transform_illness_vectorized(self, df):
        """
        Transforma el campo Illness usando operaciones vectorizadas

        Args:
            df (pd.DataFrame): DataFrame con datos

        Returns:
            pd.DataFrame: DataFrame con enfermedad transformada
        """
        try:
            logger.info("Transformando campo Illness (vectorizado)")

            # Renombrar y transformar de forma vectorizada
            df['Illness'] = df['illness']
            df['enfermedad_es'] = df['illness'].map(self.illness_translation)

            # Manejar valores no mapeados
            unmapped_mask = df['enfermedad_es'].isnull()
            if unmapped_mask.any():
                logger.warning(f"Valores de enfermedad no mapeados: {df.loc[unmapped_mask, 'illness'].unique()}")
                # Asignar valor por defecto
                df.loc[unmapped_mask, 'enfermedad_es'] = 'No'

            logger.info("Transformación de Illness completada")
            return df

        except Exception as e:
            logger.error(f"Error transformando Illness: {e}")
            raise

    def _transform_age_to_lustros_vectorized(self, df):
        """
        Transforma la edad a lustros usando operaciones vectorizadas

        Args:
            df (pd.DataFrame): DataFrame con datos

        Returns:
            pd.DataFrame: DataFrame con edad en lustros
        """
        try:
            logger.info("Transformando edad a lustros (vectorizado)")

            # Operación vectorizada directa
            df['edad_lustros'] = (df['age'] / 5).round(2)

            logger.info("Transformación de edad a lustros completada")
            return df

        except Exception as e:
            logger.error(f"Error transformando edad a lustros: {e}")
            raise

    def _transform_income_to_cop_vectorized(self, df):
        """
        Transforma los ingresos a COP usando operaciones vectorizadas

        Args:
            df (pd.DataFrame): DataFrame con datos

        Returns:
            pd.DataFrame: DataFrame con ingresos en COP
        """
        try:
            logger.info("Transformando ingresos de USD a COP (vectorizado)")

            # Operaciones vectorizadas
            trm_float = float(self.current_trm)
            df['ingreso_cop'] = (df['income'] * trm_float).round(2)

            # Agregar TRM utilizada
            df['trm_utilizada'] = trm_float

            logger.info(f"Transformación de ingresos completada usando TRM: {self.current_trm}")
            return df

        except Exception as e:
            logger.error(f"Error transformando ingresos: {e}")
            raise

    def _add_transformation_metadata_optimized(self, df):
        """
        Agrega metadatos de la transformación de forma optimizada

        Args:
            df (pd.DataFrame): DataFrame transformado

        Returns:
            pd.DataFrame: DataFrame con metadatos y estructura final
        """
        try:
            # Agregar timestamp de procesamiento
            mysql_timestamp = get_processing_timestamp()
            df['fecha_procesamiento'] = mysql_timestamp
            logger.info(f"Agregado timestamp de procesamiento: {mysql_timestamp}")

            # Eliminar columna illness original de forma vectorizada
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
        Valida los datos transformados de forma optimizada

        Args:
            df (pd.DataFrame): DataFrame transformado

        Returns:
            dict: Resultado de la validación
        """
        try:
            logger.info("Validando datos transformados (optimizado)")

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

            # Validar rangos usando operaciones vectorizadas
            if 'edad_lustros' in df.columns:
                invalid_lustros = (df['edad_lustros'] < 0) | (df['edad_lustros'] > 24)
                if invalid_lustros.any():
                    validation_result['warnings'].append(f"{invalid_lustros.sum()} registros con lustros fuera de rango")

            if 'ingreso_cop' in df.columns:
                invalid_cop = df['ingreso_cop'] < 0
                if invalid_cop.any():
                    validation_result['errors'].append(f"{invalid_cop.sum()} registros con ingresos COP negativos")
                    validation_result['is_valid'] = False

            # Estadísticas usando operaciones vectorizadas
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

class OtakuDataTransformerOptimized(DataTransformerOptimized):
    """Transformador específico optimizado para datos de OtakuLATAM"""

    def __init__(self, trm_service=None):
        super().__init__(trm_service)

    def transform_otaku_data(self, df, trm_value=None):
        """
        Aplica transformaciones específicas para OtakuLATAM de forma optimizada

        Args:
            df (pd.DataFrame): DataFrame con datos originales
            trm_value (Decimal, optional): TRM específica

        Returns:
            tuple: (DataFrame transformado, resumen de validación)
        """
        try:
            logger.info("Iniciando transformación específica optimizada de OtakuLATAM")

            # Aplicar transformaciones base optimizadas
            df_transformed = self.transform_data(df, trm_value)

            # Validar datos transformados
            validation_result = self.validate_transformed_data(df_transformed)

            if not validation_result['is_valid']:
                logger.error("Datos transformados no pasaron validación")
                for error in validation_result['errors']:
                    logger.error(f"Error de validación: {error}")

            # Generar resumen
            transformation_summary = self.get_transformation_summary(df, df_transformed)

            logger.info("Transformación específica optimizada de OtakuLATAM completada")

            return df_transformed, {
                'validation': validation_result,
                'summary': transformation_summary
            }

        except Exception as e:
            logger.error(f"Error en transformación específica optimizada de OtakuLATAM: {e}")
            raise

# Alias para compatibilidad
DataTransformer = DataTransformerOptimized
OtakuDataTransformer = OtakuDataTransformerOptimized

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

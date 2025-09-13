"""
Módulo Loader del ETL OtakuLATAM
Responsable de cargar los datos transformados a diferentes destinos
"""
import pandas as pd
import json
import os
import logging
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import sys

# Agregar el directorio backend al path para importar utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.date_utils import (
    ensure_mysql_datetime_format, 
    convert_dataframe_datetime_columns,
    format_datetime_for_json,
    get_timestamp_for_filename,
    validate_mysql_datetime
)

logger = logging.getLogger(__name__)

class DataLoader:
    """Cargador de datos para el ETL"""
    
    def __init__(self, output_dir="data/output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_to_json(self, df, file_path=None, orient='records', indent=2):
        """
        Guarda DataFrame como archivo JSON
        
        Args:
            df (pd.DataFrame): DataFrame a guardar
            file_path (str, optional): Ruta del archivo. Si no se proporciona, se genera automáticamente
            orient (str): Orientación del JSON ('records', 'index', 'values', etc.)
            indent (int): Indentación del JSON
        
        Returns:
            str: Ruta del archivo guardado
        """
        try:
            if file_path is None:
                timestamp = get_timestamp_for_filename()
                file_path = self.output_dir / f"transformed_data_{timestamp}.json"
            else:
                file_path = Path(file_path)
            
            logger.info(f"Guardando datos en JSON: {file_path}")
            
            # Preparar datos para JSON
            df_json = self._prepare_dataframe_for_json(df)
            
            # Convertir a JSON
            json_data = df_json.to_json(orient=orient, date_format='iso', indent=indent)
            
            # Guardar archivo
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(json_data)
            
            file_size = os.path.getsize(file_path)
            logger.info(f"Archivo JSON guardado exitosamente: {file_path} ({file_size} bytes)")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error guardando JSON: {e}")
            raise
    
    def save_to_parquet(self, df, file_path=None, compression='snappy'):
        """
        Guarda DataFrame como archivo Parquet
        
        Args:
            df (pd.DataFrame): DataFrame a guardar
            file_path (str, optional): Ruta del archivo
            compression (str): Tipo de compresión ('snappy', 'gzip', 'brotli', etc.)
        
        Returns:
            str: Ruta del archivo guardado
        """
        try:
            if file_path is None:
                timestamp = get_timestamp_for_filename()
                file_path = self.output_dir / f"transformed_data_{timestamp}.parquet"
            else:
                file_path = Path(file_path)
            
            logger.info(f"Guardando datos en Parquet: {file_path}")
            
            # Preparar datos para Parquet
            df_parquet = self._prepare_dataframe_for_parquet(df)
            
            # Guardar como Parquet
            df_parquet.to_parquet(
                file_path, 
                compression=compression, 
                index=False,
                engine='pyarrow'
            )
            
            file_size = os.path.getsize(file_path)
            logger.info(f"Archivo Parquet guardado exitosamente: {file_path} ({file_size} bytes)")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error guardando Parquet: {e}")
            raise
    
    def save_to_csv(self, df, file_path=None, encoding='utf-8', sep=','):
        """
        Guarda DataFrame como archivo CSV
        
        Args:
            df (pd.DataFrame): DataFrame a guardar
            file_path (str, optional): Ruta del archivo
            encoding (str): Codificación del archivo
            sep (str): Separador de columnas
        
        Returns:
            str: Ruta del archivo guardado
        """
        try:
            if file_path is None:
                timestamp = get_timestamp_for_filename()
                file_path = self.output_dir / f"transformed_data_{timestamp}.csv"
            else:
                file_path = Path(file_path)
            
            logger.info(f"Guardando datos en CSV: {file_path}")
            
            # Guardar como CSV
            df.to_csv(file_path, encoding=encoding, sep=sep, index=False)
            
            file_size = os.path.getsize(file_path)
            logger.info(f"Archivo CSV guardado exitosamente: {file_path} ({file_size} bytes)")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error guardando CSV: {e}")
            raise
    
    def _prepare_dataframe_for_json(self, df):
        """
        Prepara DataFrame para exportación JSON
        
        Args:
            df (pd.DataFrame): DataFrame original
        
        Returns:
            pd.DataFrame: DataFrame preparado
        """
        try:
            df_json = df.copy()
            
            # Convertir columnas datetime usando utilidad centralizada
            datetime_columns = []
            for col in df_json.columns:
                if df_json[col].dtype == 'datetime64[ns]':
                    datetime_columns.append(col)
                elif col in ['fecha_procesamiento', 'Processing Date', 'timestamp']:
                    datetime_columns.append(col)
            
            # Usar utilidad para conversión consistente
            if datetime_columns:
                for col in datetime_columns:
                    if col in df_json.columns:
                        df_json[col] = df_json[col].apply(lambda x: format_datetime_for_json(x) if pd.notna(x) else None)
                        logger.info(f"Convertida columna '{col}' para JSON")
            
            # Convertir decimales a float para JSON
            for col in df_json.columns:
                if df_json[col].dtype == 'object':
                    try:
                        # Intentar convertir strings numéricos
                        df_json[col] = pd.to_numeric(df_json[col], errors='ignore')
                    except:
                        pass
            
            return df_json
            
        except Exception as e:
            logger.error(f"Error preparando DataFrame para JSON: {e}")
            return df
    
    def _prepare_dataframe_for_parquet(self, df):
        """
        Prepara DataFrame para exportación Parquet
        
        Args:
            df (pd.DataFrame): DataFrame original
        
        Returns:
            pd.DataFrame: DataFrame preparado
        """
        try:
            df_parquet = df.copy()
            
            # Parquet maneja mejor los tipos de datos, menos preparación necesaria
            # Convertir object columns que son realmente categóricas
            for col in df_parquet.columns:
                if df_parquet[col].dtype == 'object':
                    unique_values = df_parquet[col].nunique()
                    total_values = len(df_parquet)
                    
                    # Si hay pocas categorías únicas, convertir a categorical
                    if unique_values < total_values * 0.5:
                        df_parquet[col] = df_parquet[col].astype('category')
            
            return df_parquet
            
        except Exception as e:
            logger.error(f"Error preparando DataFrame para Parquet: {e}")
            return df
    
    def load_to_database(self, df, db_manager, table_name='personas_transformadas'):
        """
        Carga datos a la base de datos
        
        Args:
            df (pd.DataFrame): DataFrame con datos
            db_manager: Instancia del manejador de base de datos
            table_name (str): Nombre de la tabla destino
        
        Returns:
            int: Número de registros insertados
        """
        try:
            # Validar que df sea un DataFrame
            if not hasattr(df, 'iterrows'):
                error_msg = f"El parámetro 'df' debe ser un DataFrame, pero se recibió un objeto de tipo {type(df)}"
                logger.error(error_msg)
                raise TypeError(error_msg)
            
            logger.info(f"Cargando {len(df)} registros a la base de datos")
            
            # Preparar datos para inserción
            records_data = self._prepare_dataframe_for_database(df)
            
            # Insertar en la base de datos
            rows_inserted = db_manager.insertar_personas_transformadas(records_data)
            
            logger.info(f"Datos cargados exitosamente: {rows_inserted} registros insertados")
            
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Error cargando datos a la base de datos: {e}")
            raise
    
    def _prepare_dataframe_for_database(self, df):
        """
        Prepara DataFrame para inserción en base de datos
        
        Args:
            df (pd.DataFrame): DataFrame original
        
        Returns:
            list: Lista de diccionarios con datos preparados
        """
        try:
            # Validar columnas esperadas
            expected_columns = ['Name', 'nombre', 'age', 'edad_anos', 'Age Lustrum', 'edad_lustros',
                                'Gender', 'gender', 'Gender Es', 'genero_es', 'income', 'ingreso_usd',
                                'Income Cop', 'ingreso_cop', 'Trm', 'trm_utilizada', 'Illness', 'enfermedad_original',
                                'Illness Es', 'enfermedad_es', 'Processing Date', 'fecha_procesamiento']
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Columnas esperadas faltantes en DataFrame: {missing_columns}")
            
            records = []
            
            # Primero convertir todas las fechas a formato MySQL
            df_db = df.copy()
            
            # Identificar y convertir columnas de fecha
            date_columns = ['fecha_procesamiento', 'Processing Date']
            for col in date_columns:
                if col in df_db.columns:
                    df_db[col] = df_db[col].apply(ensure_mysql_datetime_format)
                    logger.info(f"Convertida columna de fecha '{col}' a formato MySQL para base de datos")
            
            for _, row in df_db.iterrows():
                # Mapear columnas según la estructura actual del transformer
                record = {}
                
                # Mapear campos básicos
                if 'Name' in row:
                    record['nombre'] = str(row['Name'])
                elif 'nombre' in row:
                    record['nombre'] = str(row['nombre'])
                else:
                    record['nombre'] = ''
                
                if 'age' in row:
                    record['edad_anos'] = int(row['age']) if pd.notna(row['age']) else 0
                elif 'edad_anos' in row:
                    record['edad_anos'] = int(row['edad_anos']) if pd.notna(row['edad_anos']) else 0
                else:
                    record['edad_anos'] = 0
                
                if 'Age Lustrum' in row:
                    record['edad_lustros'] = float(row['Age Lustrum']) if pd.notna(row['Age Lustrum']) else 0.0
                elif 'edad_lustros' in row:
                    record['edad_lustros'] = float(row['edad_lustros']) if pd.notna(row['edad_lustros']) else 0.0
                else:
                    record['edad_lustros'] = 0.0
                
                if 'Gender' in row:
                    record['genero_original'] = str(row['Gender'])
                elif 'gender' in row:
                    record['genero_original'] = str(row['gender'])
                else:
                    record['genero_original'] = ''
                
                if 'Gender Es' in row:
                    record['genero_es'] = str(row['Gender Es'])
                elif 'genero_es' in row:
                    record['genero_es'] = str(row['genero_es'])
                else:
                    record['genero_es'] = ''
                
                if 'income' in row:
                    record['ingreso_usd'] = float(row['income']) if pd.notna(row['income']) else 0.0
                elif 'ingreso_usd' in row:
                    record['ingreso_usd'] = float(row['ingreso_usd']) if pd.notna(row['ingreso_usd']) else 0.0
                else:
                    record['ingreso_usd'] = 0.0
                
                if 'Income Cop' in row:
                    record['ingreso_cop'] = float(row['Income Cop']) if pd.notna(row['Income Cop']) else 0.0
                elif 'ingreso_cop' in row:
                    record['ingreso_cop'] = float(row['ingreso_cop']) if pd.notna(row['ingreso_cop']) else 0.0
                else:
                    record['ingreso_cop'] = 0.0
                
                if 'Trm' in row:
                    record['trm_utilizada'] = float(row['Trm']) if pd.notna(row['Trm']) else 0.0
                elif 'trm_utilizada' in row:
                    record['trm_utilizada'] = float(row['trm_utilizada']) if pd.notna(row['trm_utilizada']) else 0.0
                else:
                    record['trm_utilizada'] = 0.0
                
                if 'Illness' in row:
                    record['enfermedad_original'] = str(row['Illness'])
                elif 'enfermedad_original' in row:
                    record['enfermedad_original'] = str(row['enfermedad_original'])
                else:
                    record['enfermedad_original'] = ''
                
                if 'Illness Es' in row:
                    record['enfermedad_es'] = str(row['Illness Es'])
                elif 'enfermedad_es' in row:
                    record['enfermedad_es'] = str(row['enfermedad_es'])
                else:
                    record['enfermedad_es'] = ''
                
                # Manejar fecha de procesamiento con validación
                fecha_proc = None
                if 'Processing Date' in row:
                    fecha_proc = row['Processing Date']
                elif 'fecha_procesamiento' in row:
                    fecha_proc = row['fecha_procesamiento']
                
                if fecha_proc is not None:
                    mysql_date = ensure_mysql_datetime_format(fecha_proc)
                    if validate_mysql_datetime(mysql_date):
                        record['fecha_procesamiento'] = mysql_date
                    else:
                        logger.warning(f"Fecha inválida detectada: {fecha_proc}, usando fecha actual")
                        record['fecha_procesamiento'] = ensure_mysql_datetime_format(None)
                else:
                    record['fecha_procesamiento'] = ensure_mysql_datetime_format(None)
                
                records.append(record)
            
            logger.info(f"Preparados {len(records)} registros para inserción en base de datos")
            return records
            
        except Exception as e:
            logger.error(f"Error preparando datos para base de datos: {e}")
            raise
    
    def generate_sql_insert_script(self, df, table_name='personas_transformadas', file_path=None):
        """
        Genera script SQL de inserción
        
        Args:
            df (pd.DataFrame): DataFrame con datos
            table_name (str): Nombre de la tabla
            file_path (str, optional): Ruta del archivo SQL
        
        Returns:
            str: Ruta del archivo SQL generado
        """
        try:
            if file_path is None:
                timestamp = get_timestamp_for_filename()
                file_path = self.output_dir / f"insert_script_{timestamp}.sql"
            else:
                file_path = Path(file_path)
            
            logger.info(f"Generando script SQL: {file_path}")
            
            # Generar script SQL
            sql_lines = []
            sql_lines.append(f"-- Script de inserción generado automáticamente")
            sql_lines.append(f"-- Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            sql_lines.append(f"-- Registros: {len(df)}")
            sql_lines.append("")
            sql_lines.append(f"USE otaku;")
            sql_lines.append("")
            
            # Preparar datos
            records_data = self._prepare_dataframe_for_database(df)
            
            # Generar INSERTs
            for record in records_data:
                values = []
                for key, value in record.items():
                    if isinstance(value, str):
                        # Escapar comillas simples
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    elif value is None:
                        values.append("NULL")
                    else:
                        values.append(str(value))
                
                sql_line = f"INSERT INTO {table_name} "
                sql_line += f"({', '.join(record.keys())}) "
                sql_line += f"VALUES ({', '.join(values)});"
                sql_lines.append(sql_line)
            
            # Guardar archivo
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(sql_lines))
            
            file_size = os.path.getsize(file_path)
            logger.info(f"Script SQL generado exitosamente: {file_path} ({file_size} bytes)")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error generando script SQL: {e}")
            raise
    
    def create_data_summary(self, df, file_path=None):
        """
        Crea un resumen de los datos procesados
        
        Args:
            df (pd.DataFrame): DataFrame con datos
            file_path (str, optional): Ruta del archivo de resumen
        
        Returns:
            str: Ruta del archivo de resumen
        """
        try:
            if file_path is None:
                timestamp = get_timestamp_for_filename()
                file_path = self.output_dir / f"data_summary_{timestamp}.json"
            else:
                file_path = Path(file_path)
            
            logger.info(f"Creando resumen de datos: {file_path}")
            
            # Generar estadísticas
            summary = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_records': len(df),
                    'total_columns': len(df.columns),
                    'columns': list(df.columns)
                },
                'statistics': {
                    'age_stats': {
                        'avg_years': float(df['edad_anos'].mean()) if 'edad_anos' in df.columns else None,
                        'avg_lustros': float(df['edad_lustros'].mean()) if 'edad_lustros' in df.columns else None,
                        'min_years': int(df['edad_anos'].min()) if 'edad_anos' in df.columns else None,
                        'max_years': int(df['edad_anos'].max()) if 'edad_anos' in df.columns else None
                    },
                    'income_stats': {
                        'avg_usd': float(df['ingreso_usd'].mean()) if 'ingreso_usd' in df.columns else None,
                        'avg_cop': float(df['ingreso_cop'].mean()) if 'ingreso_cop' in df.columns else None,
                        'min_usd': float(df['ingreso_usd'].min()) if 'ingreso_usd' in df.columns else None,
                        'max_usd': float(df['ingreso_usd'].max()) if 'ingreso_usd' in df.columns else None
                    },
                    'gender_distribution': df['genero_es'].value_counts().to_dict() if 'genero_es' in df.columns else {},
                    'illness_distribution': df['enfermedad_es'].value_counts().to_dict() if 'enfermedad_es' in df.columns else {},
                    'trm_used': float(df['trm_utilizada'].iloc[0]) if 'trm_utilizada' in df.columns and len(df) > 0 else None
                }
            }
            
            # Guardar resumen
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Resumen de datos creado exitosamente: {file_path}")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error creando resumen de datos: {e}")
            raise

class OtakuDataLoader(DataLoader):
    """Cargador específico para datos de OtakuLATAM"""
    
    def __init__(self, output_dir="data/output"):
        super().__init__(output_dir)
    
    def load_otaku_data(self, df, db_manager=None, cloud_service=None):
        """
        Carga datos de OtakuLATAM a todos los destinos requeridos
        
        Args:
            df (pd.DataFrame): DataFrame con datos transformados
            db_manager: Manejador de base de datos
            cloud_service: Servicio de almacenamiento en nube
        
        Returns:
            dict: Resultado de las operaciones de carga
        """
        try:
            # Validar que df sea un DataFrame
            if not hasattr(df, 'iterrows'):
                error_msg = f"El parámetro 'df' debe ser un DataFrame, pero se recibió un objeto de tipo {type(df)}"
                logger.error(error_msg)
                raise TypeError(error_msg)
            
            logger.info("Iniciando carga de datos de OtakuLATAM")
            
            results = {
                'json_file': None,
                'parquet_file': None,
                'csv_file': None,
                'sql_script': None,
                'summary_file': None,
                'database_records': 0,
                'cloud_url': None,
                'errors': []
            }
            
            # Guardar como JSON
            try:
                results['json_file'] = self.save_to_json(df)
            except Exception as e:
                results['errors'].append(f"Error guardando JSON: {e}")
            
            # Guardar como Parquet
            try:
                results['parquet_file'] = self.save_to_parquet(df)
            except Exception as e:
                results['errors'].append(f"Error guardando Parquet: {e}")
            
            # Guardar como CSV (adicional)
            try:
                results['csv_file'] = self.save_to_csv(df)
            except Exception as e:
                results['errors'].append(f"Error guardando CSV: {e}")
            
            # Generar script SQL
            try:
                results['sql_script'] = self.generate_sql_insert_script(df)
            except Exception as e:
                results['errors'].append(f"Error generando script SQL: {e}")
            
            # Crear resumen
            try:
                results['summary_file'] = self.create_data_summary(df)
            except Exception as e:
                results['errors'].append(f"Error creando resumen: {e}")
            
            # Cargar a base de datos
            if db_manager:
                try:
                    results['database_records'] = self.load_to_database(df, db_manager)
                except Exception as e:
                    results['errors'].append(f"Error cargando a base de datos: {e}")
            
            # Subir JSON a la nube
            if cloud_service and results['json_file']:
                try:
                    results['cloud_url'] = cloud_service.upload_file(
                        results['json_file'], 
                        f"otaku_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    )
                except Exception as e:
                    results['errors'].append(f"Error subiendo a la nube: {e}")
            
            logger.info("Carga de datos de OtakuLATAM completada")
            
            return results
            
        except Exception as e:
            logger.error(f"Error en carga de datos de OtakuLATAM: {e}")
            raise

# Función de conveniencia
def load_otaku_data(df, output_dir="data/output", db_manager=None, cloud_service=None):
    """
    Función de conveniencia para cargar datos de OtakuLATAM
    
    Args:
        df (pd.DataFrame): DataFrame con datos transformados
        output_dir (str): Directorio de salida
        db_manager: Manejador de base de datos
        cloud_service: Servicio de nube
    
    Returns:
        dict: Resultado de las operaciones de carga
    """
    loader = OtakuDataLoader(output_dir)
    return loader.load_otaku_data(df, db_manager, cloud_service)

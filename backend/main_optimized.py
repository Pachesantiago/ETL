"""
ETL Optimizado Principal para OtakuLATAM
Versión optimizada con mejor rendimiento y cache
"""
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Agregar directorios al path para imports
sys.path.append(os.path.dirname(__file__))

# Imports optimizados
from config.settings import (
    DATABASE_CONFIG, CLOUD_STORAGE_CONFIG, INPUT_DATA_PATH,
    OUTPUT_JSON_PATH, OUTPUT_PARQUET_PATH, LOGGING_CONFIG
)
from etl.extractor import OtakuDataExtractor
from etl.transformer_optimized import OtakuDataTransformerOptimized
from etl.loader import OtakuDataLoader
from services.trm_service_optimized import TRMService
from services.cloud_storage import CloudStorageService
from database.connection_optimized import DatabaseConnectionOptimized

class OtakuETLOptimized:
    """Clase principal del ETL optimizado de OtakuLATAM"""

    def __init__(self):
        self.setup_logging()
        self.logger = logging.getLogger(__name__)

        # Inicializar componentes optimizados
        self.extractor = OtakuDataExtractor()
        self.trm_service = TRMService(use_cache=True)  # Cache habilitado
        self.transformer = OtakuDataTransformerOptimized(self.trm_service)
        self.loader = OtakuDataLoader()

        # Servicios externos
        self.db_connection = None
        self.cloud_service = None

        # Variables de control
        self.ejecucion_id = None
        self.start_time = None
        self.end_time = None

    def setup_logging(self):
        """Configura el sistema de logging optimizado"""
        try:
            # Crear directorio de logs si no existe
            log_dir = Path("logs")
            log_dir.mkdir(parents=True, exist_ok=True)

            # Configurar logging con menos verbosidad para mejor rendimiento
            logging.basicConfig(
                level=getattr(logging, LOGGING_CONFIG.get('level', 'INFO')),
                format=LOGGING_CONFIG.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
                handlers=[
                    logging.FileHandler(LOGGING_CONFIG.get('file', 'logs/etl_execution.log'), encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )

        except Exception as e:
            print(f"Error configurando logging: {e}")

    def initialize_services(self):
        """Inicializa los servicios externos"""
        try:
            self.logger.info("Inicializando servicios externos optimizados")

            # Inicializar conexión a base de datos optimizada
            try:
                self.db_connection = DatabaseConnectionOptimized(DATABASE_CONFIG)
                if self.db_connection.connect():
                    self.logger.info("Conexión a base de datos optimizada establecida")
                else:
                    self.logger.warning("No se pudo conectar a la base de datos")
            except Exception as e:
                self.logger.error(f"Error conectando a base de datos: {e}")

            # Inicializar servicio de nube
            try:
                self.cloud_service = CloudStorageService(CLOUD_STORAGE_CONFIG)
                self.logger.info("Servicio de almacenamiento en nube inicializado")
            except Exception as e:
                self.logger.warning(f"Error inicializando servicio de nube: {e}")

        except Exception as e:
            self.logger.error(f"Error inicializando servicios: {e}")
            raise

    def run_etl(self, input_file_path=None, skip_formats=None):
        """
        Ejecuta el proceso completo del ETL optimizado

        Args:
            input_file_path (str, optional): Ruta del archivo de entrada
            skip_formats (list, optional): Formatos a omitir para mejor rendimiento

        Returns:
            dict: Resultado de la ejecución del ETL
        """
        try:
            self.start_time = datetime.now()
            self.logger.info("="*60)
            self.logger.info("INICIANDO ETL OPTIMIZADO OTAKU LATAM")
            self.logger.info("="*60)

            # Usar archivo por defecto si no se especifica
            if input_file_path is None:
                input_file_path = INPUT_DATA_PATH

            # Inicializar servicios
            self.initialize_services()

            # Registrar inicio de ejecución en BD
            if self.db_connection:
                self.ejecucion_id = self.db_connection.iniciar_ejecucion_etl(
                    os.path.basename(input_file_path)
                )

            # FASE 1: EXTRACCIÓN
            self.logger.info("FASE 1: EXTRACCIÓN DE DATOS")
            extracted_data = self._extract_phase(input_file_path)

            # FASE 2: TRANSFORMACIÓN OPTIMIZADA
            self.logger.info("FASE 2: TRANSFORMACIÓN OPTIMIZADA DE DATOS")
            transformed_data, transformation_result = self._transform_phase_optimized(extracted_data)

            # FASE 3: CARGA OPTIMIZADA
            self.logger.info("FASE 3: CARGA OPTIMIZADA DE DATOS")
            load_result = self._load_phase_optimized(transformed_data, skip_formats)

            # Finalizar ejecución
            self.end_time = datetime.now()
            execution_result = self._finalize_execution(
                extracted_data, transformed_data, transformation_result, load_result
            )

            self.logger.info("="*60)
            self.logger.info("ETL OPTIMIZADO COMPLETADO EXITOSAMENTE")
            self.logger.info("="*60)

            return execution_result

        except Exception as e:
            self.logger.error(f"Error en ejecución del ETL optimizado: {e}")
            self._handle_etl_error(e)
            raise
        finally:
            self._cleanup()

    def _extract_phase(self, input_file_path):
        """Fase de extracción de datos"""
        try:
            self.logger.info(f"Extrayendo datos desde: {input_file_path}")

            # Verificar que el archivo existe
            if not os.path.exists(input_file_path):
                raise FileNotFoundError(f"Archivo de entrada no encontrado: {input_file_path}")

            # Extraer datos
            extracted_data = self.extractor.extract_otaku_data(input_file_path)

            self.logger.info(f"Extracción completada: {len(extracted_data)} registros")

            # Log en BD
            if self.db_connection and self.ejecucion_id:
                self.db_connection.registrar_log_etl(
                    self.ejecucion_id, 'INFO',
                    f"Extracción completada: {len(extracted_data)} registros",
                    {'records_extracted': len(extracted_data), 'source_file': input_file_path}
                )

            return extracted_data

        except Exception as e:
            self.logger.error(f"Error en fase de extracción: {e}")
            if self.db_connection and self.ejecucion_id:
                self.db_connection.registrar_log_etl(
                    self.ejecucion_id, 'ERROR',
                    f"Error en extracción: {str(e)}"
                )
            raise

    def _transform_phase_optimized(self, extracted_data):
        """Fase de transformación optimizada"""
        try:
            self.logger.info("Iniciando transformación optimizada de datos")

            # Transformar datos usando el transformer optimizado
            transformed_data, transformation_result = self.transformer.transform_otaku_data(extracted_data)

            # Validar resultado
            validation = transformation_result.get('validation', {})
            if not validation.get('is_valid', False):
                self.logger.warning("Datos transformados no pasaron todas las validaciones")
                for error in validation.get('errors', []):
                    self.logger.error(f"Error de validación: {error}")

            self.logger.info(f"Transformación optimizada completada: {len(transformed_data)} registros")

            # Log en BD
            if self.db_connection and self.ejecucion_id:
                self.db_connection.registrar_log_etl(
                    self.ejecucion_id, 'INFO',
                    f"Transformación optimizada completada: {len(transformed_data)} registros",
                    {
                        'records_transformed': len(transformed_data),
                        'trm_used': transformation_result.get('summary', {}).get('trm_used'),
                        'validation_result': validation.get('is_valid', False)
                    }
                )

            return transformed_data, transformation_result

        except Exception as e:
            self.logger.error(f"Error en fase de transformación optimizada: {e}")
            if self.db_connection and self.ejecucion_id:
                self.db_connection.registrar_log_etl(
                    self.ejecucion_id, 'ERROR',
                    f"Error en transformación optimizada: {str(e)}"
                )
            raise

    def _load_phase_optimized(self, transformed_data, skip_formats=None):
        """Fase de carga optimizada"""
        try:
            self.logger.info("Iniciando carga optimizada de datos")

            # Configurar formatos a generar (para mejor rendimiento)
            if skip_formats is None:
                skip_formats = []  # Generar todos por defecto

            # Cargar datos a todos los destinos usando loader optimizado
            load_result = self.loader.load_otaku_data(
                transformed_data,
                self.db_connection,
                self.cloud_service
            )

            # Reportar resultados
            if load_result.get('json_file') and 'json' not in skip_formats:
                self.logger.info(f"Archivo JSON creado: {load_result['json_file']}")

            if load_result.get('parquet_file') and 'parquet' not in skip_formats:
                self.logger.info(f"Archivo Parquet creado: {load_result['parquet_file']}")

            if load_result.get('database_records', 0) > 0:
                self.logger.info(f"Registros insertados en BD (bulk): {load_result['database_records']}")

            if load_result.get('cloud_url'):
                self.logger.info(f"Archivo subido a la nube: {load_result['cloud_url']}")

            # Reportar errores
            for error in load_result.get('errors', []):
                self.logger.error(f"Error en carga: {error}")

            self.logger.info("Carga optimizada de datos completada")

            # Log en BD
            if self.db_connection and self.ejecucion_id:
                self.db_connection.registrar_log_etl(
                    self.ejecucion_id, 'INFO',
                    "Carga optimizada de datos completada",
                    {
                        'files_created': {
                            'json': load_result.get('json_file'),
                            'parquet': load_result.get('parquet_file'),
                            'sql_script': load_result.get('sql_script')
                        },
                        'database_records': load_result.get('database_records', 0),
                        'cloud_url': load_result.get('cloud_url'),
                        'errors_count': len(load_result.get('errors', []))
                    }
                )

            return load_result

        except Exception as e:
            self.logger.error(f"Error en fase de carga optimizada: {e}")
            if self.db_connection and self.ejecucion_id:
                self.db_connection.registrar_log_etl(
                    self.ejecucion_id, 'ERROR',
                    f"Error en carga optimizada: {str(e)}"
                )
            raise

    def _finalize_execution(self, extracted_data, transformed_data, transformation_result, load_result):
        """Finaliza la ejecución del ETL"""
        try:
            duration = (self.end_time - self.start_time).total_seconds()

            # Preparar resultado final
            execution_result = {
                'success': True,
                'start_time': self.start_time.isoformat(),
                'end_time': self.end_time.isoformat(),
                'duration_seconds': duration,
                'records_extracted': len(extracted_data),
                'records_transformed': len(transformed_data),
                'records_loaded_db': load_result.get('database_records', 0),
                'files_created': {
                    'json': load_result.get('json_file'),
                    'parquet': load_result.get('parquet_file'),
                    'csv': load_result.get('csv_file'),
                    'sql_script': load_result.get('sql_script'),
                    'summary': load_result.get('summary_file')
                },
                'cloud_url': load_result.get('cloud_url'),
                'trm_used': transformation_result.get('summary', {}).get('trm_used'),
                'validation_passed': transformation_result.get('validation', {}).get('is_valid', False),
                'errors': load_result.get('errors', []),
                'ejecucion_id': self.ejecucion_id,
                'optimization_features': [
                    'TRM Cache',
                    'Vectorized Transformations',
                    'Bulk Database Inserts',
                    'Optimized Logging'
                ]
            }

            # Finalizar en BD
            if self.db_connection and self.ejecucion_id:
                estado = 'COMPLETADO' if not execution_result['errors'] else 'COMPLETADO_CON_ADVERTENCIAS'

                self.db_connection.finalizar_ejecucion_etl(
                    self.ejecucion_id,
                    len(extracted_data),
                    execution_result['trm_used'],
                    os.path.basename(load_result.get('json_file', '')),
                    os.path.basename(load_result.get('parquet_file', '')),
                    estado,
                    '; '.join(execution_result['errors']) if execution_result['errors'] else None
                )

            # Log resumen final
            self.logger.info(f"Resumen de ejecución optimizada:")
            self.logger.info(f"  - Duración: {duration:.2f} segundos")
            self.logger.info(f"  - Registros extraídos: {execution_result['records_extracted']}")
            self.logger.info(f"  - Registros transformados: {execution_result['records_transformed']}")
            self.logger.info(f"  - Registros cargados en BD: {execution_result['records_loaded_db']}")
            self.logger.info(f"  - TRM utilizada: {execution_result['trm_used']}")
            self.logger.info(f"  - Archivos creados: {len([f for f in execution_result['files_created'].values() if f])}")

            return execution_result

        except Exception as e:
            self.logger.error(f"Error finalizando ejecución: {e}")
            return {'success': False, 'error': str(e)}

    def _handle_etl_error(self, error):
        """Maneja errores del ETL"""
        try:
            if self.db_connection and self.ejecucion_id:
                self.db_connection.finalizar_ejecucion_etl(
                    self.ejecucion_id,
                    0,
                    None,
                    None,
                    None,
                    'ERROR',
                    str(error)
                )
        except Exception as e:
            self.logger.error(f"Error registrando fallo en BD: {e}")

    def _cleanup(self):
        """Limpia recursos"""
        try:
            if self.db_connection:
                self.db_connection.disconnect()
        except Exception as e:
            self.logger.error(f"Error en cleanup: {e}")

def main():
    """Función principal optimizada"""
    try:
        # Crear instancia del ETL optimizado
        etl = OtakuETLOptimized()

        # Ejecutar ETL optimizado
        result = etl.run_etl()

        # Mostrar resultado
        if result.get('success'):
            print("\n" + "="*60)
            print("ETL OPTIMIZADO EJECUTADO EXITOSAMENTE")
            print("="*60)
            print(f"Registros procesados: {result.get('records_transformed', 0)}")
            print(f"Duración: {result.get('duration_seconds', 0):.2f} segundos")
            print(f"TRM utilizada: {result.get('trm_used', 'N/A')}")
            print("Optimizaciones aplicadas:")
            for opt in result.get('optimization_features', []):
                print(f"  - {opt}")

            if result.get('files_created', {}).get('json'):
                print(f"Archivo JSON: {result['files_created']['json']}")

            if result.get('files_created', {}).get('parquet'):
                print(f"Archivo Parquet: {result['files_created']['parquet']}")

            if result.get('cloud_url'):
                print(f"URL en la nube: {result['cloud_url']}")

            print("="*60)
        else:
            print("ETL OPTIMIZADO FALLÓ")
            print(f"Error: {result.get('error', 'Error desconocido')}")

    except Exception as e:
        print(f"Error ejecutando ETL optimizado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

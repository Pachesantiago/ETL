"""
API REST para el ETL OtakuLATAM
Conecta el frontend con el backend ETL y la base de datos
"""
import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd

# Agregar el directorio padre al path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Imports del proyecto
from config.settings import DATABASE_CONFIG, INPUT_DATA_PATH, OUTPUT_JSON_PATH
from etl.extractor import OtakuDataExtractor
from etl.transformer import OtakuDataTransformer
from etl.loader import OtakuDataLoader
from services.trm_service import TRMService
from database.connection import DatabaseConnection
from api.cloud_routes import cloud_bp

app = Flask(__name__)
CORS(app)  # Permitir requests desde el frontend

# Registrar blueprints
app.register_blueprint(cloud_bp)

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLAPIServer:
    """Servidor API para el ETL OtakuLATAM"""
    
    def __init__(self):
        self.extractor = OtakuDataExtractor()
        self.trm_service = TRMService()
        self.transformer = OtakuDataTransformer(self.trm_service)
        self.loader = OtakuDataLoader()
        self.db_connection = None
        
        # Inicializar conexión a base de datos
        try:
            self.db_connection = DatabaseConnection(DATABASE_CONFIG)
            if self.db_connection.connect():
                logger.info("Conexión a base de datos establecida")
            else:
                logger.warning("No se pudo establecer conexión a la base de datos")
                self.db_connection = None
        except Exception as e:
            logger.warning(f"No se pudo conectar a la base de datos: {e}")
            self.db_connection = None

# Instancia global del servidor ETL
etl_server = ETLAPIServer()

@app.route('/health', methods=['GET'])
def health_check():
    """Verificar estado del servidor"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database_connected': etl_server.db_connection is not None,
        'version': '1.0.0'
    })

@app.route('/api/latest-data', methods=['GET'])
def get_latest_data():
    """Obtener los datos más recientes procesados"""
    try:
        # Buscar el archivo JSON más reciente
        output_dir = Path('data/output')
        json_files = list(output_dir.glob('transformed_data_*.json'))
        
        if not json_files:
            return jsonify({'error': 'No hay datos procesados disponibles'}), 404
        
        # Obtener el archivo más reciente
        latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
        
        with open(latest_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return jsonify({
            'data': data,
            'file': str(latest_file),
            'timestamp': datetime.fromtimestamp(latest_file.stat().st_mtime).isoformat(),
            'records_count': len(data)
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo datos más recientes: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/process-file', methods=['POST'])
def process_file():
    """Procesar archivo subido desde el frontend"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No se encontró archivo'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No se seleccionó archivo'}), 400

        # Leer parámetro opcional para insertar en BD
        insert_to_database = request.form.get('insert_to_database', 'false').lower() == 'true'

        # Guardar archivo temporalmente
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_filename = f"temp_{timestamp}_{file.filename}"
        temp_path = Path('data/input') / temp_filename

        # Crear directorio si no existe
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        file.save(temp_path)

        logger.info(f"Archivo guardado temporalmente: {temp_path}")
        logger.info(f"Insertar en base de datos: {insert_to_database}")

        # Resetear conexión a base de datos antes de procesar
        if etl_server.db_connection:
            try:
                if not etl_server.db_connection.is_connected():
                    logger.info("Reconectando a base de datos antes de procesar archivo...")
                    etl_server.db_connection.disconnect()
                    if not etl_server.db_connection.connect():
                        logger.warning("No se pudo reconectar a la base de datos")
                        etl_server.db_connection = None
                else:
                    try:
                        etl_server.db_connection.connection.rollback()
                        logger.info("Rollback ejecutado exitosamente antes de procesar archivo")
                    except Exception as rollback_error:
                        logger.warning(f"Error en rollback (ignorando) antes de procesar archivo: {rollback_error}")
            except Exception as e:
                logger.warning(f"Error reseteando conexión a BD antes de procesar archivo: {e}")
                etl_server.db_connection = None

        # Procesar archivo con ETL
        result = process_etl_pipeline(str(temp_path), insert_to_database=insert_to_database)

        # Limpiar archivo temporal
        if temp_path.exists():
            temp_path.unlink()

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error procesando archivo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/run-etl', methods=['POST'])
def run_etl():
    """Ejecutar ETL completo - requiere archivo específico"""
    try:
        # Verificar si se proporcionó un archivo en la request
        data = request.get_json() if request.is_json else {}
        input_file = data.get('input_file')
        
        if not input_file:
            # Solo usar archivo por defecto si se solicita explícitamente
            if data.get('use_sample_data', False):
                input_file = 'data/input/sample_data.csv'
            else:
                return jsonify({
                    'success': False,
                    'error': 'No se especificó archivo de entrada. Use "use_sample_data": true para usar datos de ejemplo o proporcione "input_file".'
                }), 400
        
        result = process_etl_pipeline(input_file)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error ejecutando ETL: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/database/records', methods=['GET'])
def get_database_records():
    """Obtener registros desde la base de datos"""
    try:
        if not etl_server.db_connection:
            return jsonify({'error': 'Base de datos no disponible'}), 503
        
        # Obtener registros de la base de datos
        query = """
        SELECT nombre, edad_anos, edad_lustros, genero_original, genero_es, ingreso_usd, ingreso_cop, trm_utilizada, enfermedad_original, enfermedad_es, fecha_procesamiento
        FROM personas_transformadas
        ORDER BY fecha_procesamiento DESC
        LIMIT 1000
        """
        
        cursor = etl_server.db_connection.connection.cursor(dictionary=True)
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        
        # Convertir datetime a string para JSON
        for record in records:
            if 'fecha_procesamiento' in record and record['fecha_procesamiento']:
                record['fecha_procesamiento'] = record['fecha_procesamiento'].isoformat()
        
        return jsonify({
            'data': records,
            'count': len(records),
            'source': 'database'
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo registros de BD: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Obtener estadísticas generales mejoradas"""
    try:
        stats = {
            'total_executions': 0,
            'total_records_processed': 0,
            'last_execution': None,
            'database_records': 0,
            'gender_distribution': {'males': 0, 'females': 0},
            'illness_percentage': 0.0,
            'average_income_cop': 0.0
        }
        
        # Estadísticas de archivos
        output_dir = Path('data/output')
        if output_dir.exists():
            json_files = list(output_dir.glob('transformed_data_*.json'))
            stats['total_executions'] = len(json_files)
            
            if json_files:
                latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
                stats['last_execution'] = datetime.fromtimestamp(
                    latest_file.stat().st_mtime
                ).isoformat()
                
                # Contar registros del último archivo
                with open(latest_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    stats['total_records_processed'] = len(data)
        
        # Estadísticas detalladas de base de datos
        if etl_server.db_connection:
            try:
                cursor = etl_server.db_connection.connection.cursor(dictionary=True)
                
                # Total de registros
                cursor.execute("SELECT COUNT(*) as count FROM personas_transformadas")
                result = cursor.fetchone()
                stats['database_records'] = result['count'] if result else 0
                
                # Distribución por género
                cursor.execute("""
                    SELECT genero_es, COUNT(*) as count 
                    FROM personas_transformadas 
                    GROUP BY genero_es
                """)
                gender_results = cursor.fetchall()
                for row in gender_results:
                    if row['genero_es'] == 'Masculino':
                        stats['gender_distribution']['males'] = row['count']
                    elif row['genero_es'] == 'Femenino':
                        stats['gender_distribution']['females'] = row['count']
                
                # Porcentaje con enfermedad
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN enfermedad_es = 'Sí' THEN 1 ELSE 0 END) as with_illness
                    FROM personas_transformadas
                """)
                illness_result = cursor.fetchone()
                if illness_result and illness_result['total'] > 0:
                    stats['illness_percentage'] = round(
                        (illness_result['with_illness'] / illness_result['total']) * 100, 1
                    )
                
                # Ingreso promedio en COP
                cursor.execute("""
                    SELECT AVG(ingreso_cop) as avg_income 
                    FROM personas_transformadas
                """)
                income_result = cursor.fetchone()
                if income_result and income_result['avg_income']:
                    stats['average_income_cop'] = float(income_result['avg_income'])
                
                cursor.close()
                
            except Exception as e:
                logger.warning(f"Error obteniendo stats detalladas de BD: {e}")
        
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<file_type>', methods=['GET'])
def download_file(file_type):
    """Descargar archivos generados"""
    try:
        # Buscar el archivo más reciente del tipo solicitado
        output_dir = Path('data/output')
        
        if file_type == 'json':
            files = list(output_dir.glob('transformed_data_*.json'))
        elif file_type == 'csv':
            files = list(output_dir.glob('transformed_data_*.csv'))
        elif file_type == 'parquet':
            files = list(output_dir.glob('transformed_data_*.parquet'))
        elif file_type == 'sql':
            files = list(output_dir.glob('insert_script_*.sql'))
        else:
            return jsonify({'error': 'Tipo de archivo no válido'}), 400
        
        if not files:
            return jsonify({'error': f'No hay archivos {file_type} disponibles'}), 404
        
        # Obtener el archivo más reciente
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        
        return send_file(
            latest_file,
            as_attachment=True,
            download_name=f"otaku_data_latest.{file_type}"
        )
        
    except Exception as e:
        logger.error(f"Error descargando archivo: {e}")
        return jsonify({'error': str(e)}), 500

def process_etl_pipeline(input_file, insert_to_database=True):
    """Ejecutar pipeline ETL completo"""
    try:
        start_time = datetime.now()
        logger.info(f"=== INICIANDO PIPELINE ETL ===")
        logger.info(f"Archivo: {input_file}")
        logger.info(f"Insertar en BD: {insert_to_database}")

        # Resetear conexión a base de datos antes de cada operación
        if etl_server.db_connection:
            try:
                logger.info("Verificando conexión a base de datos...")
                # Verificar y resetear conexión si es necesario
                if not etl_server.db_connection.is_connected():
                    logger.info("Reconectando a base de datos...")
                    etl_server.db_connection.disconnect()
                    if not etl_server.db_connection.connect():
                        logger.warning("No se pudo reconectar a la base de datos")
                        etl_server.db_connection = None
                else:
                    # Resetear estado de la conexión
                    try:
                        etl_server.db_connection.connection.rollback()
                        logger.info("Rollback ejecutado exitosamente")
                    except Exception as rollback_error:
                        logger.warning(f"Error en rollback (ignorando): {rollback_error}")
            except Exception as e:
                logger.warning(f"Error reseteando conexión a BD: {e}")
                etl_server.db_connection = None

        # 1. Extracción
        logger.info(f"=== PASO 1: EXTRACCIÓN ===")
        logger.info(f"Extrayendo datos desde: {input_file}")
        extracted_data = etl_server.extractor.extract_otaku_data(input_file)
        logger.info(f"Extracción completada: {len(extracted_data) if extracted_data is not None else 0} registros")

        if extracted_data is None or extracted_data.empty:
            raise Exception("No se pudieron extraer datos del archivo")

        # 2. Transformación
        logger.info("=== PASO 2: TRANSFORMACIÓN ===")
        logger.info("Transformando datos")
        transformation_result = etl_server.transformer.transform_otaku_data(extracted_data)
        logger.info("Transformación iniciada")

        # El transformer devuelve una tupla (DataFrame, validation_result)
        if isinstance(transformation_result, tuple):
            transformed_data, validation_info = transformation_result
        else:
            transformed_data = transformation_result
            validation_info = None

        logger.info(f"Transformación completada: {len(transformed_data) if transformed_data is not None else 0} registros transformados")

        if transformed_data is None or transformed_data.empty:
            raise Exception("Error en la transformación de datos")

        # 3. Carga
        logger.info("=== PASO 3: CARGA ===")
        logger.info("Cargando datos")
        logger.info(f"Insertar en BD: {insert_to_database}")
        load_result = etl_server.loader.load_otaku_data(
            transformed_data,
            etl_server.db_connection if insert_to_database else None
        )
        logger.info("Carga completada")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Convertir DataFrame a lista de diccionarios para JSON
        data_list = transformed_data.to_dict('records')

        # Obtener TRM utilizada
        trm_used = etl_server.trm_service.obtener_trm_actual()

        result = {
            'success': True,
            'data': data_list,
            'records_processed': len(data_list),
            'duration_seconds': round(duration, 2),
            'trm_used': trm_used,
            'files_created': load_result.get('files_created', {}),
            'database_inserted': insert_to_database and load_result.get('database_records', 0) > 0,
            'records_inserted': load_result.get('database_records', 0) if insert_to_database else 0,
            'timestamp': end_time.isoformat()
        }

        logger.info(f"ETL completado exitosamente: {len(data_list)} registros en {duration:.2f}s")
        if insert_to_database:
            logger.info(f"Registros insertados en BD: {result['records_inserted']}")
        return result

    except Exception as e:
        logger.error(f"Error en pipeline ETL: {e}")
        return {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

@app.route('/api/export-and-insert', methods=['POST'])
def export_and_insert():
    """Exportar archivos E insertar datos en la base de datos"""
    try:
        data = request.get_json()
        
        if not data or 'data' not in data:
            return jsonify({'error': 'No se proporcionaron datos'}), 400
        
        records_data = data['data']
        export_formats = data.get('export_formats', ['json', 'csv', 'sql'])
        insert_to_database = data.get('insert_to_database', True)
        
        if not records_data:
            return jsonify({'error': 'No hay datos para procesar'}), 400
        
        logger.info(f"Procesando exportación e inserción de {len(records_data)} registros")
        
        # Convertir datos a DataFrame
        df = pd.DataFrame(records_data)
        
        # Ejecutar carga con inserción en BD
        load_result = etl_server.loader.load_otaku_data(
            df,
            etl_server.db_connection if insert_to_database else None
        )
        
        # Preparar respuesta
        result = {
            'success': True,
            'records_processed': len(records_data),
            'records_inserted': load_result.get('database_records', 0),
            'files_created': {
                'json': bool(load_result.get('json_file')),
                'csv': bool(load_result.get('csv_file')),
                'sql': bool(load_result.get('sql_script'))
            },
            'timestamp': datetime.now().isoformat()
        }
        
        if load_result.get('errors'):
            result['warnings'] = load_result['errors']
        
        logger.info(f"Exportación e inserción completada: {result['records_inserted']} registros insertados")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error en exportación e inserción: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/import-to-database', methods=['POST'])
def import_to_database():
    """Importar archivo directamente a la base de datos"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No se encontró archivo'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No se seleccionó archivo'}), 400
        
        # Verificar conexión a base de datos
        if not etl_server.db_connection:
            return jsonify({'error': 'Base de datos no disponible'}), 503
        
        # Guardar archivo temporalmente
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_filename = f"temp_import_{timestamp}_{file.filename}"
        temp_path = Path('data/input') / temp_filename
        
        # Crear directorio si no existe
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        file.save(temp_path)
        
        logger.info(f"Importando archivo a BD: {temp_path}")
        
        # Procesar archivo con ETL completo
        try:
            # 1. Extracción
            extracted_data = etl_server.extractor.extract_otaku_data(str(temp_path))
            
            if extracted_data is None or extracted_data.empty:
                raise Exception("No se pudieron extraer datos del archivo")
            
            # 2. Transformación
            transformation_result = etl_server.transformer.transform_otaku_data(extracted_data)
            
            if isinstance(transformation_result, tuple):
                transformed_data, validation_info = transformation_result
            else:
                transformed_data = transformation_result
            
            if transformed_data is None or transformed_data.empty:
                raise Exception("Error en la transformación de datos")
            
            # 3. Inserción directa en base de datos (sin generar archivos)
            records_inserted = etl_server.loader.load_to_database(
                transformed_data, 
                etl_server.db_connection
            )
            
            # Convertir DataFrame a lista para respuesta
            data_list = transformed_data.to_dict('records')
            
            result = {
                'success': True,
                'records_processed': len(extracted_data),
                'records_inserted': records_inserted,
                'data': data_list,
                'filename': file.filename,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Importación completada: {records_inserted} registros insertados desde {file.filename}")
            return jsonify(result)
            
        finally:
            # Limpiar archivo temporal
            if temp_path.exists():
                temp_path.unlink()
        
    except Exception as e:
        logger.error(f"Error importando a base de datos: {e}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint no encontrado'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Error interno del servidor'}), 500

if __name__ == '__main__':
    logger.info("Iniciando servidor API ETL OtakuLATAM")
    logger.info("Frontend disponible en: http://localhost:3000")
    logger.info("API disponible en: http://localhost:8000")
    
    # Ejecutar servidor
    app.run(
        host='0.0.0.0',
        port=8000,
        debug=True
    )

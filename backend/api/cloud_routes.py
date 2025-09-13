"""
Rutas de API para gestión de almacenamiento en nube
"""
from flask import Blueprint, request, jsonify, send_file
import os
import logging
from werkzeug.utils import secure_filename
from datetime import datetime

from config.settings import CLOUD_STORAGE_CONFIG
from services.cloud_storage import CloudStorageService, test_cloud_connection

logger = logging.getLogger(__name__)

# Crear blueprint para rutas de nube
cloud_bp = Blueprint('cloud', __name__, url_prefix='/api/cloud')

# Inicializar servicio de nube
cloud_service = None

def get_cloud_service():
    """Obtiene o inicializa el servicio de nube"""
    global cloud_service
    if cloud_service is None:
        cloud_service = CloudStorageService(CLOUD_STORAGE_CONFIG)
    return cloud_service

@cloud_bp.route('/status', methods=['GET'])
def get_connection_status():
    """Obtiene el estado de conexión con el servicio de nube"""
    try:
        service = get_cloud_service()
        status = service.get_connection_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error obteniendo estado de conexión: {e}")
        return jsonify({
            'connected': False,
            'service': 'Error',
            'error': str(e),
            'message': 'Error interno del servidor'
        }), 500

@cloud_bp.route('/files', methods=['GET'])
def list_files():
    """Lista archivos en el almacenamiento en nube"""
    try:
        service = get_cloud_service()
        prefix = request.args.get('prefix', '')
        
        files = service.list_files(prefix)
        
        return jsonify({
            'success': True,
            'files': files,
            'total_files': len(files),
            'prefix': prefix
        })
    except Exception as e:
        logger.error(f"Error listando archivos: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error listando archivos'
        }), 500

@cloud_bp.route('/upload', methods=['POST'])
def upload_file():
    """Sube un archivo al almacenamiento en nube"""
    try:
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'message': 'No se encontró archivo en la petición'
            }), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'success': False,
                'message': 'No se seleccionó archivo'
            }), 400
        
        # Guardar archivo temporalmente
        filename = secure_filename(file.filename)
        temp_dir = 'temp_uploads'
        os.makedirs(temp_dir, exist_ok=True)
        temp_path = os.path.join(temp_dir, filename)
        
        file.save(temp_path)
        
        try:
            # Subir a la nube
            service = get_cloud_service()
            folder_prefix = request.form.get('folder', 'uploads')
            add_timestamp = request.form.get('add_timestamp', 'true').lower() == 'true'
            
            remote_name = filename
            if folder_prefix:
                remote_name = f"{folder_prefix}/{filename}"
            
            result = service.upload_file(temp_path, remote_name, add_timestamp)
            
            # Limpiar archivo temporal
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            return jsonify(result)
            
        except Exception as e:
            # Limpiar archivo temporal en caso de error
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise e
            
    except Exception as e:
        logger.error(f"Error subiendo archivo: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error subiendo archivo'
        }), 500

@cloud_bp.route('/upload-multiple', methods=['POST'])
def upload_multiple_files():
    """Sube múltiples archivos al almacenamiento en nube"""
    try:
        if 'files' not in request.files:
            return jsonify({
                'success': False,
                'message': 'No se encontraron archivos en la petición'
            }), 400
        
        files = request.files.getlist('files')
        if not files:
            return jsonify({
                'success': False,
                'message': 'No se seleccionaron archivos'
            }), 400
        
        # Guardar archivos temporalmente
        temp_dir = 'temp_uploads'
        os.makedirs(temp_dir, exist_ok=True)
        temp_paths = []
        
        for file in files:
            if file.filename != '':
                filename = secure_filename(file.filename)
                temp_path = os.path.join(temp_dir, filename)
                file.save(temp_path)
                temp_paths.append(temp_path)
        
        try:
            # Subir a la nube
            service = get_cloud_service()
            folder_prefix = request.form.get('folder', 'uploads')
            
            result = service.upload_multiple_files(temp_paths, folder_prefix)
            
            # Limpiar archivos temporales
            for temp_path in temp_paths:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            
            return jsonify(result)
            
        except Exception as e:
            # Limpiar archivos temporales en caso de error
            for temp_path in temp_paths:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            raise e
            
    except Exception as e:
        logger.error(f"Error subiendo múltiples archivos: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error subiendo archivos'
        }), 500

@cloud_bp.route('/export-summary', methods=['POST'])
def export_summary():
    """Exporta un resumen de archivos en la nube"""
    try:
        service = get_cloud_service()
        output_dir = request.json.get('output_dir', 'data/exports') if request.is_json else 'data/exports'
        
        summary_file = service.export_data_summary(output_dir)
        
        return jsonify({
            'success': True,
            'file_path': summary_file,
            'message': 'Resumen exportado exitosamente'
        })
    except Exception as e:
        logger.error(f"Error exportando resumen: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error exportando resumen'
        }), 500

@cloud_bp.route('/migrate', methods=['POST'])
def migrate_existing_data():
    """Migra datos existentes a la nube"""
    try:
        service = get_cloud_service()
        
        # Buscar archivos en directorios de datos
        data_dirs = ['data/output', 'data/processed', 'data/cloud_backup']
        files_to_migrate = []
        
        for data_dir in data_dirs:
            if os.path.exists(data_dir):
                for root, dirs, files in os.walk(data_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        files_to_migrate.append(file_path)
        
        if not files_to_migrate:
            return jsonify({
                'success': True,
                'migrated_files': 0,
                'message': 'No hay archivos para migrar'
            })
        
        # Migrar archivos
        folder_prefix = f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        result = service.upload_multiple_files(files_to_migrate, folder_prefix)
        
        return jsonify({
            'success': True,
            'migrated_files': len(result['successful']),
            'failed_files': len(result['failed']),
            'total_size_mb': result['total_size_mb'],
            'details': result,
            'message': f'Migración completada: {len(result["successful"])} archivos'
        })
        
    except Exception as e:
        logger.error(f"Error migrando datos: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error migrando datos'
        }), 500

@cloud_bp.route('/test-config', methods=['POST'])
def test_configuration():
    """Prueba una configuración de nube"""
    try:
        config = request.get_json()
        if not config:
            return jsonify({
                'connected': False,
                'message': 'Configuración no proporcionada'
            }), 400
        
        # Combinar con configuración actual para credenciales
        test_config = CLOUD_STORAGE_CONFIG.copy()
        test_config.update(config)
        
        status = test_cloud_connection(test_config)
        return jsonify(status)
        
    except Exception as e:
        logger.error(f"Error probando configuración: {e}")
        return jsonify({
            'connected': False,
            'error': str(e),
            'message': 'Error probando configuración'
        }), 500

@cloud_bp.route('/download/<path:filename>', methods=['GET'])
def download_file(filename):
    """Descarga un archivo de la nube (funcionalidad limitada)"""
    try:
        # Por ahora, solo soportamos descarga de archivos locales
        backup_dir = "data/cloud_backup"
        file_path = os.path.join(backup_dir, filename)
        
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True)
        else:
            return jsonify({
                'success': False,
                'message': 'Archivo no encontrado en respaldo local'
            }), 404
            
    except Exception as e:
        logger.error(f"Error descargando archivo: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error descargando archivo'
        }), 500

@cloud_bp.route('/delete/<path:filename>', methods=['DELETE'])
def delete_file(filename):
    """Elimina un archivo de la nube (funcionalidad limitada)"""
    try:
        # Por ahora, solo soportamos eliminación de archivos locales
        backup_dir = "data/cloud_backup"
        file_path = os.path.join(backup_dir, filename)
        
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({
                'success': True,
                'message': 'Archivo eliminado del respaldo local'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Archivo no encontrado en respaldo local'
            }), 404
            
    except Exception as e:
        logger.error(f"Error eliminando archivo: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error eliminando archivo'
        }), 500

@cloud_bp.route('/stats', methods=['GET'])
def get_cloud_stats():
    """Obtiene estadísticas del almacenamiento en nube"""
    try:
        service = get_cloud_service()
        files = service.list_files()
        
        # Calcular estadísticas básicas
        total_files = len(files)
        
        # Para archivos locales, calcular tamaño
        total_size = 0
        backup_dir = "data/cloud_backup"
        if os.path.exists(backup_dir):
            for file in files:
                file_path = os.path.join(backup_dir, file)
                if os.path.exists(file_path):
                    total_size += os.path.getsize(file_path)
        
        # Última subida (aproximada basada en archivos locales)
        last_upload = None
        if os.path.exists(backup_dir) and files:
            latest_file = None
            latest_time = 0
            for file in files:
                file_path = os.path.join(backup_dir, file)
                if os.path.exists(file_path):
                    mtime = os.path.getmtime(file_path)
                    if mtime > latest_time:
                        latest_time = mtime
                        latest_file = file
            
            if latest_file:
                last_upload = datetime.fromtimestamp(latest_time).isoformat()
        
        return jsonify({
            'success': True,
            'stats': {
                'total_files': total_files,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'last_upload': last_upload,
                'service_type': service.service_type
            }
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error obteniendo estadísticas'
        }), 500

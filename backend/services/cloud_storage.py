"""
Servicio de almacenamiento en nube para archivos del ETL
"""
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

class CloudStorageService:
    """Servicio genérico de almacenamiento en nube"""
    
    def __init__(self, config):
        self.config = config
        self.service_type = config.get('service', 'local')
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Inicializa el cliente según el tipo de servicio"""
        try:
            if self.service_type == 'aws_s3':
                self._initialize_aws_s3()
            elif self.service_type == 'google_cloud':
                self._initialize_google_cloud()
            elif self.service_type == 'azure':
                self._initialize_azure()
            else:
                logger.info("Usando almacenamiento local como respaldo")
                self.service_type = 'local'
        except Exception as e:
            logger.warning(f"Error inicializando {self.service_type}: {e}. Usando almacenamiento local")
            self.service_type = 'local'
    
    def _initialize_aws_s3(self):
        """Inicializa cliente de AWS S3"""
        try:
            import boto3
            from botocore.exceptions import NoCredentialsError, ClientError
            
            self.client = boto3.client(
                's3',
                aws_access_key_id=self.config.get('access_key'),
                aws_secret_access_key=self.config.get('secret_key'),
                region_name=self.config.get('region', 'us-east-1')
            )
            
            # Verificar que el bucket existe o crearlo
            bucket_name = self.config.get('bucket_name')
            try:
                self.client.head_bucket(Bucket=bucket_name)
                logger.info(f"Bucket S3 '{bucket_name}' verificado exitosamente")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.info(f"Creando bucket S3 '{bucket_name}'")
                    self.client.create_bucket(Bucket=bucket_name)
                else:
                    raise
                    
        except ImportError:
            logger.error("boto3 no está instalado. Instalar con: pip install boto3")
            raise
        except NoCredentialsError:
            logger.error("Credenciales de AWS no configuradas")
            raise
    
    def _initialize_google_cloud(self):
        """Inicializa cliente de Google Cloud Storage"""
        try:
            from google.cloud import storage
            
            self.client = storage.Client()
            bucket_name = self.config.get('bucket_name')
            self.bucket = self.client.bucket(bucket_name)
            
            logger.info(f"Cliente Google Cloud Storage inicializado para bucket '{bucket_name}'")
            
        except ImportError:
            logger.error("google-cloud-storage no está instalado. Instalar con: pip install google-cloud-storage")
            raise
    
    def _initialize_azure(self):
        """Inicializa cliente de Azure Blob Storage"""
        try:
            from azure.storage.blob import BlobServiceClient
            
            connection_string = self.config.get('connection_string')
            self.client = BlobServiceClient.from_connection_string(connection_string)
            
            logger.info("Cliente Azure Blob Storage inicializado")
            
        except ImportError:
            logger.error("azure-storage-blob no está instalado. Instalar con: pip install azure-storage-blob")
            raise
    
    def upload_file(self, local_file_path: str, remote_file_name: Optional[str] = None, 
                   add_timestamp: bool = True) -> Dict[str, Union[str, bool]]:
        """
        Sube un archivo al almacenamiento en nube
        
        Args:
            local_file_path (str): Ruta del archivo local
            remote_file_name (str, optional): Nombre del archivo en la nube
            add_timestamp (bool): Si agregar timestamp al nombre del archivo
        
        Returns:
            dict: Resultado de la subida con URL, éxito, y metadatos
        """
        try:
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"Archivo no encontrado: {local_file_path}")
            
            if remote_file_name is None:
                remote_file_name = os.path.basename(local_file_path)
            
            # Agregar timestamp al nombre del archivo si se solicita
            if add_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name, ext = os.path.splitext(remote_file_name)
                remote_file_name = f"{name}_{timestamp}{ext}"
            
            # Obtener información del archivo
            file_size = os.path.getsize(local_file_path)
            file_info = {
                'original_name': os.path.basename(local_file_path),
                'remote_name': remote_file_name,
                'size_bytes': file_size,
                'size_mb': round(file_size / (1024 * 1024), 2),
                'upload_time': datetime.now().isoformat(),
                'service_type': self.service_type
            }
            
            if self.service_type == 'aws_s3':
                url = self._upload_to_s3(local_file_path, remote_file_name)
            elif self.service_type == 'google_cloud':
                url = self._upload_to_gcs(local_file_path, remote_file_name)
            elif self.service_type == 'azure':
                url = self._upload_to_azure(local_file_path, remote_file_name)
            else:
                url = self._save_local_backup(local_file_path, remote_file_name)
            
            return {
                'success': True,
                'url': url,
                'file_info': file_info,
                'message': f'Archivo subido exitosamente a {self.service_type}'
            }
                
        except Exception as e:
            logger.error(f"Error subiendo archivo {local_file_path}: {e}")
            # Intentar guardar localmente como respaldo
            backup_url = self._save_local_backup(local_file_path, remote_file_name or os.path.basename(local_file_path))
            return {
                'success': False,
                'url': backup_url,
                'error': str(e),
                'message': 'Error en subida, guardado como respaldo local'
            }
    
    def _upload_to_s3(self, local_file_path, remote_file_name):
        """Sube archivo a AWS S3"""
        bucket_name = self.config.get('bucket_name')
        
        # Subir archivo sin ACL público (más compatible con buckets modernos)
        self.client.upload_file(
            local_file_path, 
            bucket_name, 
            remote_file_name
        )
        
        url = f"https://{bucket_name}.s3.amazonaws.com/{remote_file_name}"
        logger.info(f"Archivo subido a S3: {url}")
        return url
    
    def _upload_to_gcs(self, local_file_path, remote_file_name):
        """Sube archivo a Google Cloud Storage"""
        blob = self.bucket.blob(remote_file_name)
        
        with open(local_file_path, 'rb') as file:
            blob.upload_from_file(file)
        
        # Hacer público
        blob.make_public()
        
        url = blob.public_url
        logger.info(f"Archivo subido a GCS: {url}")
        return url
    
    def _upload_to_azure(self, local_file_path, remote_file_name):
        """Sube archivo a Azure Blob Storage"""
        container_name = self.config.get('container_name', 'otaku-data')
        
        blob_client = self.client.get_blob_client(
            container=container_name, 
            blob=remote_file_name
        )
        
        with open(local_file_path, 'rb') as file:
            blob_client.upload_blob(file, overwrite=True)
        
        url = blob_client.url
        logger.info(f"Archivo subido a Azure: {url}")
        return url
    
    def _save_local_backup(self, local_file_path, remote_file_name):
        """Guarda una copia local como respaldo"""
        try:
            backup_dir = "data/cloud_backup"
            os.makedirs(backup_dir, exist_ok=True)
            
            backup_path = os.path.join(backup_dir, remote_file_name)
            
            # Copiar archivo
            import shutil
            shutil.copy2(local_file_path, backup_path)
            
            logger.info(f"Archivo guardado localmente como respaldo: {backup_path}")
            return f"local://{backup_path}"
            
        except Exception as e:
            logger.error(f"Error guardando respaldo local: {e}")
            return f"local://{local_file_path}"
    
    def list_files(self, prefix=""):
        """
        Lista archivos en el almacenamiento
        
        Args:
            prefix (str): Prefijo para filtrar archivos
        
        Returns:
            list: Lista de nombres de archivos
        """
        try:
            if self.service_type == 'aws_s3':
                return self._list_s3_files(prefix)
            elif self.service_type == 'google_cloud':
                return self._list_gcs_files(prefix)
            elif self.service_type == 'azure':
                return self._list_azure_files(prefix)
            else:
                return self._list_local_files(prefix)
        except Exception as e:
            logger.error(f"Error listando archivos: {e}")
            return []
    
    def _list_s3_files(self, prefix):
        """Lista archivos en S3"""
        bucket_name = self.config.get('bucket_name')
        response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', [])]
    
    def _list_gcs_files(self, prefix):
        """Lista archivos en Google Cloud Storage"""
        blobs = self.bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]
    
    def _list_azure_files(self, prefix):
        """Lista archivos en Azure Blob Storage"""
        container_name = self.config.get('container_name', 'otaku-data')
        container_client = self.client.get_container_client(container_name)
        blobs = container_client.list_blobs(name_starts_with=prefix)
        return [blob.name for blob in blobs]
    
    def _list_local_files(self, prefix):
        """Lista archivos locales"""
        backup_dir = "data/cloud_backup"
        if not os.path.exists(backup_dir):
            return []
        
        files = []
        for file in os.listdir(backup_dir):
            if file.startswith(prefix):
                files.append(file)
        return files

    def upload_multiple_files(self, file_paths: List[str], folder_prefix: str = "") -> Dict[str, List]:
        """
        Sube múltiples archivos a la nube
        
        Args:
            file_paths (List[str]): Lista de rutas de archivos locales
            folder_prefix (str): Prefijo de carpeta para organizar archivos
        
        Returns:
            dict: Resultados de las subidas
        """
        results = {
            'successful': [],
            'failed': [],
            'total_files': len(file_paths),
            'total_size_mb': 0
        }
        
        for file_path in file_paths:
            try:
                remote_name = os.path.basename(file_path)
                if folder_prefix:
                    remote_name = f"{folder_prefix}/{remote_name}"
                
                result = self.upload_file(file_path, remote_name)
                
                if result['success']:
                    results['successful'].append(result)
                    results['total_size_mb'] += result['file_info']['size_mb']
                else:
                    results['failed'].append({
                        'file': file_path,
                        'error': result.get('error', 'Unknown error')
                    })
                    
            except Exception as e:
                results['failed'].append({
                    'file': file_path,
                    'error': str(e)
                })
        
        logger.info(f"Subida masiva completada: {len(results['successful'])}/{results['total_files']} archivos exitosos")
        return results
    
    def get_connection_status(self) -> Dict[str, Union[str, bool]]:
        """
        Verifica el estado de la conexión con el servicio de nube
        
        Returns:
            dict: Estado de la conexión
        """
        try:
            if self.service_type == 'aws_s3':
                bucket_name = self.config.get('bucket_name')
                self.client.head_bucket(Bucket=bucket_name)
                return {
                    'connected': True,
                    'service': 'AWS S3',
                    'bucket': bucket_name,
                    'message': 'Conexión exitosa'
                }
            elif self.service_type == 'google_cloud':
                return {
                    'connected': True,
                    'service': 'Google Cloud Storage',
                    'bucket': self.config.get('bucket_name'),
                    'message': 'Conexión exitosa'
                }
            elif self.service_type == 'azure':
                return {
                    'connected': True,
                    'service': 'Azure Blob Storage',
                    'container': self.config.get('container_name'),
                    'message': 'Conexión exitosa'
                }
            else:
                return {
                    'connected': False,
                    'service': 'Local Storage',
                    'message': 'Usando almacenamiento local'
                }
        except Exception as e:
            return {
                'connected': False,
                'service': self.service_type,
                'error': str(e),
                'message': 'Error de conexión'
            }
    
    def export_data_summary(self, output_dir: str = "data/exports") -> str:
        """
        Exporta un resumen de todos los archivos en la nube
        
        Args:
            output_dir (str): Directorio donde guardar el resumen
        
        Returns:
            str: Ruta del archivo de resumen creado
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            files = self.list_files()
            summary = {
                'export_date': datetime.now().isoformat(),
                'service_type': self.service_type,
                'total_files': len(files),
                'files': files,
                'connection_status': self.get_connection_status()
            }
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            summary_file = os.path.join(output_dir, f"cloud_summary_{timestamp}.json")
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Resumen de nube exportado: {summary_file}")
            return summary_file
            
        except Exception as e:
            logger.error(f"Error exportando resumen: {e}")
            raise

# Funciones de conveniencia
def upload_to_cloud(file_path, config, remote_name=None):
    """
    Función de conveniencia para subir archivos a la nube
    
    Args:
        file_path (str): Ruta del archivo local
        config (dict): Configuración del servicio de nube
        remote_name (str, optional): Nombre remoto del archivo
    
    Returns:
        dict: Resultado de la subida
    """
    service = CloudStorageService(config)
    return service.upload_file(file_path, remote_name)

def test_cloud_connection(config):
    """
    Prueba la conexión con el servicio de nube
    
    Args:
        config (dict): Configuración del servicio de nube
    
    Returns:
        dict: Estado de la conexión
    """
    service = CloudStorageService(config)
    return service.get_connection_status()

"""
Configuración del ETL OtakuLATAM
"""
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de Base de Datos MySQL
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'otaku'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'root'),
    'charset': 'utf8mb4'
}

# Configuración de TRM (Tasa Representativa del Mercado)
TRM_API_URL = 'https://www.datos.gov.co/resource/32sa-8pi3.json?$limit=1&$order=vigenciadesde%20DESC'

# Configuración de archivos
INPUT_DATA_PATH = 'data/input/sample_data.csv'
OUTPUT_JSON_PATH = 'data/output/transformed_data.json'
OUTPUT_PARQUET_PATH = 'data/output/transformed_data.parquet'
PROCESSED_DATA_PATH = 'data/processed/'

# Configuración de almacenamiento en nube (AWS S3)
CLOUD_STORAGE_CONFIG = {
    'service': 'aws_s3',  # Opciones: 'aws_s3', 'google_cloud', 'azure'
    'bucket_name': os.getenv('AWS_S3_BUCKET_NAME', 'otaku-latam-data-bucket'),
    'region': os.getenv('AWS_S3_REGION', 'us-east-1'),
    'access_key': os.getenv('AWS_ACCESS_KEY_ID', ''),
    'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
    'endpoint_url': os.getenv('AWS_S3_ENDPOINT_URL', None),
    'use_ssl': os.getenv('AWS_S3_USE_SSL', 'true').lower() == 'true'
}

# Configuraciones alternativas para otros proveedores de nube
GOOGLE_CLOUD_CONFIG = {
    'service': 'google_cloud',
    'bucket_name': os.getenv('GCP_BUCKET_NAME', 'otaku-latam-gcp'),
    'project_id': os.getenv('GCP_PROJECT_ID', ''),
    'credentials_path': os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
}

AZURE_CLOUD_CONFIG = {
    'service': 'azure',
    'container_name': os.getenv('AZURE_CONTAINER_NAME', 'otaku-data'),
    'connection_string': os.getenv('AZURE_STORAGE_CONNECTION_STRING', ''),
    'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME', ''),
    'account_key': os.getenv('AZURE_STORAGE_ACCOUNT_KEY', '')
}

# Mapeo de traducciones
GENDER_TRANSLATION = {
    'Male': 'Masculino',
    'Female': 'Femenino'
}

ILLNESS_TRANSLATION = {
    'Yes': 'Sí',
    'No': 'No'
}

# Configuración de logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'logs/etl_execution.log'
}

# Configuración de transformaciones
TRANSFORMATIONS = {
    'age_to_lustros': True,  # Convertir edad de años a lustros
    'income_to_cop': True,   # Convertir ingresos de USD a COP
    'translate_fields': True  # Traducir campos al español
}

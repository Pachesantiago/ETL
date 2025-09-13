"""
Módulo de conexión a la base de datos MySQL - Versión Corregida
"""
import mysql.connector
from mysql.connector import Error
import logging
from contextlib import contextmanager
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manejador de conexiones a MySQL"""
    
    def __init__(self, config):
        self.config = config
        self.connection = None
    
    def connect(self):
        """Establece conexión con la base de datos"""
        try:
            self.connection = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                charset=self.config.get('charset', 'utf8mb4'),
                autocommit=False
            )
            
            if self.connection.is_connected():
                logger.info(f"Conexión exitosa a MySQL: {self.config['host']}:{self.config['port']}/{self.config['database']}")
                return True
            
        except Error as e:
            logger.error(f"Error conectando a MySQL: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado en conexión: {e}")
            return False
    
    def disconnect(self):
        """Cierra la conexión a la base de datos"""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                logger.info("Conexión a MySQL cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")
    
    def is_connected(self):
        """Verifica si la conexión está activa"""
        try:
            return self.connection and self.connection.is_connected()
        except:
            return False
    
    def get_cursor(self, dictionary=True):
        """Obtiene cursor de base de datos"""
        try:
            if not self.is_connected():
                self.connect()
            return self.connection.cursor(dictionary=dictionary)
        except Exception as e:
            logger.error(f"Error en cursor de base de datos: {e}")
            raise
    
    def execute_query(self, query, params=None, fetch=False):
        """
        Ejecuta una consulta SQL
        
        Args:
            query (str): Consulta SQL
            params (tuple, optional): Parámetros para la consulta
            fetch (bool): Si True, retorna los resultados
        
        Returns:
            list|int: Resultados de la consulta o número de filas afectadas
        """
        cursor = None
        try:
            cursor = self.get_cursor()
            cursor.execute(query, params or ())
            
            if fetch:
                result = cursor.fetchall()
                cursor.close()
                return result
            else:
                self.connection.commit()
                rowcount = cursor.rowcount
                cursor.close()
                return rowcount
                
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            if cursor:
                cursor.close()
            raise
    
    def test_connection(self):
        """
        Prueba la conexión a la base de datos
        
        Returns:
            bool: True si la conexión es exitosa
        """
        try:
            result = self.execute_query("SELECT 1 as test", fetch=True)
            return len(result) > 0 and result[0]['test'] == 1
        except Exception as e:
            logger.error(f"Error en test de conexión: {e}")
            return False

    def iniciar_ejecucion_etl(self, archivo_origen):
        """Registra el inicio de una ejecución ETL"""
        try:
            cursor = self.get_cursor()
            query = """
            INSERT INTO etl_ejecuciones (fecha_inicio, archivo_origen, estado)
            VALUES (NOW(), %s, 'EN_PROCESO')
            """
            cursor.execute(query, (archivo_origen,))
            self.connection.commit()
            ejecucion_id = cursor.lastrowid
            cursor.close()
            logger.info(f"Ejecución ETL iniciada con ID: {ejecucion_id}")
            return ejecucion_id
        except Exception as e:
            logger.error(f"Error iniciando ejecución ETL: {e}")
            return None

    def finalizar_ejecucion_etl(self, ejecucion_id, registros_procesados, trm_rate, 
                               output_json_file, output_parquet_file, status, error_message=None):
        """Finaliza el registro de una ejecución ETL"""
        try:
            cursor = self.get_cursor()
            query = """
            UPDATE etl_ejecuciones
            SET fecha_fin = NOW(),
                registros_procesados = %s,
                trm_utilizada = %s,
                archivo_json_generado = %s,
                archivo_parquet_generado = %s,
                estado = %s,
                mensaje_error = %s,
                duracion_segundos = TIMESTAMPDIFF(SECOND, fecha_inicio, NOW())
            WHERE id = %s
            """
            cursor.execute(query, (registros_procesados, trm_rate, output_json_file, 
                                 output_parquet_file, status, error_message, ejecucion_id))
            self.connection.commit()
            cursor.close()
            logger.info(f"Ejecución ETL {ejecucion_id} finalizada con status: {status}")
        except Exception as e:
            logger.error(f"Error finalizando ejecución ETL: {e}")

    def verificar_duplicado(self, nombre, edad_anos, genero_original, ingreso_usd):
        """Verifica si un registro ya existe en la base de datos"""
        try:
            cursor = self.get_cursor()
            query = """
            SELECT COUNT(*) as count FROM personas_transformadas
            WHERE nombre = %s AND edad_anos = %s AND genero_original = %s AND ingreso_usd = %s
            """
            cursor.execute(query, (nombre, edad_anos, genero_original, ingreso_usd))
            result = cursor.fetchone()
            cursor.close()
            return result['count'] > 0 if result else False
        except Exception as e:
            logger.error(f"Error verificando duplicado: {e}")
            return False

    def insertar_personas_transformadas(self, records_data):
        """Inserta los datos transformados en la base de datos evitando duplicados"""
        try:
            cursor = self.get_cursor()

            # Preparar los datos para inserción
            registros_insertados = 0
            registros_duplicados = 0

            # Si recibimos un DataFrame, convertirlo a lista de diccionarios
            if hasattr(records_data, 'iterrows'):
                # Es un DataFrame - usar el método anterior para compatibilidad
                for _, row in records_data.iterrows():
                    nombre = row.get('name', row.get('Name', ''))
                    edad_anos = row.get('age', row.get('edad_anos', 0))
                    genero_original = row.get('gender', row.get('Gender', ''))
                    ingreso_usd = row.get('income_usd', row.get('income', 0.0))

                    # Verificar duplicado
                    if self.verificar_duplicado(nombre, edad_anos, genero_original, ingreso_usd):
                        logger.info(f"Registro duplicado omitido: {nombre}")
                        registros_duplicados += 1
                        continue

                    query = """
                    INSERT INTO personas_transformadas
                    (nombre, edad_anos, edad_lustros, genero_original, genero_es,
                     ingreso_usd, ingreso_cop, trm_utilizada, enfermedad_original, enfermedad_es)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    valores = (
                        nombre,
                        edad_anos,
                        row.get('age_lustros', row.get('Age Lustrum', 0.0)),
                        genero_original,
                        row.get('gender_es', row.get('Gender Es', '')),
                        ingreso_usd,
                        row.get('income_cop', row.get('Income Cop', 0.0)),
                        row.get('trm_used', row.get('Trm', 0.0)),
                        row.get('illness', row.get('Illness', '')),
                        row.get('illness_es', row.get('Illness Es', ''))
                    )

                    cursor.execute(query, valores)
                    registros_insertados += 1
            else:
                # Es una lista de diccionarios (formato del loader)
                for record in records_data:
                    nombre = record.get('nombre', '')
                    edad_anos = record.get('edad_anos', 0)
                    genero_original = record.get('genero_original', '')
                    ingreso_usd = record.get('ingreso_usd', 0.0)

                    # Verificar duplicado
                    if self.verificar_duplicado(nombre, edad_anos, genero_original, ingreso_usd):
                        logger.info(f"Registro duplicado omitido: {nombre}")
                        registros_duplicados += 1
                        continue

                    query = """
                    INSERT INTO personas_transformadas
                    (nombre, edad_anos, edad_lustros, genero_original, genero_es,
                     ingreso_usd, ingreso_cop, trm_utilizada, enfermedad_original, enfermedad_es, fecha_procesamiento)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    valores = (
                        nombre,
                        edad_anos,
                        record.get('edad_lustros', 0.0),
                        genero_original,
                        record.get('genero_es', ''),
                        ingreso_usd,
                        record.get('ingreso_cop', 0.0),
                        record.get('trm_utilizada', 0.0),
                        record.get('enfermedad_original', ''),
                        record.get('enfermedad_es', ''),
                        record.get('fecha_procesamiento', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    )

                    cursor.execute(query, valores)
                    registros_insertados += 1

            self.connection.commit()
            cursor.close()
            logger.info(f"Insertados {registros_insertados} registros en personas_transformadas")
            if registros_duplicados > 0:
                logger.info(f"Registros duplicados omitidos: {registros_duplicados}")
            return registros_insertados

        except Exception as e:
            logger.error(f"Error insertando personas transformadas: {e}")
            logger.error(f"Tipo de datos recibidos: {type(records_data)}")
            if hasattr(records_data, '__len__'):
                logger.error(f"Cantidad de registros: {len(records_data)}")
            if self.connection:
                self.connection.rollback()
            raise e

    def registrar_log_etl(self, ejecucion_id, nivel, mensaje, detalle=None):
        """Registra un log de la ejecución del ETL"""
        try:
            cursor = self.get_cursor()
            query = """
            INSERT INTO etl_logs (ejecucion_id, nivel, mensaje, detalle)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (ejecucion_id, nivel, mensaje, json.dumps(detalle) if detalle else None))
            self.connection.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Error registrando log ETL: {e}")

# Función de conveniencia para crear conexión
def crear_conexion_db(config):
    """
    Crea y retorna una conexión a la base de datos
    
    Args:
        config (dict): Configuración de la base de datos
    
    Returns:
        DatabaseConnection: Instancia de conexión
    """
    db_conn = DatabaseConnection(config)
    if db_conn.connect():
        return db_conn
    else:
        raise Exception("No se pudo establecer conexión con la base de datos")

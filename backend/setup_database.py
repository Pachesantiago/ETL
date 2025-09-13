import mysql.connector
import getpass
from config.settings import DATABASE_CONFIG

def setup_database():
    """Crear las tablas en la base de datos MySQL"""
    connection = None
    try:
        # Solicitar contraseña si no está configurada
        db_config = DATABASE_CONFIG.copy()
        if not db_config['password']:
            db_config['password'] = getpass.getpass("Ingresa la contraseña de MySQL para root: ")
        
        # Conectar a MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # Leer el archivo SQL
        with open('sql/create_tables.sql', 'r', encoding='utf-8') as file:
            sql_script = file.read()
        
        # Ejecutar cada statement SQL
        statements = sql_script.split(';')
        for statement in statements:
            if statement.strip():
                cursor.execute(statement)
        
        connection.commit()
        print("✅ Tablas creadas exitosamente en la base de datos")
        
    except mysql.connector.Error as error:
        print(f"❌ Error al crear las tablas: {error}")
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    setup_database()

import mysql.connector

def setup_database():
    """Crear las tablas en la base de datos MySQL"""
    connection = None
    try:
        # Configuración directa de conexión
        db_config = {
            'host': 'localhost',
            'port': 3306,
            'database': 'otaku',
            'user': 'root',
            'password': 'root',  # Contraseña directa
            'charset': 'utf8mb4'
        }
        
        # Conectar a MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # SQL para crear las tablas
        create_tables_sql = """
        -- Tabla para almacenar datos transformados
        CREATE TABLE IF NOT EXISTS personas_transformadas (
            id INT AUTO_INCREMENT PRIMARY KEY,
            income_usd DECIMAL(15,2) NOT NULL,
            income_cop DECIMAL(15,2) NOT NULL,
            trm_used DECIMAL(10,4) NOT NULL,
            gender_es VARCHAR(20) NOT NULL,
            illness_es VARCHAR(10) NOT NULL,
            age_lustros DECIMAL(5,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_gender (gender_es),
            INDEX idx_illness (illness_es),
            INDEX idx_income_usd (income_usd),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        -- Tabla para log de ejecuciones ETL
        CREATE TABLE IF NOT EXISTS etl_execution_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            records_processed INT NOT NULL,
            trm_rate DECIMAL(10,4) NOT NULL,
            input_file VARCHAR(255) NOT NULL,
            output_json_file VARCHAR(255),
            output_parquet_file VARCHAR(255),
            status ENUM('SUCCESS', 'ERROR', 'PARTIAL') NOT NULL,
            error_message TEXT,
            execution_time_seconds DECIMAL(10,3),
            INDEX idx_execution_date (execution_date),
            INDEX idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        
        # Ejecutar cada statement SQL
        statements = create_tables_sql.split(';')
        for statement in statements:
            if statement.strip():
                cursor.execute(statement)
        
        connection.commit()
        print("✅ Tablas creadas exitosamente en la base de datos")
        
        # Verificar que las tablas se crearon
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("📋 Tablas disponibles:")
        for table in tables:
            print(f"   - {table[0]}")
        
    except mysql.connector.Error as error:
        print(f"❌ Error al crear las tablas: {error}")
        return False
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
    
    return True

if __name__ == "__main__":
    setup_database()

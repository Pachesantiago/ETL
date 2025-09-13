-- Script de creación de tablas para ETL OtakuLATAM
-- Base de datos: otaku

USE otaku;

-- Tabla principal para almacenar los datos transformados
-- Estructura actualizada para coincidir con los datos del ETL (11 columnas sin id AUTO_INCREMENT)
CREATE TABLE IF NOT EXISTS personas_transformadas (
    edad_lustros DECIMAL(4,2) NOT NULL,
    genero_original VARCHAR(10) NOT NULL,
    genero_es VARCHAR(20) NOT NULL,
    enfermedad_original VARCHAR(10) NOT NULL,
    enfermedad_es VARCHAR(10) NOT NULL,
    ingreso_cop DECIMAL(15,2) NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    fecha_procesamiento TIMESTAMP NOT NULL,
    trm_utilizada DECIMAL(10,4) NOT NULL,
    edad_anos INT NOT NULL,
    ingreso_usd DECIMAL(10,2) NOT NULL,
    INDEX idx_genero (genero_es),
    INDEX idx_enfermedad (enfermedad_es),
    INDEX idx_fecha (fecha_procesamiento),
    INDEX idx_nombre (nombre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla para registrar las ejecuciones del ETL
CREATE TABLE IF NOT EXISTS etl_ejecuciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fecha_inicio TIMESTAMP NOT NULL,
    fecha_fin TIMESTAMP NULL,
    estado VARCHAR(20) NOT NULL DEFAULT 'EN_PROCESO', -- EN_PROCESO, COMPLETADO, ERROR
    registros_procesados INT DEFAULT 0,
    registros_exitosos INT DEFAULT 0,
    registros_con_error INT DEFAULT 0,
    trm_utilizada DECIMAL(10,4) NULL,
    archivo_origen VARCHAR(255) NOT NULL,
    archivo_json_generado VARCHAR(255) NULL,
    archivo_parquet_generado VARCHAR(255) NULL,
    url_cloud_storage VARCHAR(500) NULL,
    mensaje_error TEXT NULL,
    duracion_segundos INT NULL,
    INDEX idx_fecha_inicio (fecha_inicio),
    INDEX idx_estado (estado)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla para almacenar las tasas de cambio históricas
CREATE TABLE IF NOT EXISTS tasas_cambio (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    trm_cop_usd DECIMAL(10,4) NOT NULL,
    fuente VARCHAR(100) NOT NULL DEFAULT 'Banco de la República',
    fecha_consulta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fecha (fecha)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla para logs detallados del ETL
CREATE TABLE IF NOT EXISTS etl_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ejecucion_id INT NOT NULL,
    nivel VARCHAR(10) NOT NULL, -- INFO, WARNING, ERROR
    mensaje TEXT NOT NULL,
    detalle JSON NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ejecucion_id) REFERENCES etl_ejecuciones(id) ON DELETE CASCADE,
    INDEX idx_ejecucion (ejecucion_id),
    INDEX idx_nivel (nivel),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Vista para consultas rápidas de datos transformados
CREATE OR REPLACE VIEW vista_personas_resumen AS
SELECT 
    genero_es as genero,
    COUNT(*) as total_personas,
    AVG(edad_lustros) as edad_promedio_lustros,
    AVG(ingreso_cop) as ingreso_promedio_cop,
    SUM(CASE WHEN enfermedad_es = 'Sí' THEN 1 ELSE 0 END) as personas_con_enfermedad,
    SUM(CASE WHEN enfermedad_es = 'No' THEN 1 ELSE 0 END) as personas_sin_enfermedad
FROM personas_transformadas 
GROUP BY genero_es;

-- Insertar datos iniciales de ejemplo para TRM (opcional)
INSERT IGNORE INTO tasas_cambio (fecha, trm_cop_usd, fuente) VALUES 
(CURDATE(), 4200.00, 'Valor inicial de ejemplo'),
('2024-01-01', 4100.50, 'Histórico ejemplo');

-- Mostrar estructura de las tablas creadas
SHOW TABLES;
DESCRIBE personas_transformadas;
DESCRIBE etl_ejecuciones;
DESCRIBE tasas_cambio;
DESCRIBE etl_logs;

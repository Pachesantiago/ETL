-- Script de migración para actualizar la tabla personas_transformadas
-- Fecha: 2025-01-12
-- Propósito: Corregir estructura para coincidir con datos del ETL

USE otaku;

-- Paso 1: Respaldar datos existentes si los hay
CREATE TABLE IF NOT EXISTS personas_transformadas_backup AS 
SELECT * FROM personas_transformadas;

-- Paso 2: Eliminar la tabla actual
DROP TABLE IF EXISTS personas_transformadas;

-- Paso 3: Crear la nueva estructura (11 columnas exactas para el ETL)
CREATE TABLE personas_transformadas (
    edad_lustros DECIMAL(4,2) NOT NULL COMMENT 'Edad en lustros (años/5)',
    genero_original VARCHAR(10) NOT NULL COMMENT 'Género original en inglés',
    genero_es VARCHAR(20) NOT NULL COMMENT 'Género traducido al español',
    enfermedad_original VARCHAR(10) NOT NULL COMMENT 'Enfermedad original en inglés',
    enfermedad_es VARCHAR(10) NOT NULL COMMENT 'Enfermedad traducida al español',
    ingreso_cop DECIMAL(15,2) NOT NULL COMMENT 'Ingreso convertido a pesos colombianos',
    nombre VARCHAR(100) NOT NULL COMMENT 'Nombre de la persona',
    fecha_procesamiento TIMESTAMP NOT NULL COMMENT 'Fecha y hora de procesamiento',
    trm_utilizada DECIMAL(10,4) NOT NULL COMMENT 'TRM utilizada para conversión',
    edad_anos INT NOT NULL COMMENT 'Edad en años',
    ingreso_usd DECIMAL(10,2) NOT NULL COMMENT 'Ingreso original en USD',
    
    -- Índices para optimizar consultas
    INDEX idx_genero (genero_es),
    INDEX idx_enfermedad (enfermedad_es),
    INDEX idx_fecha (fecha_procesamiento),
    INDEX idx_nombre (nombre),
    INDEX idx_edad (edad_anos)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Tabla de personas transformadas - Estructura actualizada para ETL';

-- Paso 4: Verificar la estructura
DESCRIBE personas_transformadas;

-- Paso 5: Probar el INSERT que estaba fallando
-- Este es el INSERT exacto que proporcionaste
INSERT INTO personas_transformadas VALUES (
    3.8,                                    -- edad_lustros
    'male',                                 -- genero_original  
    'Masculino',                           -- genero_es
    'yes',                                 -- enfermedad_original
    'Sí',                                  -- enfermedad_es
    118008744.12,                          -- ingreso_cop
    'Andrew Ramirez',                      -- nombre
    'Fri, 12 Sep 2025 15:26:52 GMT',     -- fecha_procesamiento
    3903.18,                              -- trm_utilizada
    19,                                   -- edad_anos
    30234                                 -- ingreso_usd
);

-- Paso 6: Verificar que el INSERT funcionó
SELECT * FROM personas_transformadas;
SELECT COUNT(*) as total_registros FROM personas_transformadas;

-- Paso 7: Mostrar información de la migración
SELECT 
    'Migración completada exitosamente' as estado,
    NOW() as fecha_migracion,
    COUNT(*) as registros_insertados
FROM personas_transformadas;

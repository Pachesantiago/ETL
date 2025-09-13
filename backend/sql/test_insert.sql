-- Script de prueba para verificar el INSERT en personas_transformadas
-- Fecha: 2025-01-12

USE otaku;

-- Mostrar la estructura actual de la tabla
DESCRIBE personas_transformadas;

-- Probar el INSERT que estaba fallando
INSERT INTO personas_transformadas VALUES (
    3.8,                                    -- edad_lustros
    'male',                                 -- genero_original  
    'Masculino',                           -- genero_es
    'yes',                                 -- enfermedad_original
    'Sí',                                  -- enfermedad_es
    118008744.12,                          -- ingreso_cop
    'Andrew Ramirez',                      -- nombre
    '2025-09-12 15:26:52',                -- fecha_procesamiento (formato MySQL)
    3903.18,                              -- trm_utilizada
    19,                                   -- edad_anos
    30234.00                              -- ingreso_usd
);

-- Verificar que el INSERT funcionó
SELECT 'INSERT exitoso' as resultado;
SELECT * FROM personas_transformadas ORDER BY fecha_procesamiento DESC LIMIT 1;

-- Probar algunos INSERT adicionales para validar
INSERT INTO personas_transformadas VALUES 
(4.2, 'female', 'Femenino', 'no', 'No', 95000000.00, 'Maria Garcia', '2025-09-12 16:30:00', 3903.18, 21, 24500.00),
(2.6, 'male', 'Masculino', 'yes', 'Sí', 78000000.00, 'Carlos Lopez', '2025-09-12 17:15:30', 3903.18, 13, 20000.00);

-- Mostrar todos los registros
SELECT 
    nombre,
    edad_anos,
    edad_lustros,
    genero_es,
    enfermedad_es,
    ingreso_usd,
    ingreso_cop,
    trm_utilizada,
    fecha_procesamiento
FROM personas_transformadas 
ORDER BY fecha_procesamiento DESC;

-- Estadísticas básicas
SELECT 
    COUNT(*) as total_registros,
    AVG(edad_lustros) as edad_promedio_lustros,
    AVG(ingreso_cop) as ingreso_promedio_cop,
    COUNT(CASE WHEN genero_es = 'Masculino' THEN 1 END) as masculinos,
    COUNT(CASE WHEN genero_es = 'Femenino' THEN 1 END) as femeninos,
    COUNT(CASE WHEN enfermedad_es = 'Sí' THEN 1 END) as con_enfermedad,
    COUNT(CASE WHEN enfermedad_es = 'No' THEN 1 END) as sin_enfermedad
FROM personas_transformadas;

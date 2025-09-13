# ETL OtakuLATAM

Sistema ETL (Extract, Transform, Load) profesional para el procesamiento y análisis de datos demográficos de la comunidad OtakuLATAM.

## 📋 Descripción

ETL OtakuLATAM es una aplicación completa que permite extraer, transformar y cargar datos de personas, incluyendo conversión automática de monedas usando la TRM (Tasa Representativa del Mercado) de Colombia, análisis demográfico y visualización interactiva de resultados.

## 🏗️ Arquitectura del Proyecto

```
Alpha/
├── README.md                    # Documentación principal
├── TODO.md                      # Lista de tareas del proyecto
├── backend/                     # Servidor y lógica de negocio
│   ├── main.py                  # Punto de entrada principal del ETL
│   ├── requirements.txt         # Dependencias de Python
│   ├── api/                     # API REST
│   │   ├── server.py           # Servidor Flask con endpoints
│   │   └── cloud_routes.py     # Rutas para servicios en la nube
│   ├── config/                  # Configuración
│   │   └── settings.py         # Variables de configuración
│   ├── database/                # Conexión a base de datos
│   │   ├── connection.py       # Conexión principal a MySQL
│   │   ├── connection_fixed.py # Versión corregida
│   │   └── connection_optimized.py # Versión optimizada
│   ├── etl/                     # Módulos ETL
│   │   ├── extractor.py        # Extracción de datos
│   │   ├── transformer.py      # Transformación de datos
│   │   ├── transformer_optimized.py # Versión optimizada
│   │   └── loader.py           # Carga de datos
│   ├── services/                # Servicios externos
│   │   ├── trm_service.py      # Servicio de TRM (conversión USD-COP)
│   │   ├── trm_service_optimized.py # Versión optimizada
│   │   ├── trm_cache.py        # Cache para TRM
│   │   └── cloud_storage.py    # Almacenamiento en la nube
│   ├── utils/                   # Utilidades
│   │   └── date_utils.py       # Manejo de fechas
│   ├── sql/                     # Scripts SQL
│   │   ├── create_tables.sql   # Creación de tablas
│   │   └── migrate_personas_transformadas.sql # Migración
│   └── data/                    # Datos del proyecto
│       ├── input/              # Archivos de entrada
│       │   ├── sample_data.csv # Datos de muestra
│       │   └── otaku_sample_data_50_personas.xlsx # Datos de ejemplo
│       ├── exports/            # Exportaciones a la nube
│       └── cloud_backup/       # Respaldos en la nube
└── frontend/                    # Interfaz de usuario
    ├── index.html              # Página principal del dashboard
    ├── src/                    # Código JavaScript
    │   ├── main.js            # Lógica principal del frontend
    │   ├── config.js          # Configuración del frontend
    │   └── cloud-dashboard.js # Dashboard de la nube
    ├── styles/                 # Estilos CSS
    │   ├── main.css           # Estilos principales
    │   └── checkbox.css       # Estilos para checkboxes
    ├── pages/                  # Páginas adicionales
    │   └── cloud-dashboard.html # Dashboard de servicios en la nube
    ├── assets/                 # Recursos estáticos
    ├── components/             # Componentes reutilizables
    └── public/                 # Archivos públicos
```

## 🚀 Tecnologías Utilizadas

### Backend
- **Python 3.8+** - Lenguaje principal
- **Flask** - Framework web para API REST
- **Pandas** - Manipulación y análisis de datos
- **MySQL** - Base de datos relacional
- **PyArrow** - Soporte para archivos Parquet
- **Requests** - Cliente HTTP para servicios externos
- **Boto3** - SDK de AWS para almacenamiento en la nube

### Frontend
- **HTML5** - Estructura de la interfaz
- **CSS3** - Estilos y diseño responsivo
- **JavaScript (ES6+)** - Lógica del cliente
- **Chart.js** - Visualización de gráficos
- **Font Awesome** - Iconografía
- **Papa Parse** - Procesamiento de archivos CSV

### Base de Datos
- **MySQL 8.0+** - Sistema de gestión de base de datos
- **Estructura optimizada** para consultas de análisis

### Servicios Externos
- **API TRM Colombia** - Conversión de monedas USD a COP
- **AWS S3** - Almacenamiento en la nube (opcional)

## ⚙️ Instalación y Configuración

### Prerrequisitos
- Python 3.8 o superior
- MySQL 8.0 o superior
- Navegador web moderno

### 1. Clonar el repositorio 
```bash
git clone <repository-url>
cd Alpha
```

### 2. Configurar el Backend
```bash
cd backend
pip install -r requirements.txt
```

### 3. Configurar la Base de Datos
```bash
# Crear la base de datos
mysql -u root -p < sql/create_tables.sql

# Configurar conexión en config/settings.py
```

### 4. Configurar variables de entorno (opcional)
```bash
# Crear archivo .env en el directorio backend
DATABASE_HOST=localhost
DATABASE_USER=tu_usuario
DATABASE_PASSWORD=tu_contraseña
DATABASE_NAME=otaku
```

## 🎯 Uso del Sistema

## Ejecutar backend y frontend
´´´ Bash
cd backend
python start_server.py
opcion 2
´´´

### Ejecutar el Backend
```bash
cd backend
python main.py
```

### Ejecutar la API
```bash
cd backend
python api/server.py
```
La API estará disponible en: `http://localhost:8000`

### Acceder al Frontend
Abrir `frontend/index.html` en un navegador web o servir desde un servidor web local.

## 📊 Funcionalidades Principales

### 1. Extracción de Datos
- Soporte para archivos CSV y Excel (.xlsx, .xls)
- Validación automática de formato y estructura
- Carga mediante interfaz web o API

### 2. Transformación de Datos
- **Conversión de edad**: Años a lustros automáticamente
- **Normalización de género**: Estandarización de valores
- **Conversión de moneda**: USD a COP usando TRM actual
- **Traducción de campos**: Inglés a español
- **Validación de datos**: Verificación de integridad

### 3. Carga de Datos
- **Base de datos MySQL**: Inserción automática
- **Archivos JSON**: Formato estructurado
- **Archivos CSV**: Compatible con Excel
- **Archivos Parquet**: Formato optimizado para análisis
- **Scripts SQL**: Para inserción manual

### 4. Visualización
- **Dashboard interactivo**: Estadísticas en tiempo real
- **Gráficos dinámicos**: Distribución por género, edad, ingresos
- **Filtros avanzados**: Búsqueda y filtrado de datos
- **Exportación**: Múltiples formatos de salida

### 5. Servicios en la Nube
- **Respaldo automático**: Subida a AWS S3 o Google Cloud
- **Dashboard de nube**: Monitoreo de archivos remotos
- **Sincronización**: Mantener datos actualizados

## 🔧 API Endpoints

### Salud del Sistema
- `GET /health` - Estado del servidor y conexiones

### Procesamiento de Datos
- `POST /api/process-file` - Procesar archivo subido
- `POST /api/run-etl` - Ejecutar ETL completo
- `GET /api/latest-data` - Obtener datos más recientes

### Base de Datos
- `GET /api/database/records` - Obtener registros de la BD
- `POST /api/import-to-database` - Importar directamente a BD

### Estadísticas
- `GET /api/stats` - Estadísticas generales del sistema

### Exportación
- `GET /api/download/<tipo>` - Descargar archivos (json, csv, parquet, sql)
- `POST /api/export-and-insert` - Exportar e insertar en BD

## 📈 Flujo de Trabajo

1. **Carga de Datos**: Usuario sube archivo CSV/Excel
2. **Extracción**: Sistema lee y valida el archivo
3. **Transformación**: Aplicación de reglas de negocio
4. **Validación**: Verificación de integridad de datos
5. **Carga**: Inserción en BD y generación de archivos
6. **Visualización**: Dashboard actualizado con nuevos datos
7. **Exportación**: Descarga en múltiples formatos

## 🛡️ Características de Seguridad

- Validación de tipos de archivo
- Sanitización de datos de entrada
- Manejo seguro de conexiones a BD
- Logs detallados de operaciones
- Manejo de errores robusto

## 📝 Logs y Monitoreo

El sistema genera logs detallados en:
- Consola durante ejecución
- Archivos de log (cuando se configuran)
- Base de datos (tabla de ejecuciones ETL)

## 🤝 Contribución

Para contribuir al proyecto:
1. Fork del repositorio
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit de cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver archivo `LICENSE` para más detalles.

## 📞 Soporte

Para soporte técnico o consultas:
- Crear un issue en el repositorio
- Contactar al equipo de desarrollo

---

**ETL OtakuLATAM** - Sistema profesional de procesamiento de datos demográficos

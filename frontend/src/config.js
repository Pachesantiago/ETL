// Configuración para conectar frontend con backend y base de datos

const CONFIG = {
    // URLs del backend
    BACKEND_URL: 'http://localhost:8000',
    API_BASE_URL: 'http://localhost:8000/api',
    
    // Endpoints de la API
    ENDPOINTS: {
        HEALTH: '/health',
        LATEST_DATA: '/api/latest-data',
        PROCESS_FILE: '/api/process-file',
        RUN_ETL: '/api/run-etl',
        DATABASE_RECORDS: '/api/database/records',
        STATS: '/api/stats',
        DOWNLOAD: '/api/download'
    },
    
    // Configuración de la aplicación
    APP: {
        NAME: 'ETL OtakuLATAM Dashboard',
        VERSION: '1.0.0',
        RECORDS_PER_PAGE: 10,
        MAX_FILE_SIZE: 10 * 1024 * 1024, // 10MB
        SUPPORTED_FILE_TYPES: ['.csv', '.xlsx', '.xls'],
        AUTO_REFRESH_INTERVAL: 30000 // 30 segundos
    },
    
    // Configuración de la base de datos (solo para mostrar info)
    DATABASE: {
        HOST: 'localhost',
        PORT: 3306,
        NAME: 'otaku',
        TABLE: 'personas_transformadas'
    },
    
    // Configuración de notificaciones
    NOTIFICATIONS: {
        DURATION: 3000, // 3 segundos
        POSITION: 'top-right'
    },
    
    // Configuración de gráficos
    CHARTS: {
        COLORS: {
            PRIMARY: '#3498db',
            SECONDARY: '#e74c3c',
            SUCCESS: '#2ecc71',
            WARNING: '#f39c12',
            INFO: '#9b59b6'
        },
        ANIMATION_DURATION: 1000
    },
    
    // Mapeos de transformación (para mostrar al usuario)
    TRANSFORMATIONS: {
        GENDER: {
            'Male': 'Masculino',
            'Female': 'Femenino'
        },
        ILLNESS: {
            'Yes': 'Sí',
            'No': 'No'
        },
        CURRENCY: {
            FROM: 'USD',
            TO: 'COP',
            TRM_SOURCE: 'Banco de la República de Colombia'
        },
        AGE: {
            UNIT_FROM: 'años',
            UNIT_TO: 'lustros',
            CONVERSION_FACTOR: 5
        }
    }
};

// Función para obtener la URL completa de un endpoint
function getEndpointUrl(endpoint) {
    return CONFIG.BACKEND_URL + CONFIG.ENDPOINTS[endpoint];
}

// Función para verificar si el backend está disponible
async function checkBackendAvailability() {
    try {
        const response = await fetch(getEndpointUrl('HEALTH'), {
            method: 'GET',
            timeout: 5000
        });
        return response.ok;
    } catch (error) {
        console.warn('Backend no disponible:', error);
        return false;
    }
}

// Función para hacer requests con manejo de errores
async function apiRequest(endpoint, options = {}) {
    const url = endpoint.startsWith('http') ? endpoint : CONFIG.API_BASE_URL + endpoint;
    
    const defaultOptions = {
        headers: {
            'Content-Type': 'application/json',
        },
        ...options
    };
    
    try {
        const response = await fetch(url, defaultOptions);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const contentType = response.headers.get('content-type');
        if (contentType && contentType.includes('application/json')) {
            return await response.json();
        } else {
            return await response.text();
        }
        
    } catch (error) {
        console.error('Error en API request:', error);
        throw error;
    }
}

// Función para subir archivos
async function uploadFile(file, onProgress = null) {
    const formData = new FormData();
    formData.append('file', file);
    
    return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        
        // Manejar progreso de subida
        if (onProgress) {
            xhr.upload.addEventListener('progress', (e) => {
                if (e.lengthComputable) {
                    const percentComplete = (e.loaded / e.total) * 100;
                    onProgress(percentComplete);
                }
            });
        }
        
        xhr.addEventListener('load', () => {
            if (xhr.status >= 200 && xhr.status < 300) {
                try {
                    const response = JSON.parse(xhr.responseText);
                    resolve(response);
                } catch (e) {
                    resolve(xhr.responseText);
                }
            } else {
                reject(new Error(`HTTP ${xhr.status}: ${xhr.statusText}`));
            }
        });
        
        xhr.addEventListener('error', () => {
            reject(new Error('Error de red'));
        });
        
        xhr.open('POST', CONFIG.API_BASE_URL + '/process-file');
        xhr.send(formData);
    });
}

// Función para formatear números como moneda COP
function formatCurrency(amount) {
    return new Intl.NumberFormat('es-CO', {
        style: 'currency',
        currency: 'COP',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(amount);
}

// Función para formatear fechas
function formatDate(dateString) {
    const date = new Date(dateString);
    return new Intl.DateTimeFormat('es-CO', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    }).format(date);
}

// Función para validar archivos
function validateFile(file) {
    const errors = [];
    
    // Verificar tamaño
    if (file.size > CONFIG.APP.MAX_FILE_SIZE) {
        errors.push(`El archivo es muy grande. Máximo ${CONFIG.APP.MAX_FILE_SIZE / 1024 / 1024}MB`);
    }
    
    // Verificar tipo
    const fileExtension = '.' + file.name.split('.').pop().toLowerCase();
    if (!CONFIG.APP.SUPPORTED_FILE_TYPES.includes(fileExtension)) {
        errors.push(`Tipo de archivo no soportado. Use: ${CONFIG.APP.SUPPORTED_FILE_TYPES.join(', ')}`);
    }
    
    return {
        isValid: errors.length === 0,
        errors: errors
    };
}

// Función para mostrar información de conexión
function getConnectionInfo() {
    return {
        backend: CONFIG.BACKEND_URL,
        database: `${CONFIG.DATABASE.HOST}:${CONFIG.DATABASE.PORT}/${CONFIG.DATABASE.NAME}`,
        table: CONFIG.DATABASE.TABLE,
        api_endpoints: Object.keys(CONFIG.ENDPOINTS).length
    };
}

// Exportar configuración y funciones utilitarias
if (typeof module !== 'undefined' && module.exports) {
    // Node.js
    module.exports = {
        CONFIG,
        getEndpointUrl,
        checkBackendAvailability,
        apiRequest,
        uploadFile,
        formatCurrency,
        formatDate,
        validateFile,
        getConnectionInfo
    };
} else {
    // Browser
    window.CONFIG = CONFIG;
    window.getEndpointUrl = getEndpointUrl;
    window.checkBackendAvailability = checkBackendAvailability;
    window.apiRequest = apiRequest;
    window.uploadFile = uploadFile;
    window.formatCurrency = formatCurrency;
    window.formatDate = formatDate;
    window.validateFile = validateFile;
    window.getConnectionInfo = getConnectionInfo;
}

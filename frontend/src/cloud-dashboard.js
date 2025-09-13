/**
 * JavaScript para el Dashboard de Nube
 */

// Configuración de la API
const API_BASE_URL = 'http://localhost:8000/api';

// Estado global
let currentFiles = [];
let uploadQueue = [];
let isUploading = false;

// Inicialización
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    setupEventListeners();
    checkConnection();
    loadFileList();
});

function initializeDashboard() {
    console.log('Inicializando Dashboard de Nube...');
    
    // Configurar drag and drop
    const uploadArea = document.getElementById('uploadArea');
    
    uploadArea.addEventListener('dragover', function(e) {
        e.preventDefault();
        uploadArea.classList.add('dragover');
    });
    
    uploadArea.addEventListener('dragleave', function(e) {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
    });
    
    uploadArea.addEventListener('drop', function(e) {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
        
        const files = Array.from(e.dataTransfer.files);
        handleFileSelection(files);
    });
    
    uploadArea.addEventListener('click', function() {
        document.getElementById('fileInput').click();
    });
}

function setupEventListeners() {
    // Input de archivos
    document.getElementById('fileInput').addEventListener('change', function(e) {
        const files = Array.from(e.target.files);
        handleFileSelection(files);
    });
    
    // Selector de proveedor
    document.getElementById('cloudProvider').addEventListener('change', function(e) {
        updateProviderConfig(e.target.value);
    });
}

async function checkConnection() {
    const statusContent = document.getElementById('statusContent');
    const statusCard = document.getElementById('connectionStatus');
    
    try {
        statusContent.innerHTML = '<p><i class="fas fa-spinner fa-spin"></i> Verificando conexión...</p>';
        
        const response = await fetch(`${API_BASE_URL}/cloud/status`);
        const status = await response.json();
        
        // Actualizar UI según el estado
        statusCard.className = 'status-card';
        
        if (status.connected) {
            statusCard.classList.add('status-connected');
            statusContent.innerHTML = `
                <div style="color: #28a745;">
                    <h4><i class="fas fa-check-circle"></i> Conectado</h4>
                    <p><strong>Servicio:</strong> ${status.service}</p>
                    <p><strong>Bucket/Container:</strong> ${status.bucket || status.container || 'N/A'}</p>
                    <p><strong>Mensaje:</strong> ${status.message}</p>
                </div>
            `;
            document.getElementById('serviceType').textContent = status.service;
        } else {
            if (status.service === 'Local Storage') {
                statusCard.classList.add('status-local');
                statusContent.innerHTML = `
                    <div style="color: #ffc107;">
                        <h4><i class="fas fa-exclamation-triangle"></i> Modo Local</h4>
                        <p><strong>Servicio:</strong> ${status.service}</p>
                        <p><strong>Mensaje:</strong> ${status.message}</p>
                        <p><em>Los archivos se guardarán localmente como respaldo.</em></p>
                    </div>
                `;
            } else {
                statusCard.classList.add('status-disconnected');
                statusContent.innerHTML = `
                    <div style="color: #dc3545;">
                        <h4><i class="fas fa-times-circle"></i> Error de Conexión</h4>
                        <p><strong>Servicio:</strong> ${status.service}</p>
                        <p><strong>Error:</strong> ${status.error || 'Error desconocido'}</p>
                        <p><strong>Mensaje:</strong> ${status.message}</p>
                    </div>
                `;
            }
            document.getElementById('serviceType').textContent = 'Desconectado';
        }
        
    } catch (error) {
        console.error('Error verificando conexión:', error);
        statusCard.className = 'status-card status-disconnected';
        statusContent.innerHTML = `
            <div style="color: #dc3545;">
                <h4><i class="fas fa-times-circle"></i> Error de Comunicación</h4>
                <p>No se pudo conectar con el servidor backend.</p>
                <p><strong>Error:</strong> ${error.message}</p>
            </div>
        `;
    }
}

async function loadFileList() {
    const fileList = document.getElementById('fileList');
    
    try {
        fileList.innerHTML = '<p><i class="fas fa-spinner fa-spin"></i> Cargando archivos...</p>';
        
        const response = await fetch(`${API_BASE_URL}/cloud/files`);
        const data = await response.json();
        
        currentFiles = data.files || [];
        
        if (currentFiles.length === 0) {
            fileList.innerHTML = '<p><i class="fas fa-folder-open"></i> No hay archivos en la nube.</p>';
        } else {
            let html = '';
            currentFiles.forEach((file, index) => {
                html += `
                    <div class="file-item">
                        <div>
                            <i class="fas fa-file"></i>
                            <strong>${file}</strong>
                        </div>
                        <div>
                            <button class="btn btn-primary" onclick="downloadFile('${file}')">
                                <i class="fas fa-download"></i>
                            </button>
                            <button class="btn btn-danger" onclick="deleteFile('${file}')">
                                <i class="fas fa-trash"></i>
                            </button>
                        </div>
                    </div>
                `;
            });
            fileList.innerHTML = html;
        }
        
        // Actualizar estadísticas
        document.getElementById('totalFiles').textContent = currentFiles.length;
        
    } catch (error) {
        console.error('Error cargando archivos:', error);
        fileList.innerHTML = `<p style="color: #dc3545;"><i class="fas fa-exclamation-triangle"></i> Error cargando archivos: ${error.message}</p>`;
    }
}

function handleFileSelection(files) {
    if (files.length === 0) return;
    
    console.log('Archivos seleccionados:', files);
    
    // Agregar archivos a la cola de subida
    uploadQueue = [...uploadQueue, ...files];
    
    // Mostrar confirmación
    const fileNames = files.map(f => f.name).join(', ');
    if (confirm(`¿Subir ${files.length} archivo(s) a la nube?\n\n${fileNames}`)) {
        startUpload();
    } else {
        // Limpiar cola si el usuario cancela
        uploadQueue = [];
    }
}

async function startUpload() {
    if (isUploading || uploadQueue.length === 0) return;
    
    isUploading = true;
    const progressDiv = document.getElementById('uploadProgress');
    const progressFill = document.getElementById('progressFill');
    const progressText = document.getElementById('progressText');
    
    progressDiv.style.display = 'block';
    
    let completed = 0;
    const total = uploadQueue.length;
    
    for (const file of uploadQueue) {
        try {
            const formData = new FormData();
            formData.append('file', file);
            
            const response = await fetch(`${API_BASE_URL}/cloud/upload`, {
                method: 'POST',
                body: formData
            });
            
            const result = await response.json();
            
            if (result.success) {
                console.log('Archivo subido exitosamente:', result);
            } else {
                console.error('Error subiendo archivo:', result);
                alert(`Error subiendo ${file.name}: ${result.message || 'Error desconocido'}`);
            }
            
        } catch (error) {
            console.error('Error en la subida:', error);
            alert(`Error subiendo ${file.name}: ${error.message}`);
        }
        
        completed++;
        const progress = (completed / total) * 100;
        progressFill.style.width = `${progress}%`;
        progressText.textContent = `${Math.round(progress)}% (${completed}/${total})`;
    }
    
    // Limpiar y actualizar
    uploadQueue = [];
    isUploading = false;
    
    setTimeout(() => {
        progressDiv.style.display = 'none';
        progressFill.style.width = '0%';
        progressText.textContent = '0%';
    }, 2000);
    
    // Recargar lista de archivos
    loadFileList();
    
    alert('¡Subida completada!');
}

async function exportSummary() {
    try {
        const response = await fetch(`${API_BASE_URL}/cloud/export-summary`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert(`Resumen exportado exitosamente: ${result.file_path}`);
        } else {
            alert(`Error exportando resumen: ${result.message}`);
        }
        
    } catch (error) {
        console.error('Error exportando resumen:', error);
        alert(`Error exportando resumen: ${error.message}`);
    }
}

async function migrateExistingData() {
    if (!confirm('¿Migrar todos los datos existentes a la nube? Esta operación puede tomar tiempo.')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/cloud/migrate`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert(`Migración completada: ${result.migrated_files} archivos migrados`);
            loadFileList();
        } else {
            alert(`Error en migración: ${result.message}`);
        }
        
    } catch (error) {
        console.error('Error en migración:', error);
        alert(`Error en migración: ${error.message}`);
    }
}

function updateProviderConfig(provider) {
    // Mostrar/ocultar configuraciones específicas del proveedor
    const awsConfig = document.getElementById('awsConfig');
    
    // Por ahora solo mostramos configuración de AWS
    if (provider === 'aws_s3') {
        awsConfig.style.display = 'block';
    } else {
        awsConfig.style.display = 'none';
    }
}

async function testConfiguration() {
    const provider = document.getElementById('cloudProvider').value;
    const bucketName = document.getElementById('bucketName').value;
    const region = document.getElementById('awsRegion').value;
    
    const config = {
        service: provider,
        bucket_name: bucketName,
        region: region
    };
    
    try {
        const response = await fetch(`${API_BASE_URL}/cloud/test-config`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(config)
        });
        
        const result = await response.json();
        
        if (result.connected) {
            alert('✅ Configuración válida - Conexión exitosa');
        } else {
            alert(`❌ Error en configuración: ${result.message}`);
        }
        
    } catch (error) {
        console.error('Error probando configuración:', error);
        alert(`Error probando configuración: ${error.message}`);
    }
}

function saveConfiguration() {
    alert('Función de guardado de configuración en desarrollo...');
}

function resetConfiguration() {
    if (confirm('¿Resetear toda la configuración de nube? Esta acción no se puede deshacer.')) {
        // Resetear formulario
        document.getElementById('cloudProvider').value = 'aws_s3';
        document.getElementById('bucketName').value = '';
        document.getElementById('awsRegion').value = 'us-east-1';
        
        alert('Configuración reseteada');
    }
}

function cleanupOldFiles() {
    alert('Función de limpieza en desarrollo...');
}

async function downloadFile(fileName) {
    try {
        const response = await fetch(`${API_BASE_URL}/cloud/download/${encodeURIComponent(fileName)}`);
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = fileName;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } else {
            alert('Error descargando archivo');
        }
        
    } catch (error) {
        console.error('Error descargando archivo:', error);
        alert(`Error descargando archivo: ${error.message}`);
    }
}

async function deleteFile(fileName) {
    if (!confirm(`¿Eliminar el archivo "${fileName}"? Esta acción no se puede deshacer.`)) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/cloud/delete/${encodeURIComponent(fileName)}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('Archivo eliminado exitosamente');
            loadFileList();
        } else {
            alert(`Error eliminando archivo: ${result.message}`);
        }
        
    } catch (error) {
        console.error('Error eliminando archivo:', error);
        alert(`Error eliminando archivo: ${error.message}`);
    }
}

// Funciones de utilidad
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('es-ES') + ' ' + date.toLocaleTimeString('es-ES');
}

// Frontend JavaScript para conectar con el backend ETL OtakuLATAM

class ETLDashboard {
    constructor() {
        this.backendUrl = 'http://localhost:8000'; // URL del backend API
        this.currentData = null;
        this.filteredData = null;
        this.charts = {};
        
        // DataTable properties
        this.currentPage = 1;
        this.recordsPerPage = 25;
        this.sortColumn = null;
        this.sortDirection = 'asc';
        this.searchTerm = '';
        this.filters = {
            gender: '',
            illness: '',
            ageRange: ''
        };
        this.visibleColumns = {
            nombre: true,
            edad_anos: true,
            edad_lustros: true,
            genero_es: true,
            ingreso_usd: true,
            ingreso_cop: true,
            trm_utilizada: true,
            enfermedad_es: true,
            fecha_procesamiento: true
        };
        
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.checkBackendConnection();
        // this.loadExistingData(); // Comentado para evitar carga automática de datos
    }

    setupEventListeners() {
        // File upload
        const fileInput = document.getElementById('fileInput');
        const fileUploadArea = document.getElementById('fileUploadArea');
        
        fileUploadArea.addEventListener('click', () => fileInput.click());
        fileUploadArea.addEventListener('dragover', this.handleDragOver.bind(this));
        fileUploadArea.addEventListener('drop', this.handleFileDrop.bind(this));
        fileInput.addEventListener('change', this.handleFileSelect.bind(this));

        // ETL execution button
        const runETLBtn = document.getElementById('runETLBtn');
        if (runETLBtn) {
            runETLBtn.addEventListener('click', this.runETLProcess.bind(this));
        }

        // Refresh data button
        const refreshBtn = document.getElementById('refreshBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', this.loadExistingData.bind(this));
        }

        // DataTable event listeners
        this.setupDataTableEventListeners();

        // Auto-refresh every 30 seconds
        setInterval(() => {
            this.updateStats();
        }, 30000);
    }

    setupDataTableEventListeners() {
        // Search input
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.searchTerm = e.target.value;
                this.currentPage = 1;
                this.applyFiltersAndRender();
            });
        }

        // Records per page selector
        const recordsPerPage = document.getElementById('recordsPerPage');
        if (recordsPerPage) {
            recordsPerPage.addEventListener('change', (e) => {
                this.recordsPerPage = parseInt(e.target.value);
                this.currentPage = 1;
                this.renderDataTable();
            });
        }

        // Filter selectors
        const genderFilter = document.getElementById('genderFilter');
        if (genderFilter) {
            genderFilter.addEventListener('change', (e) => {
                this.filters.gender = e.target.value;
                this.currentPage = 1;
                this.applyFiltersAndRender();
            });
        }

        const illnessFilter = document.getElementById('illnessFilter');
        if (illnessFilter) {
            illnessFilter.addEventListener('change', (e) => {
                this.filters.illness = e.target.value;
                this.currentPage = 1;
                this.applyFiltersAndRender();
            });
        }

        const ageRangeFilter = document.getElementById('ageRangeFilter');
        if (ageRangeFilter) {
            ageRangeFilter.addEventListener('change', (e) => {
                this.filters.ageRange = e.target.value;
                this.currentPage = 1;
                this.applyFiltersAndRender();
            });
        }
    }

    async checkBackendConnection() {
        try {
            const response = await fetch(`${this.backendUrl}/health`);
            if (response.ok) {
                const healthData = await response.json();
                console.log('✅ Conexión con backend establecida');
                this.showNotification('Conectado al backend ETL', 'success');
                
                // Update connection status in UI
                this.updateConnectionStatus(true, healthData);
                
                // Load backend statistics
                this.loadBackendStats();
                
                return true;
            }
        } catch (error) {
            console.log('⚠️ Backend no disponible, usando modo local');
            this.showNotification('Modo local - Backend no disponible', 'warning');
            this.updateConnectionStatus(false);
            return false;
        }
    }

    updateConnectionStatus(connected, healthData = null) {
        const statusElement = document.getElementById('connectionStatus');
        const dbStatusElement = document.getElementById('dbStatus');
        
        if (statusElement) {
            statusElement.innerHTML = connected ? 
                '<span class="status-connected">🟢 Backend Conectado</span>' :
                '<span class="status-disconnected">🔴 Backend Desconectado</span>';
        }
        
        if (dbStatusElement && healthData) {
            dbStatusElement.innerHTML = healthData.database_connected ? 
                '<span class="status-connected">🟢 Base de Datos Conectada</span>' :
                '<span class="status-disconnected">🔴 Base de Datos Desconectada</span>';
        }
    }

    async loadBackendStats() {
        try {
            const response = await fetch(`${this.backendUrl}/api/stats`);
            if (response.ok) {
                const stats = await response.json();
                this.updateBackendStats(stats);
            }
        } catch (error) {
            console.log('No se pudieron cargar estadísticas del backend');
        }
    }

    updateBackendStats(stats) {
        const elements = {
            'totalExecutions': stats.total_executions,
            'totalProcessed': stats.total_records_processed,
            'lastExecution': stats.last_execution ? 
                new Date(stats.last_execution).toLocaleString() : 'Nunca',
            'dbRecords': stats.database_records
        };
        
        Object.entries(elements).forEach(([id, value]) => {
            const element = document.getElementById(id);
            if (element) element.textContent = value;
        });

        // Actualizar estadísticas principales si están disponibles desde el backend
        if (stats.gender_distribution) {
            const genderEl = document.getElementById('genderDistribution');
            if (genderEl) {
                genderEl.textContent = `${stats.gender_distribution.males}/${stats.gender_distribution.females}`;
            }
        }

        if (stats.illness_percentage !== undefined) {
            const illnessEl = document.getElementById('illnessRate');
            if (illnessEl) {
                illnessEl.textContent = `${stats.illness_percentage}%`;
            }
        }

        if (stats.average_income_cop) {
            const incomeEl = document.getElementById('avgIncome');
            if (incomeEl) {
                incomeEl.innerHTML = `$${(stats.average_income_cop / 1000000).toFixed(1)}M<br><small>COP</small>`;
            }
        }

        if (stats.database_records) {
            const totalEl = document.getElementById('totalRecords');
            if (totalEl) {
                totalEl.textContent = stats.database_records;
            }
        }
    }

    async runETLProcess() {
        this.showLoading(true);
        this.showNotification('Ejecutando proceso ETL...', 'info');
        
        try {
            const response = await fetch(`${this.backendUrl}/api/run-etl`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    use_sample_data: true
                })
            });
            
            if (response.ok) {
                const result = await response.json();
                
                if (result.success) {
                    this.processData(result.data);
                    this.showNotification(
                        `ETL ejecutado exitosamente: ${result.records_processed} registros procesados en ${result.duration_seconds}s`, 
                        'success'
                    );
                    
                    // Update backend stats
                    this.loadBackendStats();
                } else {
                    this.showNotification(`Error en ETL: ${result.error}`, 'error');
                }
            } else {
                throw new Error('Error en la respuesta del servidor');
            }
        } catch (error) {
            console.error('Error ejecutando ETL:', error);
            this.showNotification('Error ejecutando ETL. Verifique la conexión con el backend.', 'error');
        } finally {
            this.showLoading(false);
        }
    }

    async loadExistingData() {
        // Cargar datos automáticamente al iniciar la aplicación desde el backend
        console.log('Cargando datos desde backend...');

        // Intentar cargar desde backend primero
        try {
            const response = await fetch(`${this.backendUrl}/api/latest-data`);
            if (response.ok) {
                const result = await response.json();
                this.processData(result.data);
                this.showNotification(`${result.data.length} registros cargados desde backend`, 'success');
                return;
            }
        } catch (error) {
            console.log('Backend no disponible para datos más recientes');
        }

        // Fallback: intentar cargar desde base de datos
        try {
            const response = await fetch(`${this.backendUrl}/api/database/records`);
            if (response.ok) {
                const result = await response.json();
                this.processData(result.data);
                this.showNotification(`${result.count} registros cargados desde base de datos`, 'success');
                return;
            }
        } catch (error) {
            console.log('No se pudieron cargar datos desde base de datos');
        }

        // Si no hay datos disponibles, inicializar estadísticas en 0
        this.updateStats();
        console.log('No hay datos disponibles - aplicación iniciará vacía');
    }

    handleDragOver(e) {
        e.preventDefault();
        e.currentTarget.classList.add('drag-over');
    }

    handleFileDrop(e) {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');
        
        const files = Array.from(e.dataTransfer.files);
        this.processFiles(files);
    }

    handleFileSelect(e) {
        const files = Array.from(e.target.files);
        this.processFiles(files);
    }

    async processFiles(files) {
        this.showLoading(true);
        
        for (const file of files) {
            if (this.isValidFile(file)) {
                try {
                    await this.uploadAndProcessFile(file);
                } catch (error) {
                    console.error('Error procesando archivo:', error);
                    this.showNotification(`Error procesando ${file.name}`, 'error');
                }
            } else {
                this.showNotification(`Formato no válido: ${file.name}`, 'error');
            }
        }
        
        this.showLoading(false);
    }

    isValidFile(file) {
        const validTypes = [
            'text/csv',
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ];
        return validTypes.includes(file.type) || 
               file.name.endsWith('.csv') || 
               file.name.endsWith('.xlsx') || 
               file.name.endsWith('.xls');
    }

    async uploadAndProcessFile(file) {
        try {
            // Intentar enviar al backend para procesamiento
            const formData = new FormData();
            formData.append('file', file);
            formData.append('insert_to_database', 'false'); // No insertar automáticamente, solo procesar y mostrar

            console.log(`🚀 Enviando archivo ${file.name} al backend...`);

            // Agregar timeout a la petición
            const controller = new AbortController();
            const timeoutId = setTimeout(() => {
                console.warn(`⏰ Timeout alcanzado para ${file.name} - abortando petición`);
                controller.abort();
            }, 60000); // 60 segundos timeout

            const response = await fetch(`${this.backendUrl}/api/process-file`, {
                method: 'POST',
                body: formData,
                signal: controller.signal
            });

            clearTimeout(timeoutId);
            console.log(`✅ Respuesta del backend recibida para ${file.name}`);

            if (response.ok) {
                const result = await response.json();
                console.log(`📋 Resultado del procesamiento:`, result);
                this.processData(result.data);
                this.showNotification(`Archivo procesado: ${file.name}. Use "Exportar e Insertar en BD" para guardar en la base de datos.`, 'success');
                return;
            } else {
                const errorText = await response.text();
                console.error(`❌ Error en respuesta del backend:`, response.status, errorText);
                throw new Error(`Error del servidor: ${response.status}`);
            }
        } catch (error) {
            console.error('❌ Error procesando archivo:', error);
            if (error.name === 'AbortError') {
                this.showNotification(`Timeout procesando ${file.name} - operación cancelada después de 60s`, 'error');
            } else {
                this.showNotification(`Error procesando ${file.name}: ${error.message}`, 'error');
            }
        }

        // Fallback: procesamiento local
        console.log(`🔄 Intentando procesamiento local para ${file.name}`);
        await this.processFileLocally(file);
    }

    async processFileLocally(file) {
        const text = await file.text();
        
        if (file.name.endsWith('.csv')) {
            const data = this.parseCSV(text);
            const transformedData = this.transformDataLocally(data);
            this.processData(transformedData);
            this.showNotification(`Archivo procesado localmente: ${file.name}`, 'success');
        } else {
            this.showNotification('Formato Excel requiere backend activo', 'warning');
        }
    }

    parseCSV(text) {
        try {
            console.log('🔍 Parseando archivo CSV...');
            
            // Limpiar el texto y dividir en líneas
            const lines = text.trim().split('\n').filter(line => line.trim());
            
            if (lines.length === 0) {
                throw new Error('El archivo CSV está vacío');
            }
            
            // Parsear headers con mejor manejo de comillas y espacios
            const headerLine = lines[0];
            const headers = this.parseCSVLine(headerLine).map(h => h.trim().toLowerCase());
            
            console.log('📋 Columnas detectadas:', headers);
            
            // Validar que tenemos las columnas mínimas requeridas
            const requiredColumns = ['name', 'age', 'gender', 'income', 'illness'];
            const missingColumns = requiredColumns.filter(col => !headers.includes(col));
            
            if (missingColumns.length > 0) {
                console.warn('⚠️ Columnas faltantes:', missingColumns);
                console.warn('📋 Columnas disponibles:', headers);
                // Continuar pero mostrar advertencia
                this.showNotification(`Advertencia: Faltan columnas ${missingColumns.join(', ')}`, 'warning');
            }
            
            const data = [];
            
            // Parsear cada línea de datos
            for (let i = 1; i < lines.length; i++) {
                const line = lines[i].trim();
                if (line) {
                    try {
                        const values = this.parseCSVLine(line);
                        const row = {};
                        
                        headers.forEach((header, index) => {
                            row[header] = values[index]?.trim() || '';
                        });
                        
                        // Solo agregar filas que tengan al menos el nombre
                        if (row.name && row.name.trim()) {
                            data.push(row);
                        }
                    } catch (lineError) {
                        console.warn(`⚠️ Error parseando línea ${i + 1}:`, lineError.message);
                    }
                }
            }
            
            console.log(`✅ CSV parseado exitosamente: ${data.length} registros válidos`);
            
            if (data.length === 0) {
                throw new Error('No se encontraron registros válidos en el archivo CSV');
            }
            
            return data;
            
        } catch (error) {
            console.error('❌ Error parseando CSV:', error);
            throw new Error(`Error parseando archivo CSV: ${error.message}`);
        }
    }

    parseCSVLine(line) {
        // Función auxiliar para parsear una línea CSV manejando comillas
        const result = [];
        let current = '';
        let inQuotes = false;
        
        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            
            if (char === '"') {
                inQuotes = !inQuotes;
            } else if (char === ',' && !inQuotes) {
                result.push(current);
                current = '';
            } else {
                current += char;
            }
        }
        
        result.push(current); // Agregar el último campo
        return result;
    }

    transformDataLocally(data) {
        // Transformaciones básicas (simulando el ETL backend)
        const TRM = 3903.18; // TRM fija para demo
        const fechaProcesamiento = new Date().toISOString();
        
        return data.map(row => {
            // Normalizar valores de entrada
            const age = parseInt(row.age) || 0;
            const income = parseFloat(row.income) || 0;
            const genderOriginal = (row.gender || '').toString().trim();
            const illnessOriginal = (row.illness || '').toString().trim();
            
            // Transformar género
            let generoEs = 'N/A';
            const genderLower = genderOriginal.toLowerCase();
            if (genderLower === 'male' || genderLower === 'm') {
                generoEs = 'Masculino';
            } else if (genderLower === 'female' || genderLower === 'f') {
                generoEs = 'Femenino';
            }
            
            // Transformar enfermedad
            let enfermedadEs = 'No';
            const illnessLower = illnessOriginal.toLowerCase();
            if (illnessLower === 'yes' || illnessLower === 'y' || illnessLower === 'true' || illnessLower === '1') {
                enfermedadEs = 'Sí';
            } else if (illnessLower === 'no' || illnessLower === 'n' || illnessLower === 'false' || illnessLower === '0') {
                enfermedadEs = 'No';
            }
            
            // Calcular valores transformados
            const edadLustros = parseFloat((age / 5).toFixed(2));
            const ingresoCop = Math.round(income * TRM);
            
            // Retornar objeto con estructura esperada por processData()
            return {
                nombre: (row.name || '').toString().trim() || 'N/A',
                edad_anos: age,
                edad_lustros: edadLustros,
                genero_original: genderOriginal || 'N/A',
                genero_es: generoEs,
                ingreso_usd: income,
                ingreso_cop: ingresoCop,
                trm_utilizada: TRM,
                enfermedad_original: illnessOriginal || 'N/A',
                enfermedad_es: enfermedadEs,
                fecha_procesamiento: fechaProcesamiento
            };
        });
    }

    processData(data) {
        console.log('Raw data received in processData:', data.slice(0, 5));
        // Normalizar nombres de campos para consistencia entre datos procesados y de BD
        this.currentData = data.map(row => {
            return {
                nombre: row.nombre || 'N/A',
                edad_anos: row.edad_anos !== undefined ? row.edad_anos : 'N/A',
                edad_lustros: row.edad_lustros !== undefined ? row.edad_lustros : 'N/A',
                genero_original: row.genero_original || 'N/A',
                genero_es: row.genero_es || 'N/A',
                ingreso_usd: row.ingreso_usd !== undefined ? row.ingreso_usd : 'N/A',
                ingreso_cop: row.ingreso_cop !== undefined ? row.ingreso_cop : 'N/A',
                trm_utilizada: row.trm_utilizada !== undefined ? row.trm_utilizada : 'N/A',
                enfermedad_original: row.enfermedad_original || 'N/A',
                enfermedad_es: row.enfermedad_es || 'N/A',
                fecha_procesamiento: row.fecha_procesamiento || 'N/A'
            };
        });

        this.filteredData = this.currentData;
        this.updateStats();
        this.updateCharts();
        this.showDataSections();
    }

    updateStats() {
        const data = this.filteredData || this.currentData;
        
        // Elementos de estadísticas
        const totalRecordsEl = document.getElementById('totalRecords');
        const genderDistributionEl = document.getElementById('genderDistribution');
        const illnessRateEl = document.getElementById('illnessRate');
        const avgIncomeEl = document.getElementById('avgIncome');
        
        if (!data || data.length === 0) {
            if (totalRecordsEl) totalRecordsEl.textContent = '0';
            if (genderDistributionEl) genderDistributionEl.textContent = '0/0';
            if (illnessRateEl) illnessRateEl.textContent = '0.0%';
            if (avgIncomeEl) avgIncomeEl.textContent = '$0.0M';
            return;
        }
        
        // 1. Total de registros
        if (totalRecordsEl) totalRecordsEl.textContent = data.length;
        
        // 2. Distribución por género (M/F)
        const males = data.filter(d => (d.genero_es || '').toLowerCase() === 'masculino').length;
        const females = data.filter(d => (d.genero_es || '').toLowerCase() === 'femenino').length;
        if (genderDistributionEl) genderDistributionEl.textContent = `${males}/${females}`;

        // 3. Porcentaje con enfermedad
        const withIllness = data.filter(d => (d.enfermedad_es || '').toLowerCase() === 'sí').length;
        const illnessRate = data.length > 0 ? ((withIllness / data.length) * 100).toFixed(1) : 0;
        if (illnessRateEl) illnessRateEl.textContent = `${illnessRate}%`;
        
        // 4. Ingreso promedio en COP
        const totalIncome = data.reduce((sum, d) => sum + (d.ingreso_cop || 0), 0);
        const avgIncome = data.length > 0 ? totalIncome / data.length : 0;
        if (avgIncomeEl) {
            avgIncomeEl.innerHTML = `$${(avgIncome / 1000000).toFixed(1)}M<br><small>COP</small>`;
        }
    }

    updateCharts() {
        this.createGenderChart();
        this.createIncomeChart();
        this.createHealthChart();
        this.createAgeChart();
    }

    createGenderChart() {
        const ctx = document.getElementById('genderChart');
        if (!ctx) return;
        
        let males = 0;
        let females = 0;

        if (this.filteredData && this.filteredData.length > 0) {
            males = this.filteredData.filter(d => (d.genero_es || '').toLowerCase() === 'masculino').length;
            females = this.filteredData.filter(d => (d.genero_es || '').toLowerCase() === 'femenino').length;
        }
        
        if (this.charts.gender) this.charts.gender.destroy();
        
        this.charts.gender = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Masculino', 'Femenino'],
                datasets: [{
                    data: [males, females],
                    backgroundColor: ['#3498db', '#e74c3c']
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }

    createIncomeChart() {
        const ctx = document.getElementById('incomeChart');
        if (!ctx) return;
        
        const ranges = ['0-50M', '50-100M', '100-150M', '150M+'];
        let counts = [0, 0, 0, 0];

        if (this.filteredData && this.filteredData.length > 0) {
            const incomes = this.filteredData.map(d => (d.ingreso_cop || 0) / 1000000);
            counts = [
                incomes.filter(i => i < 50).length,
                incomes.filter(i => i >= 50 && i < 100).length,
                incomes.filter(i => i >= 100 && i < 150).length,
                incomes.filter(i => i >= 150).length
            ];
        }
        
        if (this.charts.income) this.charts.income.destroy();
        
        this.charts.income = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ranges,
                datasets: [{
                    label: 'Personas',
                    data: counts,
                    backgroundColor: '#2ecc71'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }

    createHealthChart() {
        const ctx = document.getElementById('healthChart');
        if (!ctx) return;

        let healthy = 0;
        let sick = 0;

        if (this.filteredData && this.filteredData.length > 0) {
            healthy = this.filteredData.filter(d => (d.enfermedad_es || '').toLowerCase() === 'no').length;
            sick = this.filteredData.filter(d => (d.enfermedad_es || '').toLowerCase() === 'sí').length;
        }
        
        if (this.charts.health) this.charts.health.destroy();
        
        this.charts.health = new Chart(ctx, {
            type: 'pie',
            data: {
                labels: ['Sano', 'Con Enfermedad'],
                datasets: [{
                    data: [healthy, sick],
                    backgroundColor: ['#2ecc71', '#f39c12']
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }

    createAgeChart() {
        const ctx = document.getElementById('ageChart');
        if (!ctx) return;
        
        const ranges = ['0-2', '2-4', '4-6', '6-8', '8+'];
        let counts = [0, 0, 0, 0, 0];

        if (this.filteredData && this.filteredData.length > 0) {
            const ages = this.filteredData.map(d => d.edad_lustros || 0);
            counts = [
                ages.filter(a => a < 2).length,
                ages.filter(a => a >= 2 && a < 4).length,
                ages.filter(a => a >= 4 && a < 6).length,
                ages.filter(a => a >= 6 && a < 8).length,
                ages.filter(a => a >= 8).length
            ];
        }
        
        if (this.charts.age) this.charts.age.destroy();
        
        this.charts.age = new Chart(ctx, {
            type: 'line',
            data: {
                labels: ranges,
                datasets: [{
                    label: 'Personas',
                    data: counts,
                    borderColor: '#9b59b6',
                    backgroundColor: 'rgba(155, 89, 182, 0.1)',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });
    }

    showDataSections() {
        document.getElementById('welcomeScreen').style.display = 'none';
        document.getElementById('statsGrid').style.display = 'grid';
        document.getElementById('dataTableContainer').style.display = 'block';
        document.getElementById('chartsContainer').style.display = 'block';
        document.getElementById('exportSection').style.display = 'block';
        
        // Render the data table
        this.renderDataTable();
    }

    // DataTable Methods
    applyFiltersAndRender() {
        this.applyFilters();
        this.renderDataTable();
        this.updateStats();
        this.updateCharts();
    }

    applyFilters() {
        if (!this.currentData) {
            this.filteredData = [];
            return;
        }

        let filtered = [...this.currentData];

        // Apply search filter
        if (this.searchTerm) {
            const searchLower = this.searchTerm.toLowerCase();
            filtered = filtered.filter(row => {
                return Object.values(row).some(value => 
                    String(value).toLowerCase().includes(searchLower)
                );
            });
        }

        // Apply gender filter
        if (this.filters.gender) {
            filtered = filtered.filter(row => 
                (row.genero_es || '').toLowerCase() === this.filters.gender.toLowerCase()
            );
        }

        // Apply illness filter
        if (this.filters.illness) {
            filtered = filtered.filter(row => 
                (row.enfermedad_es || '').toLowerCase() === this.filters.illness.toLowerCase()
            );
        }

        // Apply age range filter
        if (this.filters.ageRange) {
            filtered = filtered.filter(row => {
                const age = row.edad_anos || 0;
                switch (this.filters.ageRange) {
                    case '18-25': return age >= 18 && age <= 25;
                    case '26-35': return age >= 26 && age <= 35;
                    case '36-45': return age >= 36 && age <= 45;
                    case '46-55': return age >= 46 && age <= 55;
                    case '56+': return age >= 56;
                    default: return true;
                }
            });
        }

        this.filteredData = filtered;
    }

    renderDataTable() {
        if (!this.filteredData || this.filteredData.length === 0) {
            this.renderEmptyTable();
            return;
        }

        // Apply sorting
        let sortedData = [...this.filteredData];
        if (this.sortColumn) {
            sortedData.sort((a, b) => {
                let aVal = a[this.sortColumn];
                let bVal = b[this.sortColumn];

                // Handle numeric values
                if (typeof aVal === 'number' && typeof bVal === 'number') {
                    return this.sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
                }

                // Handle string values
                aVal = String(aVal || '').toLowerCase();
                bVal = String(bVal || '').toLowerCase();
                
                if (this.sortDirection === 'asc') {
                    return aVal.localeCompare(bVal);
                } else {
                    return bVal.localeCompare(aVal);
                }
            });
        }

        // Apply pagination
        const totalRecords = sortedData.length;
        const totalPages = Math.ceil(totalRecords / this.recordsPerPage);
        const startIndex = (this.currentPage - 1) * this.recordsPerPage;
        const endIndex = startIndex + this.recordsPerPage;
        const pageData = sortedData.slice(startIndex, endIndex);

        // Render table headers
        this.renderTableHeaders();

        // Render table body
        this.renderTableBody(pageData);

        // Render pagination
        this.renderPagination(totalRecords, totalPages);
    }

    renderTableHeaders() {
        const tableHeaders = document.getElementById('tableHeaders');
        if (!tableHeaders) return;

        const columnLabels = {
            nombre: 'Nombre',
            edad_anos: 'Edad (Años)',
            edad_lustros: 'Edad (Lustros)',
            genero_es: 'Género',
            ingreso_usd: 'Ingreso (USD)',
            ingreso_cop: 'Ingreso (COP)',
            trm_utilizada: 'TRM Utilizada',
            enfermedad_es: 'Enfermedad',
            fecha_procesamiento: 'Fecha Procesamiento'
        };

        let headersHTML = '';
        Object.entries(this.visibleColumns).forEach(([column, visible]) => {
            if (visible) {
                const label = columnLabels[column] || column;
                const sortIcon = this.getSortIcon(column);
                headersHTML += `
                    <th class="sortable-header" onclick="sortTable('${column}')">
                        ${label}
                        <span class="sort-icon ${sortIcon.class}">${sortIcon.icon}</span>
                    </th>
                `;
            }
        });

        tableHeaders.innerHTML = headersHTML;
    }

    getSortIcon(column) {
        if (this.sortColumn !== column) {
            return { class: '', icon: '↕️' };
        }
        
        if (this.sortDirection === 'asc') {
            return { class: 'asc', icon: '↑' };
        } else {
            return { class: 'desc', icon: '↓' };
        }
    }

    renderTableBody(data) {
        const tableBody = document.getElementById('tableBody');
        if (!tableBody) return;

        if (data.length === 0) {
            tableBody.innerHTML = `
                <tr>
                    <td colspan="${Object.values(this.visibleColumns).filter(v => v).length}" 
                        style="text-align: center; padding: 2rem; color: var(--dark-gray);">
                        No se encontraron registros que coincidan con los filtros aplicados
                    </td>
                </tr>
            `;
            return;
        }

        let bodyHTML = '';
        data.forEach(row => {
            bodyHTML += '<tr>';
            Object.entries(this.visibleColumns).forEach(([column, visible]) => {
                if (visible) {
                    let value = row[column];
                    
                    // Format specific columns
                    if (column === 'ingreso_cop' && typeof value === 'number') {
                        value = new Intl.NumberFormat('es-CO', {
                            style: 'currency',
                            currency: 'COP',
                            minimumFractionDigits: 0
                        }).format(value);
                    } else if (column === 'ingreso_usd' && typeof value === 'number') {
                        value = new Intl.NumberFormat('en-US', {
                            style: 'currency',
                            currency: 'USD',
                            minimumFractionDigits: 2
                        }).format(value);
                    } else if (column === 'fecha_procesamiento' && value !== 'N/A') {
                        try {
                            const date = new Date(value);
                            value = date.toLocaleString('es-CO');
                        } catch (e) {
                            // Keep original value if parsing fails
                        }
                    }
                    
                    bodyHTML += `<td>${value || 'N/A'}</td>`;
                }
            });
            bodyHTML += '</tr>';
        });

        tableBody.innerHTML = bodyHTML;
    }

    renderEmptyTable() {
        const tableHeaders = document.getElementById('tableHeaders');
        const tableBody = document.getElementById('tableBody');
        
        if (tableHeaders) {
            tableHeaders.innerHTML = '<th>No hay datos disponibles</th>';
        }
        
        if (tableBody) {
            tableBody.innerHTML = `
                <tr>
                    <td style="text-align: center; padding: 2rem; color: var(--dark-gray);">
                        Carga un archivo para ver los datos transformados
                    </td>
                </tr>
            `;
        }

        // Update pagination info
        const paginationInfo = document.getElementById('paginationInfo');
        if (paginationInfo) {
            paginationInfo.textContent = 'Mostrando 0 de 0 registros';
        }

        // Hide pagination controls
        const pageNumbers = document.getElementById('pageNumbers');
        if (pageNumbers) {
            pageNumbers.innerHTML = '';
        }

        const prevBtn = document.getElementById('prevPageBtn');
        const nextBtn = document.getElementById('nextPageBtn');
        if (prevBtn) prevBtn.disabled = true;
        if (nextBtn) nextBtn.disabled = true;
    }

    renderPagination(totalRecords, totalPages) {
        // Update pagination info
        const paginationInfo = document.getElementById('paginationInfo');
        if (paginationInfo) {
            const startRecord = totalRecords === 0 ? 0 : (this.currentPage - 1) * this.recordsPerPage + 1;
            const endRecord = Math.min(this.currentPage * this.recordsPerPage, totalRecords);
            paginationInfo.textContent = `Mostrando ${startRecord} a ${endRecord} de ${totalRecords} registros`;
        }

        // Update pagination controls
        const prevBtn = document.getElementById('prevPageBtn');
        const nextBtn = document.getElementById('nextPageBtn');
        
        if (prevBtn) {
            prevBtn.disabled = this.currentPage <= 1;
        }
        
        if (nextBtn) {
            nextBtn.disabled = this.currentPage >= totalPages;
        }

        // Render page numbers
        this.renderPageNumbers(totalPages);
    }

    renderPageNumbers(totalPages) {
        const pageNumbers = document.getElementById('pageNumbers');
        if (!pageNumbers) return;

        let numbersHTML = '';
        
        if (totalPages <= 7) {
            // Show all pages if 7 or fewer
            for (let i = 1; i <= totalPages; i++) {
                numbersHTML += this.createPageButton(i);
            }
        } else {
            // Show first page
            numbersHTML += this.createPageButton(1);
            
            if (this.currentPage > 4) {
                numbersHTML += '<span class="page-ellipsis">...</span>';
            }
            
            // Show pages around current page
            const start = Math.max(2, this.currentPage - 1);
            const end = Math.min(totalPages - 1, this.currentPage + 1);
            
            for (let i = start; i <= end; i++) {
                numbersHTML += this.createPageButton(i);
            }
            
            if (this.currentPage < totalPages - 3) {
                numbersHTML += '<span class="page-ellipsis">...</span>';
            }
            
            // Show last page
            if (totalPages > 1) {
                numbersHTML += this.createPageButton(totalPages);
            }
        }

        pageNumbers.innerHTML = numbersHTML;
    }

    createPageButton(pageNum) {
        const isActive = pageNum === this.currentPage;
        const activeClass = isActive ? 'active' : '';
        return `<span class="page-number ${activeClass}" onclick="goToPage(${pageNum})">${pageNum}</span>`;
    }

    sortTable(column) {
        if (this.sortColumn === column) {
            // Toggle sort direction
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            // New column, default to ascending
            this.sortColumn = column;
            this.sortDirection = 'asc';
        }
        
        this.renderDataTable();
    }

    changePage(direction) {
        const totalPages = Math.ceil((this.filteredData?.length || 0) / this.recordsPerPage);
        
        if (direction === -1 && this.currentPage > 1) {
            this.currentPage--;
        } else if (direction === 1 && this.currentPage < totalPages) {
            this.currentPage++;
        }
        
        this.renderDataTable();
    }

    goToPage(pageNum) {
        const totalPages = Math.ceil((this.filteredData?.length || 0) / this.recordsPerPage);
        
        if (pageNum >= 1 && pageNum <= totalPages) {
            this.currentPage = pageNum;
            this.renderDataTable();
        }
    }

    clearAllFilters() {
        // Reset all filters
        this.searchTerm = '';
        this.filters = {
            gender: '',
            illness: '',
            ageRange: ''
        };
        this.currentPage = 1;

        // Reset UI elements
        const searchInput = document.getElementById('searchInput');
        if (searchInput) searchInput.value = '';

        const genderFilter = document.getElementById('genderFilter');
        if (genderFilter) genderFilter.value = '';

        const illnessFilter = document.getElementById('illnessFilter');
        if (illnessFilter) illnessFilter.value = '';

        const ageRangeFilter = document.getElementById('ageRangeFilter');
        if (ageRangeFilter) ageRangeFilter.value = '';

        // Re-render table
        this.applyFiltersAndRender();
        this.showNotification('Filtros limpiados', 'info');
    }

    toggleColumnSelector() {
        // This would open a modal to select visible columns
        // For now, we'll just show a notification
        this.showNotification('Selector de columnas - Funcionalidad próximamente', 'info');
    }

    showLoading(show) {
        document.getElementById('loadingOverlay').style.display = show ? 'flex' : 'none';
    }

    showNotification(message, type = 'info') {
        // Crear notificación temporal
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.textContent = message;
        
        document.body.appendChild(notification);
        
        setTimeout(() => {
            notification.remove();
        }, 3000);
    }

    // Métodos para exportación
    async exportData(format) {
        if (!this.currentData || !this.currentData.length) {
            this.showNotification('No hay datos para exportar', 'warning');
            return;
        }
        
        // Intentar descargar desde el backend primero
        try {
            const response = await fetch(`${this.backendUrl}/api/download/${format}`);
            if (response.ok) {
                const blob = await response.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `otaku_data_latest.${format}`;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
                
                this.showNotification(`Archivo descargado desde el backend: otaku_data_latest.${format}`, 'success');
                return;
            }
        } catch (error) {
            console.log('Backend no disponible, exportando localmente');
        }
        
        // Fallback: exportación local
        switch (format) {
            case 'json':
                this.exportJSON();
                break;
            case 'csv':
                this.exportCSV();
                break;
            case 'sql':
                this.exportSQL();
                break;
        }
    }

    exportJSON() {
        const dataStr = JSON.stringify(this.currentData, null, 2);
        this.downloadFile(dataStr, 'datos_transformados.json', 'application/json');
    }

    exportCSV() {
        const headers = Object.keys(this.currentData[0]);
        const csvContent = [
            headers.join(','),
            ...this.currentData.map(row =>
                headers.map(h => `"${row[h]}"`).join(',')
            )
        ].join('\n');

        this.downloadFile(csvContent, 'datos_transformados.csv', 'text/csv');
    }

    exportSQL() {
        const sqlStatements = this.currentData.map(row => {
            const values = Object.values(row).map(v =>
                typeof v === 'string' ? `'${v}'` : v
            ).join(', ');
            return `INSERT INTO personas_transformadas VALUES (${values});`;
        });

        const sqlContent = sqlStatements.join('\n');
        this.downloadFile(sqlContent, 'insert_datos.sql', 'text/sql');
    }

    downloadFile(content, filename, mimeType) {
        const blob = new Blob([content], { type: mimeType });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        this.showNotification(`Archivo descargado: ${filename}`, 'success');
    }

    // Método para exportar a la nube
    async exportToCloud() {
        if (!this.currentData || this.currentData.length === 0) {
            this.showNotification('No hay datos para exportar a la nube', 'warning');
            return;
        }

        // Verificar conexión con el backend primero
        const backendConnected = await this.checkBackendConnection();
        if (!backendConnected) {
            this.showNotification('Backend no disponible. No se puede exportar a la nube.', 'error');
            return;
        }

        // Deshabilitar botón durante la exportación
        const exportBtn = document.getElementById('exportCloudBtn');
        if (exportBtn) {
            exportBtn.disabled = true;
            exportBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Exportando...';
        }

        this.showLoading(true);
        this.showNotification('Preparando datos para exportar a la nube...', 'info');

        try {
            // Crear archivo JSON con todos los datos
            const jsonData = JSON.stringify(this.currentData, null, 2);
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
            const filename = `datos_otaku_${timestamp}.json`;

            // Crear un blob y FormData para enviar
            const blob = new Blob([jsonData], { type: 'application/json' });
            const formData = new FormData();
            formData.append('file', blob, filename);
            formData.append('folder', 'exports');
            formData.append('add_timestamp', 'true');

            // Enviar a la API de la nube
            const response = await fetch(`${this.backendUrl}/api/cloud/upload`, {
                method: 'POST',
                body: formData
            });

            if (response.ok) {
                const result = await response.json();

                if (result.success) {
                    this.showNotification(
                        `¡Datos exportados exitosamente a la nube! Archivo: ${result.remote_file_name}`,
                        'success'
                    );

                    // Mostrar información adicional si está disponible
                    if (result.cloud_url) {
                        console.log(`URL en la nube: ${result.cloud_url}`);
                    }
                } else {
                    throw new Error(result.message || 'Error desconocido al subir a la nube');
                }
            } else {
                const errorData = await response.json();
                throw new Error(errorData.message || 'Error en la respuesta del servidor');
            }

        } catch (error) {
            console.error('Error exportando a la nube:', error);
            this.showNotification(
                `Error exportando a la nube: ${error.message}`,
                'error'
            );
        } finally {
            this.showLoading(false);

            // Rehabilitar botón
            if (exportBtn) {
                exportBtn.disabled = false;
                exportBtn.innerHTML = '<i class="fas fa-cloud-upload-alt"></i> Exportar a la Nube';
            }
        }
    }

    // Método para verificar el estado de la conexión a la nube
    async checkCloudConnection() {
        try {
            const response = await fetch(`${this.backendUrl}/api/cloud/status`);
            if (response.ok) {
                const status = await response.json();
                return status.connected;
            }
            return false;
        } catch (error) {
            console.log('Error verificando conexión a la nube:', error);
            return false;
        }
    }

    // Método: Exportar archivos E insertar en base de datos
    async exportAndInsertToDatabase() {
        if (!this.currentData || this.currentData.length === 0) {
            this.showNotification('No hay datos para exportar e insertar en la base de datos', 'warning');
            return;
        }

        // Verificar conexión con el backend
        const backendConnected = await this.checkBackendConnection();
        if (!backendConnected) {
            this.showNotification('Backend no disponible. No se puede insertar en la base de datos.', 'error');
            return;
        }

        // Deshabilitar botón durante la operación
        const exportInsertBtn = document.getElementById('exportInsertBtn');
        if (exportInsertBtn) {
            exportInsertBtn.disabled = true;
            exportInsertBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Exportando e Insertando...';
        }

        this.showLoading(true);
        this.showNotification('Exportando archivos e insertando en base de datos...', 'info');

        try {
            // Enviar datos al endpoint de exportar e insertar
            const response = await fetch(`${this.backendUrl}/api/export-and-insert`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    data: this.currentData,
                    export_formats: ['json', 'csv', 'sql'], // Formatos a exportar
                    insert_to_database: true
                })
            });

            if (response.ok) {
                const result = await response.json();
                
                if (result.success) {
                    let message = `¡Operación completada exitosamente!\n`;
                    message += `• ${result.records_inserted} registros insertados en BD\n`;
                    
                    if (result.files_created) {
                        message += `• Archivos creados: `;
                        const files = [];
                        if (result.files_created.json) files.push('JSON');
                        if (result.files_created.csv) files.push('CSV');
                        if (result.files_created.sql) files.push('SQL');
                        message += files.join(', ');
                    }
                    
                    this.showNotification(message, 'success');
                    
                    // Actualizar estadísticas del backend
                    this.loadBackendStats();
                } else {
                    throw new Error(result.error || 'Error desconocido en la operación');
                }
            } else {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Error en la respuesta del servidor');
            }

        } catch (error) {
            console.error('Error en exportar e insertar:', error);
            this.showNotification(
                `Error en la operación: ${error.message}`, 
                'error'
            );
        } finally {
            this.showLoading(false);
            
            // Rehabilitar botón
            if (exportInsertBtn) {
                exportInsertBtn.disabled = false;
                exportInsertBtn.innerHTML = '<i class="fas fa-database"></i> Exportar e Insertar en BD';
            }
        }
    }
}

// Funciones globales para eventos HTML
function exportData(format) {
    if (window.etlDashboard) {
        window.etlDashboard.exportData(format);
    }
}

function resetData() {
    if (window.etlDashboard) {
        window.etlDashboard.currentData = null;
        window.etlDashboard.filteredData = null;
        
        // Ocultar secciones de datos
        document.getElementById('statsGrid').style.display = 'none';
        document.getElementById('dataTableContainer').style.display = 'none';
        document.getElementById('chartsContainer').style.display = 'none';
        document.getElementById('exportSection').style.display = 'none';
        document.getElementById('welcomeScreen').style.display = 'block';
        
        window.etlDashboard.showNotification('Datos limpiados', 'info');
    }
}

// Funciones para exportación a la nube
async function exportToCloud() {
    if (window.etlDashboard) {
        await window.etlDashboard.exportToCloud();
    }
}

function openCloudDashboard() {
    // Abrir el dashboard de la nube en una nueva pestaña
    window.open('pages/cloud-dashboard.html', '_blank');
}

// Funciones para exportar e insertar en BD
async function exportAndInsertToDatabase() {
    if (window.etlDashboard) {
        await window.etlDashboard.exportAndInsertToDatabase();
    }
}

// Funciones globales para DataTable
function sortTable(column) {
    if (window.etlDashboard) {
        window.etlDashboard.sortTable(column);
    }
}

function changePage(direction) {
    if (window.etlDashboard) {
        window.etlDashboard.changePage(direction);
    }
}

function goToPage(pageNum) {
    if (window.etlDashboard) {
        window.etlDashboard.goToPage(pageNum);
    }
}

function clearAllFilters() {
    if (window.etlDashboard) {
        window.etlDashboard.clearAllFilters();
    }
}

function toggleColumnSelector() {
    if (window.etlDashboard) {
        window.etlDashboard.toggleColumnSelector();
    }
}

// Inicializar dashboard cuando se carga la página
document.addEventListener('DOMContentLoaded', () => {
    window.etlDashboard = new ETLDashboard();
});

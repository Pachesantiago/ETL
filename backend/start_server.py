"""
Script para iniciar el servidor API del ETL OtakuLATAM
"""
import os
import sys
import subprocess
import time
import webbrowser
from pathlib import Path

def check_dependencies():
    """Verificar que las dependencias estén instaladas"""
    try:
        import flask
        import flask_cors
        print("✅ Dependencias de Flask instaladas")
        return True
    except ImportError:
        print("❌ Dependencias faltantes. Instalando...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "Flask", "Flask-CORS"])
            print("✅ Dependencias instaladas exitosamente")
            return True
        except subprocess.CalledProcessError:
            print("❌ Error instalando dependencias")
            return False

def start_api_server():
    """Iniciar el servidor API"""
    print("🚀 Iniciando servidor API ETL OtakuLATAM...")
    
    # Cambiar al directorio del backend
    backend_dir = Path(__file__).parent
    os.chdir(backend_dir)
    
    # Verificar dependencias
    if not check_dependencies():
        return False
    
    try:
        # Iniciar servidor Flask
        from api.server import app
        
        print("\n" + "="*60)
        print("🌐 SERVIDOR API ETL OTAKULATAM INICIADO")
        print("="*60)
        print("📡 API Backend: http://localhost:8000")
        print("🌍 Frontend: Abrir frontend/index.html en navegador")
        print("📊 Health Check: http://localhost:8000/health")
        print("📋 Endpoints disponibles:")
        print("   - GET  /health                 - Estado del servidor")
        print("   - GET  /api/latest-data        - Datos más recientes")
        print("   - POST /api/process-file       - Procesar archivo")
        print("   - POST /api/run-etl            - Ejecutar ETL")
        print("   - GET  /api/database/records   - Registros de BD")
        print("   - GET  /api/stats              - Estadísticas")
        print("   - GET  /api/download/<type>    - Descargar archivos")
        print("="*60)
        print("💡 Presiona Ctrl+C para detener el servidor")
        print("="*60)
        
        # Ejecutar servidor
        app.run(
            host='0.0.0.0',
            port=8000,
            debug=False,  # Cambiar a False para producción
            use_reloader=False
        )
        
    except KeyboardInterrupt:
        print("\n🛑 Servidor detenido por el usuario")
        return True
    except Exception as e:
        print(f"❌ Error iniciando servidor: {e}")
        return False

def open_frontend():
    """Abrir el frontend en el navegador"""
    try:
        frontend_path = Path(__file__).parent.parent / "frontend" / "index.html"
        if frontend_path.exists():
            webbrowser.open(f"file://{frontend_path.absolute()}")
            print("🌍 Frontend abierto en el navegador")
        else:
            print("⚠️ Archivo frontend no encontrado")
    except Exception as e:
        print(f"⚠️ No se pudo abrir el frontend: {e}")

if __name__ == "__main__":
    print("🔧 ETL OtakuLATAM - Servidor API")
    print("Preparando entorno...")
    
    # Mostrar opciones
    print("\nOpciones:")
    print("1. Iniciar solo API (puerto 8000)")
    print("2. Iniciar API y abrir frontend")
    print("3. Solo abrir frontend")
    
    try:
        choice = input("\nSelecciona una opción (1-3) [2]: ").strip() or "2"
        
        if choice == "1":
            start_api_server()
        elif choice == "2":
            # Iniciar API en proceso separado y abrir frontend
            print("🚀 Iniciando API y frontend...")
            time.sleep(1)
            open_frontend()
            start_api_server()
        elif choice == "3":
            open_frontend()
        else:
            print("❌ Opción no válida")
            
    except KeyboardInterrupt:
        print("\n👋 Saliendo...")
    except Exception as e:
        print(f"❌ Error: {e}")

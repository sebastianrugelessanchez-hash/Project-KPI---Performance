"""
EJECUTAR PIPELINE ETL - Script Simple para Anaconda
====================================================
Script creado desde cero para v2.1

Uso:
    python ejecutar.py
"""

import os
import sys
from datetime import datetime

# Configurar UTF-8 para soportar emojis en Windows
sys.stdout.reconfigure(encoding='utf-8')


def print_header():
    """Imprime header"""
    print("\n" + "="*60)
    print("🚀 PIPELINE ETL v2.1 - PROCESAMIENTO AUTOMATIZADO")
    print("="*60 + "\n")




def verificar_archivos_datos():
    """Verifica que existan AMBOS archivos de datos usando las rutas del io_module"""
    print("📂 Verificando archivos de datos...")
    
    # Importar rutas desde io_module
    try:
        from etl_modules import io_module
        
        archivos_necesarios = {
            "Base de datos": io_module.DB_FILE_PATH,
            "Coordinadores": io_module.COORDINATORS_FILE_PATH
        }
        
        print(f"\n📂 Directorio configurado: {io_module.BASE_DIR}\n")
        
        archivos_faltantes = []
        archivos_encontrados = []
        
        for nombre, ruta in archivos_necesarios.items():
            if os.path.exists(ruta):
                archivos_encontrados.append((nombre, ruta))
                print(f"   ✅ {nombre}: {os.path.basename(ruta)}")
            else:
                archivos_faltantes.append((nombre, ruta))
                print(f"   ❌ {nombre}: {os.path.basename(ruta)} - NO ENCONTRADO")
        
        if archivos_faltantes:
            print(f"\n⚠️  ADVERTENCIA: Archivos faltantes:")
            for nombre, ruta in archivos_faltantes:
                print(f"   • {ruta}")
            
            print(f"\n💡 Verifica que los archivos estén en:")
            print(f"   {io_module.BASE_DIR}\n")
            
            respuesta = input("¿Deseas continuar de todas formas? (S/N): ")
            if respuesta.upper() != 'S':
                print("\n❌ Ejecución cancelada")
                print("\n📂 Asegúrate de tener los archivos en la ruta correcta:")
                print(f"   {io_module.BASE_DIR}")
                sys.exit(1)
        else:
            print("\n✅ Todos los archivos de datos encontrados")
        
        return True
        
    except ImportError as e:
        print(f"\n❌ ERROR: No se pudo importar etl_modules")
        print(f"   Detalles: {str(e)}")
        print("\n💡 Verifica que:")
        print("   1. La carpeta 'etl_modules' existe")
        print("   2. El archivo '__init__.py' está dentro de 'etl_modules'")
        print("   3. Todos los módulos están en la carpeta 'etl_modules'")
        sys.exit(1)


def verificar_dependencias():
    """Verifica que las dependencias estén instaladas"""
    print("\n📦 Verificando dependencias...")
    
    dependencias = {
        'pandas': 'pandas',
        'numpy': 'numpy',
        'openpyxl': 'openpyxl',
        'pyxlsb': 'pyxlsb'
    }
    
    faltantes = []
    
    for nombre, modulo in dependencias.items():
        try:
            __import__(modulo)
            print(f"   ✅ {nombre}")
        except ImportError:
            print(f"   ❌ {nombre} - NO INSTALADO")
            faltantes.append(nombre)
    
    if faltantes:
        print(f"\n⚠️  Dependencias faltantes: {', '.join(faltantes)}")
        print("\nPara instalar:")
        print("   pip install " + " ".join(faltantes))
        print("\nO usa conda:")
        print("   conda install " + " ".join(faltantes))
        sys.exit(1)
    
    print("\n✅ Todas las dependencias instaladas correctamente")


def crear_directorios():
    """Crea directorios necesarios"""
    print("\n📂 Preparando directorios...")

    from etl_modules import config

    # Crear directorio de salida si no existe
    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
        print(f"   📁 Creado: {config.OUTPUT_DIR}/")
    else:
        print(f"   ✓ Existe: {config.OUTPUT_DIR}/")


def ejecutar_pipeline():
    """Ejecuta la pipeline principal"""
    print("\n" + "="*60)
    print("⚙️  INICIANDO PIPELINE...")
    print("="*60 + "\n")
    
    inicio = datetime.now()
    
    try:
        # Importar y ejecutar main
        from main import main
        resultado = main()
        
        if resultado is None:
            print("\n⚠️  El pipeline terminó pero no devolvió resultados")
            return False
        
        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()
        
        print("\n" + "="*60)
        print("✅ PIPELINE COMPLETADA EXITOSAMENTE")
        print("="*60)
        print(f"\n⏱️  Tiempo total: {duracion:.2f} segundos")
        print(f"📊 Registros procesados: {len(resultado):,}")
        
        # Buscar archivo generado
        print("\n📄 Archivos generados:")
        
        # Buscar en directorio actual y en output/
        directorios_busqueda = ['.', 'output']
        archivos_encontrados = []

        for directorio in directorios_busqueda:
            if os.path.exists(directorio):
                archivos = [f for f in os.listdir(directorio)
                           if f.startswith('Performance') and f.endswith('.xlsx')]
                for archivo in archivos:
                    ruta_completa = os.path.join(directorio, archivo)
                    archivos_encontrados.append((ruta_completa, os.path.getmtime(ruta_completa)))
        
        # Ordenar por fecha (más reciente primero)
        archivos_encontrados.sort(key=lambda x: x[1], reverse=True)
        
        if archivos_encontrados:
            archivo_mas_reciente = archivos_encontrados[0][0]
            print(f"   • {archivo_mas_reciente}")
            print("\n   🔖 Pestañas:")
            print("      1. Resumen - Todos los datos")
            print("      2. APEX - Incidentes APEX")
            print("      3. COMMAND - Incidentes COMMAND")
            print("      4. Billing Coordinators - Desempeño de coordinadores")
            print("      5. Plants - Top 3 plantas por coordinador")
            print("      6. Issues - Distribución de categorías por biller")
        else:
            print("   ⚠️  No se encontró el archivo de salida")
        
        print("\n🎯 Siguiente paso: Subir a Looker Studio")
        print("="*60 + "\n")
        
        return True
        
    except ImportError as e:
        print("\n❌ ERROR: No se pudo importar el módulo 'main'")
        print(f"   Detalles: {str(e)}")
        print("\n💡 Verifica que el archivo 'main.py' existe")
        return False
        
    except Exception as e:
        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()
        
        print("\n" + "="*60)
        print("❌ ERROR EN LA EJECUCIÓN")
        print("="*60)
        print(f"\n🐛 {str(e)}")
        print(f"\n⏱️  Tiempo transcurrido: {duracion:.2f} segundos")
        
        # Mostrar traceback completo
        import traceback
        print("\n📋 Detalles del error:")
        print("-" * 60)
        traceback.print_exc()
        print("-" * 60)
        
        print("\n💡 Sugerencias:")
        print("   1. Verifica que todos los archivos de datos existen")
        print("   2. Revisa que los módulos estén correctamente configurados")
        print("   3. Verifica las rutas de los archivos en io_module.py")
        print("="*60 + "\n")
        
        return False


def main():
    """Función principal"""
    print_header()
    
    # Verificaciones previas
    verificar_archivos_datos()
    verificar_dependencias()
    crear_directorios()
    
    # Ejecutar pipeline
    exito = ejecutar_pipeline()
    
    # Mensaje final
    if exito:
        input("\n✅ Presiona Enter para salir...")
    else:
        input("\n❌ Presiona Enter para salir...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Ejecución interrumpida por el usuario")
        print("="*60 + "\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        print("="*60 + "\n")
        sys.exit(1)
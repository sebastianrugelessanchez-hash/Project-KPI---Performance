"""
ETL Modules Package
===================
Paquete modular para procesamiento de datos de facturación

Módulos disponibles:
- config: Configuración y constantes
- io_module: Lectura y escritura de archivos
- processing: Limpieza y procesamiento de datos
- transformation: Transformaciones y categorizaciones
- output: Exportación de resultados
"""

__version__ = "2.0"
__author__ = "ETL Pipeline Team"

# Importar módulos principales para facilitar el acceso
from . import config
from . import io_module
from . import processing
from . import transformation
from . import output

# Lista de módulos exportados
__all__ = [
    'config',
    'io_module',
    'processing',
    'transformation',
    'output'
]
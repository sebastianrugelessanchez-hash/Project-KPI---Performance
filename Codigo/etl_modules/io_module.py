"""
I/O Module
==========
Módulo para lectura y escritura de archivos Excel

VERSIÓN 2.0 - Usa archivo separado "Billing Coordinators.xlsx"
"""

import pandas as pd
import numpy as np
from typing import Dict
import warnings
import os
import re
warnings.filterwarnings('ignore')


# ============================================================
# FUNCIONES DE NORMALIZACIÓN
# ============================================================

def normalize_text(text):
    """
    Normaliza texto eliminando números (0-9), caracteres especiales,
    valores mixtos (letras con números como D245, DB09, AGG3858) y
    convierte todo a minúsculas.

    Ejemplos:
    - "Delivery 820235055 is incomplete" → "delivery is incomplete"
    - "Invoice-Error_123" → "invoice error"
    - "D245 issue" → "issue"
    - "Status: DB09" → "status"
    - "AGG3858_Problem" → "problem"

    Args:
        text: Texto a normalizar

    Returns:
        Texto normalizado en minúsculas sin números ni caracteres especiales
    """
    if pd.isna(text) or text == '':
        return text

    # Convertir a string si no lo es
    text = str(text)

    # 1. Convertir a minúsculas
    text = text.lower()

    # 2. Reemplazar caracteres especiales por espacios (para mantener separación de palabras)
    # Esto convierte "Invoice-Error" en "Invoice Error"
    text = re.sub(r'[^a-z0-9\s]', ' ', text)

    # 3. Eliminar valores mixtos (palabras que contienen números)
    # Esto elimina palabras como "D245", "DB09", "AGG3858", "123"
    text = re.sub(r'\b\w*\d+\w*\b', '', text)

    # 4. Eliminar espacios múltiples
    text = re.sub(r'\s+', ' ', text)

    # 5. Eliminar espacios al inicio y final
    text = text.strip()

    return text


def normalize_dataframe_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Normaliza una columna específica del DataFrame.

    Args:
        df: DataFrame
        column_name: Nombre de la columna a normalizar

    Returns:
        DataFrame con la columna normalizada
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(normalize_text)
    return df


# ============================================================
# RUTAS DE ARCHIVOS
# ============================================================
BASE_DIR = r"C:\Users\Sebas\OneDrive\Desktop\Proyecto KPI\Base de datos"

# Nombres de archivos (ambos están en BASE_DIR)
DB_FILE_NAME = "2025-08 DB WF.xlsb"
COORDINATORS_FILE_NAME = "Billing Coordinators.xlsx"

# Rutas completas
DB_FILE_PATH = os.path.join(BASE_DIR, DB_FILE_NAME)
COORDINATORS_FILE_PATH = os.path.join(BASE_DIR, COORDINATORS_FILE_NAME)


def load_excel_data(file_path: str = None) -> pd.DataFrame:
    """
    Carga los datos de la hoja DB del archivo Excel principal

    Args:
        file_path: Ruta al archivo Excel principal. Si no se proporciona,
                   usa la ruta por defecto configurada.

    Returns:
        DataFrame con datos de la hoja DB, con columna "Work item text" normalizada
    """
    # Si no se proporciona ruta, usar la ruta por defecto
    if file_path is None:
        file_path = DB_FILE_PATH

    print(f"   📂 Cargando archivo principal: {file_path}")

    # Verificar que el archivo existe
    if not os.path.exists(file_path):
        print(f"   ❌ ERROR: Archivo no encontrado en: {file_path}")
        raise FileNotFoundError(f"El archivo no existe: {file_path}")

    # Cargar hoja DB
    print("   • Leyendo hoja 'DB'...")
    db_df = pd.read_excel(
        file_path,
        sheet_name='DB',
        engine='pyxlsb'  # Engine específico para .xlsb
    )

    # Normalizar columna "Work item text" si existe
    if 'Work item text' in db_df.columns:
        print("   • Normalizando columna 'Work item text'...")
        db_df = normalize_dataframe_column(db_df, 'Work item text')
        print("   ✓ Columna normalizada (números, caracteres especiales y valores mixtos eliminados)")

    return db_df


def load_billing_coordinators(file_path: str = None) -> pd.DataFrame:
    """
    Carga el archivo de Billing Coordinators (archivo separado)
    
    Args:
        file_path: Ruta al archivo de Billing Coordinators. Si no se proporciona,
                   usa la ruta por defecto configurada.
        
    Returns:
        DataFrame con información de coordinadores por Plant
    """
    # Si no se proporciona ruta, usar la ruta por defecto
    if file_path is None:
        file_path = COORDINATORS_FILE_PATH
    
    print(f"   📂 Cargando archivo de coordinadores: {file_path}")
    
    try:
        # Verificar que el archivo existe
        if not os.path.exists(file_path):
            print(f"   ❌ ERROR: Archivo no encontrado en: {file_path}")
            raise FileNotFoundError(f"El archivo no existe: {file_path}")
        
        coordinators_df = pd.read_excel(file_path, engine='openpyxl')
        print(f"   ✓ Coordinadores cargados: {len(coordinators_df):,} registros")
        
        # Verificar que existe columna Plant
        if 'Plant' not in coordinators_df.columns:
            available_cols = list(coordinators_df.columns)
            print(f"   ❌ ERROR: Columna 'Plant' no encontrada")
            print(f"   Columnas disponibles: {available_cols}")
            raise ValueError("Columna 'Plant' es requerida en Billing Coordinators")
        
        return coordinators_df
        
    except FileNotFoundError:
        print(f"   ❌ ERROR: Archivo '{file_path}' no encontrado")
        print(f"   Asegúrate de que el archivo esté en: {file_path}")
        raise
    except Exception as e:
        print(f"   ❌ ERROR cargando coordinadores: {str(e)}")
        raise


def save_to_excel(df: pd.DataFrame, filename: str, sheet_name: str = 'Data'):
    """
    Guarda un DataFrame a un archivo Excel
    
    Args:
        df: DataFrame a guardar
        filename: Nombre del archivo de salida
        sheet_name: Nombre de la hoja
    """
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"   ✓ Archivo guardado: {filename}")


def save_multiple_sheets(data_dict: Dict[str, pd.DataFrame], filename: str):
    """
    Guarda múltiples DataFrames en diferentes hojas de un mismo archivo Excel
    
    Args:
        data_dict: Diccionario {nombre_hoja: dataframe}
        filename: Nombre del archivo de salida
    """
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for sheet_name, df in data_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"   ✓ Archivo multi-hoja guardado: {filename}")


# Lista de módulos disponibles
AVAILABLE_MODULES = [
    "config - Configuración y constantes",
    "io_module - Lectura y escritura de archivos",
    "processing - Limpieza y procesamiento de datos",
    "transformation - Transformaciones y categorizaciones",
    "output - Exportación de resultados"
]


def list_modules():
    """Imprime la lista de módulos disponibles"""
    print("\n📦 MÓDULOS DISPONIBLES EN LA PIPELINE:")
    for module in AVAILABLE_MODULES:
        print(f"   • {module}")


def print_config():
    """Imprime la configuración de rutas actual"""
    print("\n⚙️  CONFIGURACIÓN DE RUTAS:")
    print(f"   📁 Directorio base: {BASE_DIR}")
    print(f"   📄 Archivo DB: {DB_FILE_NAME}")
    print(f"   📄 Archivo Coordinadores: {COORDINATORS_FILE_NAME}")
    print(f"\n   Ruta completa DB: {DB_FILE_PATH}")
    print(f"   Ruta completa Coordinadores: {COORDINATORS_FILE_PATH}")
    print(f"\n   ✓ Archivo DB existe: {os.path.exists(DB_FILE_PATH)}")
    print(f"   ✓ Coordinadores existe: {os.path.exists(COORDINATORS_FILE_PATH)}")
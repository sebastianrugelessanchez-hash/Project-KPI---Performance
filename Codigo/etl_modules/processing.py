"""
Processing Module
=================
Módulo robusto para procesamiento de datos con chunks y vectorización

VERSIÓN 2.1 - INNER JOIN con Billing Coordinators
Nota: La columna resultante es 'BILLING COORDINATORS' (mayúscula, con espacio)
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Generator
from . import config


class ChunkProcessor:
    """Procesador de datos por chunks con vectorización"""
    
    def __init__(self, chunk_size: int = config.CHUNK_SIZE):
        self.chunk_size = chunk_size
        self.processed_chunks = []
        
    def process_in_chunks(
        self, 
        df: pd.DataFrame, 
        process_func: callable
    ) -> pd.DataFrame:
        """
        Procesa un DataFrame en chunks aplicando una función
        
        Args:
            df: DataFrame a procesar
            process_func: Función a aplicar a cada chunk
            
        Returns:
            DataFrame procesado y consolidado
        """
        total_rows = len(df)
        num_chunks = (total_rows // self.chunk_size) + 1
        
        print(f"   📄 Procesando {total_rows:,} filas en {num_chunks} chunks...")
        
        for i, chunk in enumerate(self._chunk_generator(df)):
            processed_chunk = process_func(chunk)
            self.processed_chunks.append(processed_chunk)
            
            if (i + 1) % 5 == 0:  # Progress update every 5 chunks
                progress = ((i + 1) / num_chunks) * 100
                print(f"      Progreso: {progress:.1f}% ({i+1}/{num_chunks} chunks)")
        
        result = pd.concat(self.processed_chunks, ignore_index=True)
        self.processed_chunks = []  # Reset for next use
        return result
    
    def _chunk_generator(self, df: pd.DataFrame) -> Generator:
        """Genera chunks del DataFrame"""
        for start in range(0, len(df), self.chunk_size):
            yield df.iloc[start:start + self.chunk_size]


def recognize_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Reconoce y valida las columnas del DataFrame DB
    
    Args:
        df: DataFrame de la hoja DB
        
    Returns:
        Diccionario con el mapeo de columnas
    """
    expected_columns = [
        'Task text', 'Sales Office', 'Sales Group', 'Sales district', 
        'Plant', 'Sold-to party', 'Name 1', 'Ship-to party', 
        'Ticket', 'IDOC/SD Document', 'Work item text', 'ID', 
        'Product Code', 'Command Order No.', 'Truck Type', 'Date',
        'Delivery quantity', 'Base Unit of Measure', 'Ticket Date',
        'Actual (last) agent', 'Object Type', 'OK - Actual End Date of Work Item',
        'Stronghold'
    ]
    
    column_mapping = {}
    missing_columns = []
    
    for col in expected_columns:
        if col in df.columns:
            column_mapping[col] = col
        else:
            missing_columns.append(col)
    
    if missing_columns:
        print(f"   ⚠️  Columnas faltantes: {missing_columns}")
    else:
        print(f"   ✓ Todas las columnas reconocidas correctamente")
    
    return column_mapping


def filter_batchman_vectorized(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas cuya columna 'Work item text' contenga la frase
    'is currently being processed' (independientemente del usuario).
    Tolerante a mayúsculas/minúsculas, espacios múltiples y caracteres invisibles.
    """
    work_item_col = 'Work item text'
    if work_item_col not in chunk.columns:
        try:
            k_idx = config.DB_COLUMNS['WORK_ITEM_TEXT']  # Índice de la columna K
            work_item_col = chunk.columns[k_idx]
        except Exception:
            print(f"   ⚠️  Columna '{work_item_col}' no encontrada en chunk")
            return chunk

    before = len(chunk)

    # Normalizar texto
    s_norm = (
        chunk[work_item_col]
        .astype(str)
        .str.replace(r'[\u00A0\u200B\u200C\u200D\uFEFF]', ' ', regex=True)  # NBSP/ZW chars
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
        .str.lower()
    )

    # Frase genérica a buscar
    phrase = "is currently being processed"

    # Mantener solo filas que NO contienen la frase
    keep_mask = ~s_norm.str.contains(phrase, case=False, regex=False, na=False)
    filtered_chunk = chunk.loc[keep_mask].copy()

    removed = before - len(filtered_chunk)
    if removed > 0:
        print(f"      • Chunk: {removed} filas eliminadas por frase '{phrase}' en '{work_item_col}'")

    return filtered_chunk


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia los datos eliminando registros con BATCHMAN
    Usa procesamiento por chunks para manejar grandes volúmenes (800k+ registros)
    
    Args:
        df: DataFrame de la hoja DB
        
    Returns:
        DataFrame limpio sin registros de BATCHMAN
    """
    print("   • Filtrando registros con BATCHMAN en 'Work item text'...")
    
    # Verificar que existe la columna
    if 'Work item text' not in df.columns:
        print("   ⚠️  Columna 'Work item text' no encontrada")
        return df
    
    # Reconocer columnas
    recognize_columns(df)
    
    # Contar registros antes del filtrado
    initial_count = len(df)
    
    # Crear procesador por chunks para manejar grandes volúmenes
    processor = ChunkProcessor()
    
    # Procesar en chunks con filtro vectorizado
    clean_df = processor.process_in_chunks(df, filter_batchman_vectorized)
    
    # Contar registros después del filtrado
    final_count = len(clean_df)
    removed_count = initial_count - final_count
    
    print(f"   ✓ Registros después de limpieza: {final_count:,}")
    print(f"   ✓ Registros eliminados (BATCHMAN): {removed_count:,}")
    
    return clean_df


def merge_with_billing_coordinators(
    db_df: pd.DataFrame, 
    coordinators_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Une los datos de DB con Billing Coordinators usando Plant como llave
    IMPORTANTE: Usa INNER JOIN - solo mantiene registros con match
    
    Nota: La columna resultante se llama 'BILLING COORDINATORS' (mayúscula, con espacio)
    
    Args:
        db_df: DataFrame limpio de la hoja DB
        coordinators_df: DataFrame del archivo Billing Coordinators.xlsx
        
    Returns:
        DataFrame enriquecido con información de Billing Coordinators
    """
    print("   🔗 Realizando INNER JOIN por Plant...")
    
    # Asegurar que Plant sea del mismo tipo en ambos DataFrames
    db_df['Plant'] = pd.to_numeric(db_df['Plant'], errors='coerce')
    
    # Verificar que existe la columna Plant en coordinators
    if 'Plant' not in coordinators_df.columns:
        print("   ❌ ERROR: Columna 'Plant' no encontrada en Billing Coordinators")
        print(f"   Columnas disponibles: {list(coordinators_df.columns)}")
        raise ValueError("Columna 'Plant' no encontrada en archivo de coordinadores")
    
    coordinators_df['Plant'] = pd.to_numeric(coordinators_df['Plant'], errors='coerce')
    
    # Registros antes del merge
    before_count = len(db_df)
    
    # Realizar INNER JOIN (solo registros con match en ambas tablas)
    enriched_df = db_df.merge(
        coordinators_df,
        on='Plant',
        how='inner',  # INNER JOIN - solo registros con match
        suffixes=('', '_coord')
    )
    
    # Reportar estadísticas del merge
    after_count = len(enriched_df)
    match_rate = (after_count / before_count) * 100
    filtered_out = before_count - after_count
    
    print(f"   ✓ INNER JOIN completado:")
    print(f"      • Registros antes: {before_count:,}")
    print(f"      • Registros después: {after_count:,}")
    print(f"      • Registros con match: {match_rate:.1f}%")
    print(f"      • Registros filtrados: {filtered_out:,}")
    
    if filtered_out > 0:
        # Mostrar plants que fueron filtradas
        plants_in_db = set(db_df['Plant'].dropna().unique())
        plants_in_coord = set(coordinators_df['Plant'].dropna().unique())
        plants_filtered = plants_in_db - plants_in_coord
        
        if len(plants_filtered) > 0:
            print(f"      • Plants sin coordinador: {len(plants_filtered)}")
            print(f"        Ejemplos: {list(plants_filtered)[:5]}")
    
    return enriched_df


def filter_by_agents(df: pd.DataFrame, agent_list: List[str] = None) -> pd.DataFrame:
    """
    Filtra el DataFrame para mantener solo registros de agentes específicos

    Args:
        df: DataFrame con datos enriquecidos
        agent_list: Lista de códigos de agentes a mantener
                   Si es None, usa la lista por defecto

    Returns:
        DataFrame filtrado con solo los agentes especificados
    """
    # Lista por defecto de agentes
    if agent_list is None:
        agent_list = ['SRUGELES', 'CAMVELEZ', 'JUAHENA', 'JUANRUIZ', 'REGARCI1', 'SPINEDAA', 'MPEREZPA', 'CHREVANS']

    print(f"   🔍 Filtrando por agentes específicos: {len(agent_list)} agentes")

    # Verificar que existe la columna 'Actual (last) agent'
    if 'Actual (last) agent' not in df.columns:
        print(f"   ⚠️  Columna 'Actual (last) agent' no encontrada")
        print(f"   Columnas disponibles: {list(df.columns)}")
        return df

    before_count = len(df)

    # Filtrar por agentes en la lista
    filtered_df = df[df['Actual (last) agent'].isin(agent_list)].copy()

    after_count = len(filtered_df)
    removed_count = before_count - after_count

    print(f"   ✓ Filtro de agentes aplicado:")
    print(f"      • Registros antes: {before_count:,}")
    print(f"      • Registros después: {after_count:,}")
    print(f"      • Registros filtrados: {removed_count:,}")

    # Mostrar agentes encontrados
    agents_found = filtered_df['Actual (last) agent'].unique()
    print(f"   ✓ Agentes encontrados: {len(agents_found)}")
    for agent in sorted(agents_found):
        count = len(filtered_df[filtered_df['Actual (last) agent'] == agent])
        print(f"      • {agent}: {count:,} registros")

    return filtered_df


def validate_data_quality(df: pd.DataFrame) -> Dict[str, any]:
    """
    Valida la calidad de los datos procesados
    
    Args:
        df: DataFrame a validar
        
    Returns:
        Diccionario con métricas de calidad
    """
    metrics = {
        'total_records': len(df),
        'null_counts': df.isnull().sum().to_dict(),
        'duplicate_count': df.duplicated().sum(),
        'unique_plants': df['Plant'].nunique() if 'Plant' in df.columns else 0,
        'unique_tasks': df['Task text'].nunique() if 'Task text' in df.columns else 0
    }
    
    print("\n   📊 MÉTRICAS DE CALIDAD:")
    print(f"      • Total registros: {metrics['total_records']:,}")
    print(f"      • Duplicados: {metrics['duplicate_count']:,}")
    print(f"      • Plants únicos: {metrics['unique_plants']:,}")
    print(f"      • Tipos de tareas únicos: {metrics['unique_tasks']:,}")
    
    return metrics


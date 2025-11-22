"""
Transformation Module
=====================
Módulo para transformaciones y categorizaciones de datos
"""

import pandas as pd
import numpy as np
from typing import Dict, List
from . import config


def categorize_incidents(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categoriza los incidentes basándose en el Task text
    
    Args:
        df: DataFrame con datos enriquecidos
        
    Returns:
        DataFrame con columna 'Category' agregada
    """
    print("   🏷️  Categorizando incidentes...")
    
    # Crear columna de categoría usando vectorización
    df['Category'] = df['Task text'].map(config.TASK_TO_CATEGORY)
    
    # Marcar registros sin categoría
    uncategorized_count = df['Category'].isna().sum()
    df['Category'] = df['Category'].fillna('Other')
    
    # Reportar estadísticas
    category_stats = df['Category'].value_counts()
    
    print(f"   ✓ Categorización completada")
    print(f"      • Registros categorizados: {len(df) - uncategorized_count:,}")
    print(f"      • Registros sin categoría (Other): {uncategorized_count:,}")
    
    return df


def calculate_billing_coordinator_performance(df: pd.DataFrame, agent_column: str = 'Actual (last) agent') -> pd.DataFrame:
    """
    Calcula el desempeño de cada Billing Coordinator usando métricas:

    1. Promedio de Días Dedicados = promedio de ("OK - Actual End Date of Work Item" - "Date")
    2. Tickets procesados = conteo de tickets únicos (validados por Object Type)
    3. Categoria_Principal = categoría más frecuente (excluyendo "Inventory")
    4. Issue = subcategoría más frecuente (moda de "Work item text" dentro de su categoría)

    Args:
        df: DataFrame con datos enriquecidos y categorizados
        agent_column: Nombre de la columna a usar (por defecto 'Actual (last) agent')

    Returns:
        DataFrame con métricas de desempeño por Billing Coordinator
    """
    print("   📊 Calculando desempeño de Billing Coordinators...")

    # Verificar columnas necesarias
    required_columns = [
        agent_column, 'Plant', 'Date',
        'OK - Actual End Date of Work Item', 'Ticket', 'Object Type', 'Work item text'
    ]
    
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"   ⚠️  Columnas faltantes: {missing_cols}")
        return pd.DataFrame()
    
    # Crear copia para no modificar el original
    work_df = df.copy()
    
    # Convertir fechas desde formato de número de serie de Excel (int64)
    work_df['Date'] = pd.to_datetime(work_df['Date'], unit='D', origin=pd.Timestamp('1900-01-01'), errors='coerce')
    work_df['OK - Actual End Date of Work Item'] = pd.to_datetime(
        work_df['OK - Actual End Date of Work Item'], 
        unit='D', 
        origin=pd.Timestamp('1900-01-01'),
        errors='coerce'
    )
    
    # Calcular Días dedicados por registro
    work_df['Dias_Dedicados'] = (
        work_df['OK - Actual End Date of Work Item'] - work_df['Date']
    ).dt.days
    
    # Reemplazar valores negativos o nulos por 0
    work_df['Dias_Dedicados'] = work_df['Dias_Dedicados'].fillna(0)
    work_df.loc[work_df['Dias_Dedicados'] < 0, 'Dias_Dedicados'] = 0
    
    # Agrupar por Agent
    performance = work_df.groupby(agent_column).agg({
        'Dias_Dedicados': 'mean',  # PROMEDIO de días dedicados
        'ID': 'nunique',           # IDs únicos procesados
        'Plant': 'nunique',        # Plants asociadas
    }).reset_index()
    
    performance.columns = [
        'Billing_Coordinator',
        'Average_Days_Spent',
        'Tickets_Processed',
        'Associated_Plants'
    ]
    
    # ============================================================
    # CALCULAR CATEGORIA_PRINCIPAL (excluyendo Inventory)
    # ============================================================
    
    def get_category_principal(coordinator_name):
        """Obtiene la categoría más frecuente excluyendo Inventory"""
        mask = work_df[agent_column] == coordinator_name
        filtered_categories = work_df[mask]['Category']
        
        # Excluir "Inventory" y obtener la más frecuente
        filtered_categories = filtered_categories[filtered_categories != 'Inventory']
        
        if len(filtered_categories) == 0:
            return 'No Category'
        
        return filtered_categories.value_counts().index[0]
    
    performance['Main_Category'] = performance['Billing_Coordinator'].apply(get_category_principal)
    
    
    # ============================================================
    # CALCULAR ISSUE (moda de Work item text por categoría)
    # ============================================================
    
    def get_issue_for_coordinator(coordinator_name, category):
        """Obtiene la subcategoría más frecuente (moda) de Work item text para un coordinador y categoría"""
        mask = (work_df[agent_column] == coordinator_name) & (work_df['Category'] == category)
        filtered = work_df[mask]['Work item text']
        
        if len(filtered) == 0:
            return 'Unknown'
        
        # Obtener la moda (valor más frecuente)
        return filtered.value_counts().index[0]
    
    # Aplicar función para obtener Issue
    performance['Issue'] = performance.apply(
        lambda row: get_issue_for_coordinator(row['Billing_Coordinator'], row['Main_Category']),
        axis=1
    )

    # ============================================================
    # CALCULAR NUMERO_DE_VECES (contar apariciones del issue)
    # ============================================================

    def count_issue_occurrences(coordinator_name, category, issue):
        """Cuenta cuántas veces aparece un issue específico para un coordinador y categoría"""
        mask = (work_df[agent_column] == coordinator_name) & (work_df['Category'] == category) & (work_df['Work item text'] == issue)
        return len(work_df[mask])

    performance['Occurrences'] = performance.apply(
        lambda row: count_issue_occurrences(row['Billing_Coordinator'], row['Main_Category'], row['Issue']),
        axis=1
    )

    # ============================================================
    # CALCULAR NUMERO_DE_VECES y PORCENTAJE para Categoria_Principal
    # ============================================================

    def count_category_occurrences(coordinator_name, category):
        """Cuenta cuántas veces aparece una categoría específica para un coordinador"""
        mask = (work_df[agent_column] == coordinator_name) & (work_df['Category'] == category)
        return len(work_df[mask])

    def calculate_category_percentage(coordinator_name, category):
        """Calcula el porcentaje de una categoría respecto al total de registros del coordinador"""
        total_coordinator = len(work_df[work_df[agent_column] == coordinator_name])
        category_count = count_category_occurrences(coordinator_name, category)
        if total_coordinator == 0:
            return '0%'
        percentage = round((category_count / total_coordinator) * 100, 2)
        return f'{percentage}%'

    performance['Category_Count'] = performance.apply(
        lambda row: count_category_occurrences(row['Billing_Coordinator'], row['Main_Category']),
        axis=1
    )

    performance['Category_Percentage'] = performance.apply(
        lambda row: calculate_category_percentage(row['Billing_Coordinator'], row['Main_Category']),
        axis=1
    )

    # Filtrar Main_Category para excluir "Inventory"
    performance_filtered = performance[performance['Main_Category'] != 'Inventory'].copy()

    # Reordenar columnas para mejor visualización
    performance_filtered = performance_filtered[[
        'Billing_Coordinator',
        'Average_Days_Spent',
        'Tickets_Processed',
        'Associated_Plants',
        'Main_Category',
        'Category_Count',
        'Category_Percentage',
        'Issue',
        'Occurrences'
    ]]
    
    print(f"   ✓ Desempeño calculado para {len(performance_filtered)} coordinadores (excluyendo Inventory)")
    
    return performance_filtered


def create_category_subsets(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Crea subconjuntos de datos por categoría
    
    Args:
        df: DataFrame categorizado
        
    Returns:
        Diccionario con {categoria: dataframe}
    """
    print("   📦 Creando subconjuntos por categoría...")
    
    subsets = {}
    for category in config.INCIDENT_CATEGORIES.keys():
        subset = df[df['Category'] == category].copy()
        if len(subset) > 0:
            subsets[category] = subset
            print(f"      • {category}: {len(subset):,} registros")
    
    # Agregar categoría 'Other'
    other_subset = df[df['Category'] == 'Other'].copy()
    if len(other_subset) > 0:
        subsets['Other'] = other_subset
        print(f"      • Other: {len(other_subset):,} registros")
    
    return subsets


def add_calculated_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega campos calculados útiles para análisis
    
    Args:
        df: DataFrame base
        
    Returns:
        DataFrame con campos adicionales
    """
    print("   ➕ Agregando campos calculados...")
    
    # Extraer año y mes de la fecha
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        df['Month_Name'] = df['Date'].dt.strftime('%B')
        df['Week'] = df['Date'].dt.isocalendar().week
    
    # Calcular días desde la fecha del ticket
    if 'Ticket Date' in df.columns and 'Date' in df.columns:
        df['Ticket Date'] = pd.to_datetime(df['Ticket Date'], errors='coerce')
        df['Days_Since_Ticket'] = (df['Date'] - df['Ticket Date']).dt.days
    
    # Crear flag para registros completados
    if 'Object Type' in df.columns:
        df['Is_Completed'] = (df['Object Type'] == 'COMPLETED').astype(int)
    
    # Crear identificador único de incidente
    if 'Plant' in df.columns and 'Ticket' in df.columns:
        df['Incident_ID'] = df['Plant'].astype(str) + '_' + df['Ticket'].astype(str)
    
    print(f"   ✓ Campos calculados agregados")
    
    return df


def aggregate_by_coordinator(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea resumen agregado por Billing Coordinator
    
    Args:
        df: DataFrame categorizado
        
    Returns:
        DataFrame con métricas por coordinador
    """
    print("   📊 Agregando métricas por Billing Coordinator...")
    
    # CAMBIO: usar 'BILLING COORDINATORS'
    if 'BILLING COORDINATORS' not in df.columns:
        print("   ⚠️  Columna 'BILLING COORDINATORS' no encontrada")
        return pd.DataFrame()
    
    summary = df.groupby(['BILLING COORDINATORS', 'Category']).agg({
        'ID': 'count',
        'Delivery quantity': 'sum',
        'Is_Completed': 'sum' if 'Is_Completed' in df.columns else 'count'
    }).reset_index()
    
    summary.columns = ['Billing_Coordinator', 'Category', 'Total_Incidents', 
                       'Total_Quantity', 'Completed_Count']
    
    # Calcular tasa de completitud
    summary['Completion_Rate'] = (
        summary['Completed_Count'] / summary['Total_Incidents'] * 100
    ).round(2)
    
    print(f"   ✓ Resumen creado: {len(summary)} registros")
    
    return summary


def aggregate_by_plant(df: pd.DataFrame, agent_column: str = 'Actual (last) agent') -> pd.DataFrame:
    """
    Crea resumen agregado por Agent y Plant
    Muestra los top 3 plantas con más issues por cada agente

    Estructura:
    - Biller: Agent
    - Plants: Nombre de la planta
    - Category: Categoría del incidente
    - Porcentaje: Porcentaje de incidentes en esa planta/categoria respecto al total del agente
    - N-veces: Cantidad de incidentes

    Args:
        df: DataFrame categorizado
        agent_column: Nombre de la columna a usar (por defecto 'Actual (last) agent')

    Returns:
        DataFrame con top 3 plantas por agente
    """
    print("   📊 Agregando métricas por Agent y Plant (Top 3)...")

    if agent_column not in df.columns:
        print(f"   ⚠️  Columna '{agent_column}' no encontrada")
        return pd.DataFrame()

    # Agrupar por Agent, Planta y Categoría
    summary = df.groupby([agent_column, 'Plant', 'Category']).agg({
        'ID': 'count'
    }).reset_index()

    summary.columns = ['Biller', 'Plants', 'Category', 'N-veces']

    # Calcular total de incidentes por agente para el porcentaje
    coordinator_totals = df.groupby(agent_column)['ID'].count().reset_index()
    coordinator_totals.columns = ['Biller', 'Total_Coordinator']

    summary = summary.merge(coordinator_totals, on='Biller', how='left')

    # Calcular porcentaje
    summary['Porcentaje'] = (summary['N-veces'] / summary['Total_Coordinator'] * 100).round(2)
    summary['Porcentaje'] = summary['Porcentaje'].astype(str) + '%'

    # Obtener top 3 plantas por coordinador (ordenadas por N-veces descendente)
    summary = summary.sort_values(['Biller', 'N-veces'], ascending=[True, False])
    summary = summary.groupby('Biller').head(3).reset_index(drop=True)

    # Reordenar columnas finales
    summary = summary[['Biller', 'Plants', 'Category', 'Porcentaje', 'N-veces']]

    print(f"   ✓ Resumen creado: {len(summary)} registros (top 3 plantas por coordinador)")

    return summary


def aggregate_by_issue(df: pd.DataFrame, agent_column: str = 'Actual (last) agent') -> pd.DataFrame:
    """
    Crea resumen de categorías por Agent con porcentajes

    Estructura:
    - Biller: Nombre del agente
    - Contract: Porcentaje de incidentes en categoría Contract
    - Interface: Porcentaje de incidentes en categoría Interface
    - Inventory: Porcentaje de incidentes en categoría Inventory
    - Pricing: Porcentaje de incidentes en categoría Pricing
    - STPO: Porcentaje de incidentes en categoría STPO
    - Incomplete: Porcentaje de incidentes en categoría Incomplete

    Args:
        df: DataFrame categorizado
        agent_column: Nombre de la columna a usar (por defecto 'Actual (last) agent')

    Returns:
        DataFrame con porcentajes de categorías por agente
    """
    print("   📊 Agregando métricas de categorías por Agent...")

    if agent_column not in df.columns or 'Category' not in df.columns:
        print(f"   ⚠️  Columnas '{agent_column}' o 'Category' no encontradas")
        return pd.DataFrame()

    # Agrupar por Agent y Categoría para contar incidentes
    category_count = df.groupby([agent_column, 'Category']).agg({
        'ID': 'count'
    }).reset_index()

    category_count.columns = ['Biller', 'Category', 'Count']

    # Calcular total de incidentes por agente
    biller_totals = df.groupby(agent_column)['ID'].count().reset_index()
    biller_totals.columns = ['Biller', 'Total']

    # Merge para agregar el total a cada registro
    category_count = category_count.merge(biller_totals, on='Biller', how='left')

    # Calcular porcentaje
    category_count['Percentage'] = (category_count['Count'] / category_count['Total'] * 100).round(2)
    category_count['Percentage'] = category_count['Percentage'].astype(str) + '%'

    # Crear pivot table con Billers en filas y Categorías en columnas
    pivot = category_count.pivot_table(
        index='Biller',
        columns='Category',
        values='Percentage',
        aggfunc='first'
    ).reset_index()

    # Asegurar que todas las categorías esperadas están presentes, llenar con 0% si no existen
    expected_categories = ['Contract', 'Interface', 'Inventory', 'Pricing', 'STPO', 'Incomplete']
    for category in expected_categories:
        if category not in pivot.columns:
            pivot[category] = '0%'
        else:
            # Reemplazar NaN con 0%
            pivot[category] = pivot[category].fillna('0%')

    # Calcular Total sumando los porcentajes de todas las categorías
    def calculate_total(row):
        """Suma los porcentajes de todas las categorías"""
        total = 0
        for category in expected_categories:
            # Remover el '%' y convertir a float
            percentage_value = float(row[category].rstrip('%'))
            total += percentage_value
        return f'{round(total, 2)}%'

    pivot['Total'] = pivot.apply(calculate_total, axis=1)

    # Reordenar columnas
    pivot = pivot[['Biller'] + expected_categories + ['Total']]

    print(f"   ✓ Resumen creado: {len(pivot)} billers")

    return pivot


def create_pivot_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea tabla pivote para análisis multidimensional

    Args:
        df: DataFrame categorizado

    Returns:
        DataFrame pivote
    """
    print("   📊 Creando análisis pivote...")

    if 'Month_Name' not in df.columns or 'Category' not in df.columns:
        print("   ⚠️  Campos necesarios no encontrados")
        return pd.DataFrame()

    # CAMBIO: usar 'BILLING COORDINATORS'
    pivot = pd.pivot_table(
        df,
        values='ID',
        index=['BILLING COORDINATORS', 'Plant'],
        columns='Category',
        aggfunc='count',
        fill_value=0
    )

    # Agregar total por fila
    pivot['Total'] = pivot.sum(axis=1)

    print(f"   ✓ Tabla pivote creada: {pivot.shape[0]} filas x {pivot.shape[1]} columnas")

    return pivot


def aggregate_by_inventory(df: pd.DataFrame, agent_column: str = 'Actual (last) agent') -> pd.DataFrame:
    """
    Crea resumen de inventario por región y planta, separando por Base Unit of Measure
    Filtra por los issues específicos de inventario: "COMMAND - Ticket not Goods Issued", "JWS/APEX - Ticket not Goods Issued"

    Estructura:
    - Region: Nombre de la región
    - Plant: Planta
    - Biller: Agent
    - Ton: Cantidad en TON (de registros con issue de inventario)
    - To: Cantidad en TO (de registros con issue de inventario)
    - YD3: Cantidad en YD3 (de registros con issue de inventario)
    - Ton%: Porcentaje de TON respecto al total de TON
    - To%: Porcentaje de TO respecto al total de TO
    - YD3%: Porcentaje de YD3 respecto al total de YD3

    Args:
        df: DataFrame categorizado con columnas REGION, Plant, Base Unit of Measure,
            Delivery quantity, agent_column y Task text
        agent_column: Nombre de la columna a usar (por defecto 'Actual (last) agent')

    Returns:
        DataFrame con métricas de inventario por región, planta y unidad de medida
    """
    print("   📊 Agregando métricas de inventario por Región y Planta...")

    # Verificar columnas necesarias
    required_columns = ['REGION', 'Plant', 'Base Unit of Measure', 'Delivery quantity', agent_column, 'Task text']
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"   ⚠️  Columnas faltantes: {missing_cols}")
        return pd.DataFrame()

    # Crear copia para no modificar el original
    inventory_df = df.copy()

    # Filtrar por los issues específicos de inventario
    inventory_issues = ["COMMAND - Ticket not Goods Issued", "JWS/APEX - Ticket not Goods Issued"]
    inventory_data = inventory_df[
        inventory_df['Task text'].isin(inventory_issues)
    ].copy()

    print(f"   ✓ Registros de inventario identificados: {len(inventory_data):,}")

    # Procesar datos de inventario (filtrar TON, TO, YD3)
    inventory_processed = inventory_data[
        inventory_data['Base Unit of Measure'].isin(['TON', 'TO', 'YD3'])
    ][['REGION', 'Plant', 'Base Unit of Measure', 'Delivery quantity', agent_column, 'Task text']].copy()

    print(f"   ✓ Registros procesados (TON/TO/YD3): {len(inventory_processed):,}")

    # Si no hay datos procesados, retornar DataFrame vacío
    if len(inventory_processed) == 0:
        print("   ⚠️  No se encontraron registros de inventario (TON, TO, YD3)")
        return pd.DataFrame()

    # Agrupar por REGION, Plant, Base Unit of Measure, Task text y Agent
    inventory_by_unit = inventory_processed.groupby(
        ['REGION', 'Plant', 'Base Unit of Measure', 'Task text', agent_column]
    ).agg({
        'Delivery quantity': 'sum'
    }).reset_index()

    inventory_by_unit.columns = ['Region', 'Plant', 'Unit', 'Task_text', 'Biller', 'Quantity']

    # Pivotar para obtener TON, TO, YD3 en columnas
    pivot_inventory = inventory_by_unit.pivot_table(
        index=['Region', 'Plant', 'Biller'],
        columns='Unit',
        values='Quantity',
        fill_value=0
    ).reset_index()

    # Asegurar que existan las columnas de unidades
    for unit in ['TON', 'TO', 'YD3']:
        if unit not in pivot_inventory.columns:
            pivot_inventory[unit] = 0

    # Reordenar columnas
    pivot_inventory = pivot_inventory[['Region', 'Plant', 'Biller', 'TON', 'TO', 'YD3']]

    # Calcular totales para porcentajes
    ton_total = pivot_inventory['TON'].sum()
    to_total = pivot_inventory['TO'].sum()
    yd3_total = pivot_inventory['YD3'].sum()

    # Calcular porcentajes
    pivot_inventory['Ton%'] = (pivot_inventory['TON'] / ton_total * 100).round(2) if ton_total > 0 else 0.0
    pivot_inventory['To%'] = (pivot_inventory['TO'] / to_total * 100).round(2) if to_total > 0 else 0.0
    pivot_inventory['YD3%'] = (pivot_inventory['YD3'] / yd3_total * 100).round(2) if yd3_total > 0 else 0.0

    # Renombrar columnas a minúsculas según especificación
    pivot_inventory = pivot_inventory.rename(columns={
        'TON': 'Ton',
        'TO': 'To',
        'YD3': 'YD3'
    })

    # Reordenar columnas finales según especificación
    pivot_inventory = pivot_inventory[['Region', 'Plant', 'Biller', 'Ton', 'To', 'YD3', 'Ton%', 'To%', 'YD3%']]

    print(f"   ✓ Resumen de inventario creado: {len(pivot_inventory)} registros (Region + Plant)")

    return pivot_inventory
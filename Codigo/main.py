"""
ETL Pipeline - Main Entry Point
================================
Pipeline modular para procesamiento de datos Excel de facturación

VERSIÓN 2.3 - CAMBIOS:
- Genera 1 archivo con 7 pestañas: Resumen, APEX, COMMAND, Billing Coordinators, Plants, Issues, Inventory
- Nueva pestaña "Billing Coordinators" con análisis de desempeño
- Nueva pestaña "Plants" con top 3 plantas por coordinador
- Nueva pestaña "Issues" con distribución de categorías por biller
- Nueva pestaña "Inventory" con análisis de inventario por región
- Nota: La columna de coordinadores se llama 'BILLING COORDINATORS' (mayúscula, con espacio)
  después del INNER JOIN con el archivo Billing Coordinators.xlsx
"""

from etl_modules import io_module, config, processing, transformation, output

def main():
    """Función principal que ejecuta toda la pipeline ETL"""
    
    print("="*60)
    print("INICIANDO PIPELINE ETL v2.1")
    print("="*60)
    
    # Mostrar configuración de rutas
    io_module.print_config()
    
    # 1. EXTRACT - Cargar datos
    print("\n[1/5] EXTRAYENDO DATOS...")
    
    # Cargar archivo principal (DB) - usa ruta por defecto
    db_data = io_module.load_excel_data()
    print(f"✓ Datos DB cargados: {len(db_data):,} registros")
    
    # Cargar archivo de Billing Coordinators - usa ruta por defecto
    coordinators_data = io_module.load_billing_coordinators()
    print(f"✓ Billing Coordinators cargados: {len(coordinators_data):,} registros")
    
    # 2. TRANSFORM - Limpiar datos
    print("\n[2/5] LIMPIANDO DATOS...")
    clean_data = processing.clean_data(db_data)
    print(f"✓ Registros después de limpieza: {len(clean_data):,}")
    print(f"✓ Registros eliminados (BATCHMAN): {len(db_data) - len(clean_data):,}")
    
    # 3. TRANSFORM - Enriquecer con Billing Coordinators (INNER JOIN)
    print("\n[3/5] ENRIQUECIENDO DATOS CON BILLING COORDINATORS...")
    enriched_data = processing.merge_with_billing_coordinators(clean_data, coordinators_data)
    print(f"✓ Datos enriquecidos (INNER JOIN)")
    print(f"✓ Registros finales: {len(enriched_data):,}")

    # 3.5 TRANSFORM - Filtrar por agentes específicos
    print("\n[3.5/5] FILTRANDO POR AGENTES ESPECÍFICOS...")
    agent_list = ['SRUGELES', 'CAMVELEZ', 'JUAHENA', 'JUANRUIZ', 'REGARCI1', 'SPINEDAA', 'MPEREZPA', 'CHREVANS']
    enriched_data = processing.filter_by_agents(enriched_data, agent_list)
    print(f"✓ Datos filtrados por agentes")
    print(f"✓ Registros después de filtrado de agentes: {len(enriched_data):,}")

    # 4. TRANSFORM - Categorizar incidentes
    print("\n[4/5] CATEGORIZANDO INCIDENTES...")
    categorized_data = transformation.categorize_incidents(enriched_data)
    
    # Mostrar estadísticas por categoría
    print("\n📊 DISTRIBUCIÓN POR CATEGORÍA:")
    for category, count in categorized_data['Category'].value_counts().items():
        print(f"   {category}: {count:,} registros")
    
    # Contar APEX y COMMAND
    apex_count = categorized_data['Task text'].astype(str).str.contains('APEX', case=False, na=False).sum()
    command_count = categorized_data['Task text'].astype(str).str.contains('COMMAND', case=False, na=False).sum()
    print(f"\n📊 DISTRIBUCIÓN APEX/COMMAND:")
    print(f"   APEX: {apex_count:,} registros")
    print(f"   COMMAND: {command_count:,} registros")
    
    # 5. LOAD - Exportar resultados (1 archivo con 4 pestañas)
    print("\n[5/5] EXPORTANDO RESULTADOS...")
    output_files = output.export_results(categorized_data)
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETADA EXITOSAMENTE")
    print("="*60)
    print("\n📁 Archivo generado:")
    for file in output_files:
        print(f"   • {file}")
    print("\n   Pestañas incluidas:")
    print("      1. Resumen - Todos los datos (con BILLING COORDINATORS)")
    print("      2. APEX - Incidentes APEX")
    print("      3. COMMAND - Incidentes COMMAND")
    print("      4. Billing Coordinators - Desempeño de coordinadores")
    print("      5. Plants - Agregado por planta con métricas")
    print("      6. Issues - Agregado por issue con métricas")
    print("      7. Inventory - Inventario por región")
    
    return categorized_data

if __name__ == "__main__":
    result = main()
"""
Output Module
=============
Módulo para exportar resultados en formato simplificado

VERSIÓN 2.2 - Genera 1 archivo Excel con 6 pestañas:
  1. Resumen - Todos los datos
  2. APEX - Incidentes APEX
  3. COMMAND - Incidentes COMMAND
  4. Billing Coordinators - Desempeño de coordinadores
  5. Plants - Top 3 plantas por coordinador
  6. Issues - Distribución de categorías por biller

Sin visualizaciones (para Looker Studio)

Nota: Usa la columna 'BILLING COORDINATORS' (mayúscula, con espacio) del INNER JOIN
"""

import pandas as pd
import os
from datetime import datetime
from typing import List
from . import config
from . import transformation

class OutputManager:
    """Gestor de exportación de resultados"""

    def __init__(self, output_dir: str = config.OUTPUT_DIR):
        self.output_dir = output_dir
        # Obtener el nombre del mes actual
        current_month = datetime.now().strftime("%B")  # Nombre completo del mes en inglés
        self.month_name = current_month
        self.created_files = []

        # Crear directorio de salida si no existe
        os.makedirs(output_dir, exist_ok=True)

    def _get_filename(self, base_name: str, extension: str = 'xlsx') -> str:
        """Genera nombre de archivo con el mes actual"""
        filename = f"{base_name}_{self.month_name}.{extension}"
        return os.path.join(self.output_dir, filename)
    
    def export_final_report(self, df: pd.DataFrame) -> str:
        """
        Exporta el reporte final con 6 pestañas:
        1. Resumen: Todos los datos después del merge (ya tiene BILLING COORDINATORS)
        2. APEX: Solo incidentes con "APEX" en columna Task text
        3. COMMAND: Solo incidentes con "COMMAND" en columna Task text
        4. Billing Coordinators: Desempeño de cada coordinador (calculado desde Resumen)
        5. Plants: Agregado por planta con métricas
        6. Issue: Agregado por issue con métricas

        IMPORTANTE: Usa la columna 'BILLING COORDINATORS' (mayúscula, con espacio)

        Args:
            df: DataFrame procesado, categorizado y enriquecido (con BILLING COORDINATORS)

        Returns:
            Ruta del archivo generado
        """
        filename = self._get_filename("Performance")
        
        print("   📊 Creando reporte final con 6 pestañas...")
        
        # Pestaña 1: Resumen (todos los datos con BILLING COORDINATORS del INNER JOIN)
        resumen_df = df.copy()

        # Eliminar filas sin valor en la columna Plant
        if 'Plant' in resumen_df.columns:
            rows_before = len(resumen_df)
            resumen_df = resumen_df.dropna(subset=['Plant'])
            rows_removed = rows_before - len(resumen_df)
            if rows_removed > 0:
                print(f"      • Eliminadas {rows_removed:,} filas sin Plant en Resumen")
        
        # Pestaña 2: APEX (filtrar Task text que contenga "APEX")
        if 'Task text' in resumen_df.columns:
            apex_df = resumen_df[resumen_df['Task text'].astype(str).str.contains('APEX', case=False, na=False)].copy()
            print(f"      • APEX: {len(apex_df):,} registros")
        else:
            apex_df = pd.DataFrame()
            print("      ⚠️  Columna 'Task text' no encontrada para filtro APEX")

        # Pestaña 3: COMMAND (filtrar Task text que contenga "COMMAND")
        if 'Task text' in resumen_df.columns:
            command_df = resumen_df[resumen_df['Task text'].astype(str).str.contains('COMMAND', case=False, na=False)].copy()
            print(f"      • COMMAND: {len(command_df):,} registros")
        else:
            command_df = pd.DataFrame()
            print("      ⚠️  Columna 'Task text' no encontrada para filtro COMMAND")
        
        # Pestaña 4: Billing Coordinators Performance
        # Usa el DataFrame del Resumen que YA tiene la columna BILLING COORDINATORS del INNER JOIN
        print("      • Calculando desempeño de Billing Coordinators desde Resumen...")
        billing_coord_df = transformation.calculate_billing_coordinator_performance(resumen_df)
        if not billing_coord_df.empty:
            print(f"      • Billing Coordinators: {len(billing_coord_df):,} coordinadores evaluados")
        else:
            print("      ⚠️  No se pudo calcular desempeño de coordinadores")

        # Pestaña 5: Plants
        print("      • Agregando datos por Plant...")
        plants_df = transformation.aggregate_by_plant(resumen_df)
        if not plants_df.empty:
            print(f"      • Plants: {len(plants_df):,} registros")
        else:
            print("      ⚠️  No se pudo agregar datos por plant")

        # Pestaña 6: Issues
        print("      • Agregando datos por Issue...")
        issues_df = transformation.aggregate_by_issue(resumen_df)
        if not issues_df.empty:
            print(f"      • Issues: {len(issues_df):,} registros")
        else:
            print("      ⚠️  No se pudo agregar datos por issue")

        # Crear archivo Excel con 6 pestañas
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Pestaña 1: Resumen
            resumen_df.to_excel(writer, sheet_name='Resumen', index=False)
            print(f"      • Resumen: {len(resumen_df):,} registros")
            
            # Pestaña 2: APEX
            if not apex_df.empty:
                apex_df.to_excel(writer, sheet_name='APEX', index=False)
            else:
                # Crear hoja vacía con mensaje
                pd.DataFrame({'Mensaje': ['No se encontraron registros APEX']}).to_excel(
                    writer, sheet_name='APEX', index=False
                )
            
            # Pestaña 3: COMMAND
            if not command_df.empty:
                command_df.to_excel(writer, sheet_name='COMMAND', index=False)
            else:
                # Crear hoja vacía con mensaje
                pd.DataFrame({'Mensaje': ['No se encontraron registros COMMAND']}).to_excel(
                    writer, sheet_name='COMMAND', index=False
                )
            
            # Pestaña 4: Billing Coordinators
            if not billing_coord_df.empty:
                billing_coord_df.to_excel(writer, sheet_name='Billing Coordinators', index=False)
            else:
                # Crear hoja vacía con mensaje
                pd.DataFrame({'Mensaje': ['No se pudo calcular desempeño de coordinadores']}).to_excel(
                    writer, sheet_name='Billing Coordinators', index=False
                )

            # Pestaña 5: Plants
            if not plants_df.empty:
                plants_df.to_excel(writer, sheet_name='Plants', index=False)
            else:
                # Crear hoja vacía con mensaje
                pd.DataFrame({'Mensaje': ['No se encontraron datos por plant']}).to_excel(
                    writer, sheet_name='Plants', index=False
                )

            # Pestaña 6: Issues
            if not issues_df.empty:
                issues_df.to_excel(writer, sheet_name='Issues', index=False)
            else:
                # Crear hoja vacía con mensaje
                pd.DataFrame({'Mensaje': ['No se encontraron datos por issue']}).to_excel(
                    writer, sheet_name='Issues', index=False
                )

        self.created_files.append(filename)
        print(f"   ✓ Archivo guardado: {filename}")
        
        return filename
    
    def get_created_files(self) -> List[str]:
        """Retorna lista de archivos creados"""
        return self.created_files


def export_results(df: pd.DataFrame) -> List[str]:
    """
    Función principal para exportar resultados
    Genera UN SOLO archivo con 6 pestañas:
    1. Resumen
    2. APEX
    3. COMMAND
    4. Billing Coordinators
    5. Plants
    6. Issues

    IMPORTANTE: El DataFrame debe venir DESPUÉS del INNER JOIN (debe tener BILLING COORDINATORS)
    Usa la columna 'BILLING COORDINATORS' (mayúscula, con espacio)

    Args:
        df: DataFrame procesado, categorizado y enriquecido (con BILLING COORDINATORS del merge)

    Returns:
        Lista con el archivo generado
    """
    manager = OutputManager()
    
    print("\n📄 Exportando reporte final...")
    manager.export_final_report(df)
    
    return manager.get_created_files()
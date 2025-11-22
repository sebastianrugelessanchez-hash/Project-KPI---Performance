[README.md](https://github.com/user-attachments/files/23594978/README.md)
# KPI ETL Pipeline - Documentación del Proyecto

## Descripción General

Pipeline modular **ETL (Extract → Transform → Load)** para procesamiento de datos de **facturación y coordinadores de billing**. Sistema robusto que extrae información de archivos Excel, la limpia, enriquece y genera reportes analíticos en 7 pestañas diferentes.

**Versión:** 2.3
**Última actualización:** Noviembre 2025

---

## Objetivo del Proyecto

Procesar datos de incidentes de facturación (APEX y COMMAND) para:
- Identificar y categorizar incidentes por tipo (Contract, Pricing, Interface, etc.)
- Analizar desempeño de coordinadores de billing
- Generar métricas por plantas y regiones
- Crear reportes de inventario por unidad de medida

---

## Arquitectura del Sistema

```
main.py (Orquestador)
  ↓
[1] EXTRACT: io_module.py
  ├─ load_excel_data() → Datos principales (DB)
  └─ load_billing_coordinators() → Datos de coordinadores
  ↓
[2] TRANSFORM: processing.py + transformation.py
  ├─ clean_data() → Eliminar registros BATCHMAN
  ├─ merge_with_billing_coordinators() → INNER JOIN por Plant
  ├─ filter_by_agents() → Mantener solo 8 agentes específicos
  └─ categorize_incidents() → Clasificar por tipo
  ↓
[3] LOAD: output.py
  └─ export_results() → Generar Excel con 7 pestañas
```

---

## Pipeline de Ejecución

### Paso 1: EXTRACT (Extracción)
Carga datos de dos fuentes:

1. **Archivo Principal (DB)**: `Base de datos/Billing Coordinators.xlsx`
   - Contiene registros de incidentes con detalles (Task text, Plant, Agent, etc.)
   - ~36,718 registros iniciales

2. **Archivo de Coordinadores**: `Base de datos/Billing Coordinators.xlsx` (hoja separada)
   - Mapeo de Plants a Billing Coordinators
   - Datos de región, mercado, stronghold

### Paso 2: TRANSFORM (Transformación)

#### 2.1 Limpieza (clean_data)
```python
❌ Elimina registros donde "Work item text" contiene:
   "is currently being processed" (BATCHMAN)
```

#### 2.2 Enriquecimiento (merge_with_billing_coordinators)
```python
INNER JOIN:
  DB.Plant == Parametros.Plant

Resultado:
  ✓ Agrega columna "BILLING COORDINATORS"
  ✓ Agrega columnas de región (REGION, MARKET_NAME, STRONGHOLD)
```

#### 2.3 Filtrado por Agentes (filter_by_agents)
```python
Mantiene SOLO estos 8 agentes:
  ['SRUGELES', 'CAMVELEZ', 'JUAHENA', 'JUANRUIZ',
   'REGARCI1', 'SPINEDAA', 'MPEREZPA', 'CHREVANS']

Ejemplo: 36,718 registros → 23,984 registros (65.3%)
```

#### 2.4 Categorización (categorize_incidents)
Mapea "Task text" a categorías:

| Categoría | Ejemplos de Task Text |
|-----------|----------------------|
| **Contract** | Error Shipto related to Contract, JWS/APEX - Assign Contract |
| **Pricing** | COMMAND - Pricing Incomplete, JWS/APEX - Shipment cost not transferred |
| **Interface** | JWS/APEX - Interface Errors, COMMAND - Interface Errors |
| **Incomplete** | JWS/APEX - Incomplete Deliveries, COMMAND - Incomplete Orders |
| **Inventory** | COMMAND - Ticket not Goods Issued, JWS/APEX - Ticket not Goods Issued |
| **STPO** | JWS/APEX - STPO Errors |
| **Other** | Incidentes sin categoría definida |

### Paso 3: LOAD (Carga)
Genera **1 archivo Excel** con **7 pestañas**:

---

## Descripción de Pestañas (Output)

### 1. **Resumen**
Todos los datos procesados con todas las columnas originales + "BILLING COORDINATORS"

```
Columnas incluidas:
  • Task text, Plant, Sales Office, Sales Group, etc.
  • BILLING COORDINATORS (del INNER JOIN)
  • Actual (last) agent (agente actual)
  • Category (categoría asignada)
  • ... todas las columnas del dataset original
```

### 2. **APEX**
Incidentes que contienen "APEX" en columna `Task text`

```
Estructura: Igual a "Resumen" pero filtrado por APEX
Recordar: Los 8 agentes ya han sido aplicados en Paso 2.3
```

### 3. **COMMAND**
Incidentes que contienen "COMMAND" en columna `Task text`

```
Estructura: Igual a "Resumen" pero filtrado por COMMAND
```

### 4. **Billing Coordinators** ⭐
Desempeño agregado por **Actual (last) agent**

```
Columnas:
  • Billing_Coordinator: Nombre del agente
  • Average_Days_Spent: Promedio de días (OK-End Date - Date)
  • Unique_Tickets_Processed: Conteo de tickets únicos
  • Plant_Count: Cantidad de plantas diferentes
  • Main_Category: Categoría más frecuente (excluyendo Inventory)
  • Issue: Issue más común para esa categoría
  • Occurrences: Veces que aparece ese issue
  • Category_Count: Cantidad de registros en categoría principal
  • Category_Percentage: Porcentaje respecto a total del agente

Ejemplo fila:
  CAMVELEZ | 3.45 días | 1,200 tickets | 12 plantas | Contract | Assign Contract | 450 | 450 | 45.2%
```

### 5. **Plants** ⭐
Top 3 plantas con más incidentes por **Actual (last) agent**

```
Columnas:
  • Biller: Agente (Actual last agent)
  • Plants: Nombre de la planta
  • Category: Categoría del incidente
  • Porcentaje: % respecto al total del agente
  • N-veces: Cantidad de incidentes

Lógica:
  1. Agrupar por (Agent, Plant, Category)
  2. Contar incidentes
  3. Calcular porcentaje: (incidentes_plant_category / total_agent) * 100
  4. Top 3 plantas por agente
```

### 6. **Issues** ⭐
Distribución de categorías por **Actual (last) agent** con porcentajes

```
Columnas:
  • Biller: Agente
  • Contract: % incidentes categoría Contract
  • Interface: % incidentes categoría Interface
  • Inventory: % incidentes categoría Inventory
  • Pricing: % incidentes categoría Pricing
  • STPO: % incidentes categoría STPO
  • Incomplete: % incidentes categoría Incomplete
  • Total: Suma de todos los porcentajes (~100%)

Lógica:
  1. Agrupar por (Agent, Category)
  2. Contar incidentes: Count
  3. Total por agente
  4. Calcular porcentaje: (Count / Total_Agent) * 100
  5. Pivotar: cada categoría es columna
  6. Total = suma de todos los %

Nota: Total puede ser 99.99% o 100.01% debido a redondeo (aceptable)
```

### 7. **Inventory** ⭐
Análisis de inventario por región, planta y agente

```
Columnas:
  • Region: Región geográfica
  • Plant: Planta
  • Biller: Agente (Actual last agent)
  • Ton: Cantidad en TON (de APEX con Base Unit='TON')
  • To: Cantidad en TO (de APEX con Base Unit='TO')
  • YD3: Cantidad en YD3 (de COMMAND con Base Unit='YD3')
  • Ton%: % de TON respecto al total de TON
  • To%: % de TO respecto al total de TO
  • YD3%: % de YD3 respecto al total de YD3

Lógica:
  1. Filtrar APEX: Task text contiene 'APEX'
     - Tomar solo registros con Base Unit = 'TON' o 'TO'
  2. Filtrar COMMAND: Task text contiene 'COMMAND'
     - Tomar solo registros con Base Unit = 'YD3'
  3. Agrupar por (Region, Plant, Agent, Unit)
  4. Sumar Delivery quantity
  5. Pivotar por Unit (TON, TO, YD3)
  6. Calcular porcentajes globales por unidad
```

---

## Estructura de Archivos

```
Proyecto KPI/
├── Codigo/
│   ├── main.py                    # Orquestador principal
│   ├── ejecutar.py                # Script de ejecución
│   ├── README.md                  # Esta documentación
│   │
│   └── etl_modules/
│       ├── __init__.py
│       ├── config.py              # Configuración y constantes
│       ├── io_module.py           # Lectura/escritura Excel
│       ├── processing.py          # Limpieza y enriquecimiento
│       ├── transformation.py      # Transformaciones complejas
│       └── output.py              # Exportación de resultados
│
├── Base de datos/
│   └── Billing Coordinators.xlsx  # Datos principales + coordinadores
│
└── output/
    └── Performance_[Month].xlsx   # Archivo Excel generado
```

---

## Cómo Ejecutar

### Opción 1: Ejecutar directamente
```bash
cd "c:\Users\Sebas\OneDrive\Desktop\Proyecto KPI\Codigo"
python ejecutar.py
```

### Opción 2: Importar en Python
```python
from main import main
result = main()
```

### Output esperado
```
============================================================
INICIANDO PIPELINE ETL v2.1
============================================================

[1/5] EXTRAYENDO DATOS...
✓ Datos DB cargados: 36,718 registros
✓ Billing Coordinators cargados: 38 registros

[2/5] LIMPIANDO DATOS...
✓ Registros después de limpieza: 36,718
✓ Registros eliminados (BATCHMAN): 0

[3/5] ENRIQUECIENDO DATOS CON BILLING COORDINATORS...
✓ Datos enriquecidos (INNER JOIN)
✓ Registros finales: 36,718

[3.5/5] FILTRANDO POR AGENTES ESPECÍFICOS...
✓ Datos filtrados por agentes
✓ Registros después de filtrado de agentes: 23,984

[4/5] CATEGORIZANDO INCIDENTES...
📊 DISTRIBUCIÓN POR CATEGORÍA:
   Contract: 8,234 registros
   Interface: 4,521 registros
   ...

[5/5] EXPORTANDO RESULTADOS...

============================================================
PIPELINE COMPLETADA EXITOSAMENTE
============================================================

📁 Archivo generado:
   • Performance_November.xlsx

   Pestañas incluidas:
      1. Resumen - Todos los datos
      2. APEX - Incidentes APEX
      3. COMMAND - Incidentes COMMAND
      4. Billing Coordinators - Desempeño
      5. Plants - Top 3 plantas
      6. Issues - Distribución de categorías
      7. Inventory - Inventario por región
```

---

## Módulos Principales

### `main.py`
**Orquestador central** que ejecuta toda la pipeline en orden

```python
main()
├── io_module.load_excel_data()
├── io_module.load_billing_coordinators()
├── processing.clean_data()
├── processing.merge_with_billing_coordinators()
├── processing.filter_by_agents()
├── transformation.categorize_incidents()
└── output.export_results()
```

### `config.py`
Constantes y configuración

```python
# Categorización de incidentes
INCIDENT_CATEGORIES = {
    'Contract': [...],
    'Pricing': [...],
    'Interface': [...],
    'Incomplete': [...],
    'Inventory': [...],
    'STPO': [...]
}

# Directorio de salida
OUTPUT_DIR = r"C:\Users\Sebas\OneDrive\Desktop\Proyecto KPI\output"

# Tamaño de chunks para procesamiento
CHUNK_SIZE = 10000
```

### `io_module.py`
Lectura y escritura de datos

```python
load_excel_data()                      # Carga datos principales
load_billing_coordinators()            # Carga coordinadores
normalize_text()                       # Normaliza columnas de texto
```

### `processing.py`
Limpieza, enriquecimiento y filtrado

```python
clean_data()                           # Elimina BATCHMAN
merge_with_billing_coordinators()      # INNER JOIN por Plant
filter_by_agents()                     # Filtra por 8 agentes específicos
```

### `transformation.py`
Transformaciones complejas y agregaciones

```python
categorize_incidents()                 # Mapea Task text → Categoría
calculate_billing_coordinator_performance()    # Métricas por agente
aggregate_by_plant()                   # Top 3 plantas por agente
aggregate_by_issue()                   # Distribución por categoría
aggregate_by_inventory()               # Análisis de inventario
```

### `output.py`
Exportación a Excel

```python
export_results()                       # Genera archivo con 7 pestañas
```

### `ejecutar.py`
Script ejecutable

```python
# Configura UTF-8 para emojis en Windows
sys.stdout.reconfigure(encoding='utf-8')

# Ejecuta la pipeline
from main import main
main()
```

---

## Variables Clave

### Agentes Filtrados
```python
agent_list = [
    'SRUGELES',      # Sergio Rugeles
    'CAMVELEZ',      # Carmen Vélez
    'JUAHENA',       # Juan Ahena
    'JUANRUIZ',      # Juan Ruiz
    'REGARCI1',      # Régulo García
    'SPINEDAA',      # Amparo Spineda
    'MPEREZPA',      # Mabel Pérez
    'CHREVANS'       # Christopher Evans
]
```

### Columnas de Agrupación por Pestaña

| Pestaña | Columna Utilizada | Razón |
|---------|-------------------|-------|
| Resumen | - (todas) | Muestra todos los datos |
| APEX | - (filtrado) | Solo Task text con APEX |
| COMMAND | - (filtrado) | Solo Task text con COMMAND |
| **Billing Coordinators** | `Actual (last) agent` | Agente responsable del trabajo |
| **Plants** | `Actual (last) agent` | Agente por planta |
| **Issues** | `Actual (last) agent` | Distribución de agente |
| **Inventory** | `Actual (last) agent` | Inventario por agente |

---

## Columnas del Dataset

### Columnas Originales (DB)
| Posición | Nombre | Descripción |
|----------|--------|-------------|
| A | Task text | Descripción del incidente |
| B | Sales Office | Oficina de ventas |
| C | Sales Group | Grupo de ventas |
| D | Sales District | Distrito de ventas |
| E | Plant | Planta (usado para INNER JOIN) |
| F | Sold-to Party | Cliente |
| G | Name_1 | Nombre cliente |
| H | Ship-to Party | Destino |
| I | Ticket | Número de ticket |
| J | IDOC SD Document | Documento IDOC |
| K | Work item text | Descripción de trabajo (filtro BATCHMAN) |
| L | ID | Identificador único |
| M | Product Code | Código de producto |
| N | Command Order No | Orden COMMAND |
| O | Truck Type | Tipo de camión |
| P | Date | Fecha del incidente |
| Q | Delivery Quantity | Cantidad entregada |
| R | Base Unit of Measure | Unidad (TON, TO, YD3) |
| S | Ticket Date | Fecha del ticket |
| T | **Actual (last) agent** | Agente responsable (usado para agregaciones) |
| U | Object Type | Tipo de objeto |
| V | OK - Actual End Date of Work Item | Fecha finalización |
| W | Stronghold | Fortaleza/Región |

### Columnas Agregadas (Post-INNER JOIN)
| Nombre | Fuente | Descripción |
|--------|--------|-------------|
| BILLING COORDINATORS | Merge | Coordinador de billing por planta |
| REGION | Merge | Región (US/CA) |
| MARKET_NAME | Merge | Nombre de mercado |
| Category | Transformation | Categoría asignada |

---

## Métricas y Cálculos

### Average_Days_Spent (Pestaña Billing Coordinators)
```
Fórmula: PROMEDIO("OK - Actual End Date of Work Item" - "Date")

Lógica:
  1. Para cada agente, calcular: (Fecha fin - Fecha inicio) en días
  2. Reemplazar valores negativos por 0
  3. Calcular promedio por agente
```

### Porcentajes (Pestaña Issues)
```
Fórmula: (Incidentes_Categoría / Total_Agente) * 100

Ejemplo:
  Agent: CAMVELEZ
  Total: 10,603 registros
  Contract: 4,521 registros → (4,521 / 10,603) * 100 = 42.65%

  Total de porcentajes ≈ 100% (pueden ser 99.99% o 100.01% por redondeo)
```

### Porcentajes Inventario
```
Fórmula por unidad: (Cantidad_Unidad / Total_Global_Unidad) * 100

Ejemplo TON:
  Global TON total: 50,000
  CAMVELEZ TON: 12,345
  Ton%: (12,345 / 50,000) * 100 = 24.69%
```

---

## Changelog

### v2.3 (Actual)
- ✅ Cambio de columna agregación: "BILLING COORDINATORS" → "Actual (last) agent" para pestañas 4-7
- ✅ Cambio de categoría: "Contrato" → "Contract" en todo el código
- ✅ Función `filter_by_agents()` para filtrar 8 agentes específicos
- ✅ Pestaña 7 "Inventory" con análisis por región

### v2.2
- Pestañas de Plants (top 3 por agente)
- Pestañas de Issues (distribución de categorías)

### v2.1
- INNER JOIN con Billing Coordinators
- Pestaña separada de Billing Coordinators

### v2.0
- Archivo separado para Billing Coordinators

---

## Notas Importantes

### ⚠️ BATCHMAN
Registros donde "Work item text" contiene `"is currently being processed"` son eliminados en la limpieza.

### ⚠️ INNER JOIN
Solo se mantienen registros donde el Plant existe en AMBOS archivos:
```
DB.Plant == Parametros.Plant
```

### ⚠️ Redondeo de Porcentajes
Los porcentajes en "Issues" pueden sumar 99.99% o 100.01% por redondeo. Esto es aceptable.

### ⚠️ Encoding
El archivo `ejecutar.py` configura UTF-8 para soportar emojis en Windows.

---

## Requerimientos

```
pandas >= 1.3.0
numpy >= 1.21.0
openpyxl >= 3.6.0  (para Excel)
```

---

## Contacto y Soporte

Para reportar problemas o sugerencias:
- Revisar logs de ejecución
- Verificar archivos de entrada en `Base de datos/`
- Asegurar que no hay archivos abiertos durante ejecución

---

**Documentación creada:** Noviembre 2025
**Versión del código:** 2.3
**Estado:** Activo y en mantenimiento

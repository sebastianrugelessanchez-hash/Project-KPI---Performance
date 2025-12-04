# Pipeline ETL - Diagrama de Flujo

## Flujo General Completo

```
┌─────────────────────────────────────────────────────────────────┐
│  ejecutar.py - Punto de Entrada Principal                       │
│  Verifica: archivos, dependencias, directorios                  │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  main.py - Orquestador de la Pipeline                           │
└────────────────┬────────────────────────────────────────────────┘
                 │
     ┌───────────┴───────────┐
     │                       │
     ▼                       ▼
┌─────────────────────┐  ┌──────────────────────┐
│  [1/5] EXTRACT      │  │  io_module.py        │
│  Cargar datos       │  │  ─────────────────   │
└─────────────────────┘  │  • load_excel_data() │
     │                   │  • load_billing_     │
     │                   │    coordinators()    │
     │                   │  • print_config()    │
     └───────────────────┘
                 │
     ┌───────────┴─────────────────────┐
     │                                 │
     ▼                                 ▼
 ┌────────────────┐          ┌──────────────────────┐
 │  DB Excel      │          │ Coordinators Excel   │
 │  ──────────    │          │ ──────────────────   │
 │  800k+ rows    │          │  Plants, Billing     │
 │  23 columns    │          │  Coordinators        │
 └────────────────┘          └──────────────────────┘
     │                                 │
     └───────────────────┬─────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │  [2/5] TRANSFORM - Limpieza        │
        │  processing.clean_data()           │
        │  ─────────────────────────────────┐│
        │  ChunkProcessor.process_in_chunks()││
        │  filter_batchman_vectorized()     ││
        │                                    ││
        │  Elimina: BATCHMAN records         ││
        │  Procesa: 10,000 filas por chunk  ││
        └────────────────────────────────────┘
                         │
                         ▼ (Datos limpios)
        ┌────────────────────────────────────┐
        │  [3/5] TRANSFORM - Enriquecimiento │
        │  processing.merge_with_billing_    │
        │  coordinators()                    │
        │                                    │
        │  INNER JOIN por Plant              │
        │  Agrega: BILLING COORDINATORS      │
        └────────────────────────────────────┘
                         │
                         ▼ (Datos enriquecidos)
        ┌────────────────────────────────────┐
        │  [3.5/5] Filtrado por Agentes      │
        │  processing.filter_by_agents()     │
        │                                    │
        │  Mantiene solo: 8 agentes          │
        │  SRUGELES, CAMVELEZ, JUAHENA, etc. │
        └────────────────────────────────────┘
                         │
                         ▼ (Datos filtrados)
        ┌────────────────────────────────────┐
        │  [4/5] TRANSFORM - Categorización  │
        │  transformation.categorize_        │
        │  incidents()                       │
        │                                    │
        │  Categorías:                       │
        │  • Contract                        │
        │  • Pricing                         │
        │  • Interface                       │
        │  • Incomplete                      │
        │  • STPO                            │
        │  • Inventory                       │
        │  • Other                           │
        └────────────────────────────────────┘
                         │
                         ▼ (Datos categorizados)
        ┌────────────────────────────────────┐
        │  [5/5] LOAD - Exportación          │
        │  output.export_results()           │
        │  ─────────────────────────────────│
        │  Crea Excel con 7 pestañas:        │
        └────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────────────────┐
        │                │                            │
        ▼                ▼                            ▼
    ┌────────┐    ┌────────────┐           ┌──────────────────┐
    │Resumen │    │ APEX/CMD   │           │Análisis Avanzados│
    └────────┘    └────────────┘           └──────────────────┘
        │                │                     │     │      │
        │                │                     │     │      │
        ▼                ▼                     ▼     ▼      ▼
    Todos      APEX y    Billing Plants  Issues Inventory
    los datos  COMMAND   Coord   (Top 3) (Dist) (Regional)
              por sep.  Perf.

```

---

## Detalles de Cada Módulo

### 1. **io_module.py** - Entrada/Salida de Datos
```
Funciones principales:
├── load_excel_data()
│   └── Lee: 2025-08 DB WF.xlsb (hoja DB)
│       Retorna: DataFrame con ~800k registros
│
├── load_billing_coordinators()
│   └── Lee: Billing Coordinators.xlsx
│       Retorna: DataFrame con mapeo Plant → Coordinador
│
└── print_config()
    └── Muestra rutas configuradas
```

---

### 2. **config.py** - Configuración Global
```
Define:
├── CHUNK_SIZE = 10,000 filas/chunk
├── INCIDENT_CATEGORIES = Mapeo de categorías
├── TASK_TO_CATEGORY = Búsqueda inversa rápida
├── DB_COLUMNS = Índices de columnas
└── OUTPUT_DIR = Ruta de salida
```

---

### 3. **processing.py** - Procesamiento y Limpieza
```
Flujo:
┌─ ChunkProcessor
│  ├── __init__(chunk_size=10000)
│  ├── process_in_chunks(df, func)
│  │   └── Divide en chunks
│  │       Procesa cada chunk
│  │       Concatena resultados
│  │
│  └── _chunk_generator(df)
│      └── Generador de chunks
│
├── clean_data(df)
│  └── Usa ChunkProcessor
│      Aplica: filter_batchman_vectorized()
│      Elimina: "is currently being processed"
│
├── merge_with_billing_coordinators(db_df, coord_df)
│  └── INNER JOIN por Plant
│      Filtra registros sin coordinador
│
└── filter_by_agents(df, agent_list)
   └── Mantiene solo 8 agentes específicos
```

---

### 4. **transformation.py** - Transformaciones y Análisis
```
Flujo:
├── categorize_incidents(df)
│   └── Mapea Task text → Categoría
│       Rellena no categorizados con "Other"
│
├── calculate_billing_coordinator_performance(df)
│   └── Agrupa por: Billing Coordinator
│       Calcula:
│       • Average_Days_Spent
│       • Tickets_Processed
│       • Main_Category (excluyendo Inventory)
│       • Issue (moda de Work item text)
│
├── aggregate_by_plant(df)
│   └── Agrupa por: Agent, Plant, Category
│       Calcula: Porcentajes respecto al total del agent
│       Filtra: TOP 3 plantas por agente
│
├── aggregate_by_issue(df)
│   └── Agrupa por: Agent, Category
│       Pivot table: Categorías en columnas
│       Calcula: Porcentajes y totales
│
└── aggregate_by_inventory(df) ⭐ ACTUALIZADO
    └── Agrupa por: Region, Plant, Biller, Unit
        Filtra: Issues de inventario específicos
        Calcula: Porcentajes POR BILLER (suma 100% por biller)
        Unidades: TON, TO, YD3
```

---

### 5. **output.py** - Exportación
```
Flujo:
OutputManager.export_final_report(df)
├── Pestaña 1: Resumen
│   └── Todos los datos del DataFrame
│
├── Pestaña 2: APEX
│   └── Filtro: Task text contiene "APEX"
│
├── Pestaña 3: COMMAND
│   └── Filtro: Task text contiene "COMMAND"
│
├── Pestaña 4: Billing Coordinators
│   └── Usa: calculate_billing_coordinator_performance()
│
├── Pestaña 5: Plants
│   └── Usa: aggregate_by_plant()
│
├── Pestaña 6: Issues
│   └── Usa: aggregate_by_issue()
│
└── Pestaña 7: Inventory
    └── Usa: aggregate_by_inventory()

Salida: Performance_{Mes}.xlsx
```

---

## Flujo de Datos Paso a Paso

```
[ENTRADA]
    ↓
[800k+ registros] → [Limpieza BATCHMAN] → [~750k registros]
    ↓
[INNER JOIN Coordinators] → [~700k registros]
    ↓
[Filtro 8 agentes] → [~200k registros]
    ↓
[Categorización] → [7 categorías]
    ↓
[EXPORTACIÓN - 7 PESTAÑAS]
    ├── Resumen: Todos (~200k)
    ├── APEX: Filtrados (~150k)
    ├── COMMAND: Filtrados (~50k)
    ├── Billing Coordinators: Agregado por coordinador (~8 filas)
    ├── Plants: Top 3 por coordinador (~24 filas)
    ├── Issues: Agregado por categoría (~8 filas)
    └── Inventory: Agregado por región/planta (~300+ filas)
```

---

## Procesamiento por Chunks

```
EJEMPLO CON 800k REGISTROS Y CHUNK_SIZE=10,000:

Iteración 1: Registros 0-10,000      ✓
Iteración 2: Registros 10,000-20,000 ✓
Iteración 3: Registros 20,000-30,000 ✓
...
Iteración 80: Registros 790,000-800,000 ✓

Cada iteración:
├── Lee chunk en memoria
├── Aplica función (ej: filter_batchman_vectorized)
├── Guarda resultado en lista
└── Repite con siguiente chunk

Al final:
└── Concatena todos los chunks → DataFrame final

Beneficios:
✓ Manejo eficiente de memoria (no carga todo a la vez)
✓ Tolerancia a fallos (si un chunk falla, otros continúan)
✓ Progreso visible (reporta cada 5 chunks)
```

---

## Estadísticas Clave del Pipeline

| Aspecto | Valor |
|---------|-------|
| Registros entrada (máx) | 800,000 |
| Registros por chunk | 10,000 |
| Número de chunks | ~80 |
| Número de columnas | 23+ |
| Número de categorías | 7 |
| Agentes a filtrar | 8 |
| Pestañas de salida | 7 |
| Unidades de inventario | 3 (TON, TO, YD3) |

---

## Cuello de Botella Potencial

```
⚠️ Punto más lento:
   aggregate_by_inventory() con cálculo de porcentajes

Razón:
   • Itera por cada fila
   • Calcula suma por biller para cada unidad
   • Múltiples búsquedas en DataFrame

Solución:
   • Pre-calcular totales por biller
   • Usar groupby().transform() para vectorización
```

---

## Columnas Clave en el Flujo

| Columna | Origen | Uso | Destino |
|---------|--------|-----|---------|
| Task text | DB | Categorización, filtros | Todas las pestañas |
| Plant | DB | INNER JOIN, agregación | Todas las pestañas |
| Actual (last) agent | DB | Filtrado, agrupación | Billing Coord, Plants, Issues |
| BILLING COORDINATORS | INNER JOIN | Análisis de coordinadores | Billing Coordinators |
| Work item text | DB | Categorización, moda | Billing Coordinators, Issues |
| Base Unit of Measure | DB | Filtrado de inventario | Inventory |
| Delivery quantity | DB | Suma de inventario | Inventory |
| REGION | Parametros | Agregación de inventario | Inventory |
| Category | Transformación | Distribución, filtros | Issues, Billing Coord |

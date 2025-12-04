# Resumen Detallado: processing.py y transformation.py

## 📋 Tabla de Contenidos
1. [processing.py](#processingpy)
2. [transformation.py](#transformationpy)
3. [Comparación](#comparación)
4. [Flujo Integrado](#flujo-integrado)

---

## processing.py

### Propósito General
**Limpieza, preparación y enriquecimiento de datos**
- Elimina registros corruptos
- Enriquece con información externa (Coordinadores)
- Filtra por criterios específicos
- Maneja grandes volúmenes eficientemente

---

### Funciones Principales

#### 1️⃣ **ChunkProcessor** (Clase)
```python
class ChunkProcessor:
    """Procesador de datos por chunks con vectorización"""
```

**Para qué sirve:**
- Procesa DataFrames grandes (800k+ filas) sin sobrecargar memoria
- Divide los datos en pedazos manejables (10,000 filas por defecto)

**Métodos:**

| Método | Entrada | Salida | Qué Hace |
|--------|---------|--------|----------|
| `__init__(chunk_size)` | Tamaño chunk | Objeto inicializado | Define tamaño de chunks (10,000 filas) |
| `process_in_chunks(df, func)` | DataFrame + función | DataFrame procesado | Divide en chunks, aplica función a cada uno, concatena |
| `_chunk_generator(df)` | DataFrame | Generador de chunks | Genera chunks secuencialmente para ahorrar memoria |

**Ejemplo de uso:**
```python
processor = ChunkProcessor(chunk_size=10000)
cleaned_df = processor.process_in_chunks(df, filter_batchman_vectorized)
```

**Ventajas:**
- ✅ Eficiente en memoria (no carga 800k filas simultáneamente)
- ✅ Progreso visible (reporta cada 5 chunks)
- ✅ Tolerancia a fallos (si falla un chunk, otros continúan)

---

#### 2️⃣ **recognize_columns(df)**
```python
def recognize_columns(df: pd.DataFrame) -> Dict[str, str]
```

**Para qué sirve:**
- Verifica que el DataFrame tenga todas las columnas esperadas
- Identifica columnas faltantes

**Entrada:**
- DataFrame de la hoja "DB" del Excel

**Salida:**
- Diccionario con mapeo de columnas encontradas
- Imprime warnings si faltan columnas

**Columnas que valida:**
```
Task text, Sales Office, Sales Group, Sales district, Plant,
Sold-to party, Name 1, Ship-to party, Ticket, IDOC/SD Document,
Work item text, ID, Product Code, Command Order No., Truck Type,
Date, Delivery quantity, Base Unit of Measure, Ticket Date,
Actual (last) agent, Object Type, OK - Actual End Date of Work Item,
Stronghold
```

---

#### 3️⃣ **filter_batchman_vectorized(chunk)** ⭐ CLAVE
```python
def filter_batchman_vectorized(chunk: pd.DataFrame) -> pd.DataFrame
```

**Para qué sirve:**
- Elimina registros que están siendo procesados por BATCHMAN
- Filtra registros "en proceso" para obtener datos limpios

**Entrada:**
- Chunk del DataFrame (10,000 filas)

**Salida:**
- Chunk sin registros que contengan "is currently being processed"

**Lógica:**
```
1. Busca en columna 'Work item text'
2. Normaliza el texto (mayúsculas, espacios múltiples, caracteres invisibles)
3. Busca la frase "is currently being processed"
4. Mantiene solo las filas que NO contienen esa frase
```

**Ejemplo:**
```
ANTES:
┌─────────────────────────────────────────┐
│ Work item text                          │
├─────────────────────────────────────────┤
│ BATCHMAN is currently being processed   │ ← ELIMINA
│ COMMAND - Ticket not Goods Issued       │ ← MANTIENE
│ is currently being processed            │ ← ELIMINA
└─────────────────────────────────────────┘

DESPUÉS:
┌─────────────────────────────────────────┐
│ Work item text                          │
├─────────────────────────────────────────┤
│ COMMAND - Ticket not Goods Issued       │
└─────────────────────────────────────────┘
```

---

#### 4️⃣ **clean_data(df)** 🧹
```python
def clean_data(df: pd.DataFrame) -> pd.DataFrame
```

**Para qué sirve:**
- Orquesta la limpieza de datos usando ChunkProcessor
- Elimina registros BATCHMAN de forma eficiente

**Entrada:**
- DataFrame bruto (~800k registros)

**Salida:**
- DataFrame limpio (sin BATCHMAN)

**Flujo interno:**
```
1. Valida que exista columna 'Work item text'
2. Reconoce columnas
3. Crea ChunkProcessor
4. Procesa en chunks con filter_batchman_vectorized
5. Reporta estadísticas:
   - Registros antes/después
   - Registros eliminados
```

**Ejemplo de salida:**
```
Registros después de limpieza: 750,000
Registros eliminados (BATCHMAN): 50,000
```

---

#### 5️⃣ **merge_with_billing_coordinators(db_df, coord_df)** 🔗 IMPORTANTE
```python
def merge_with_billing_coordinators(
    db_df: pd.DataFrame,
    coordinators_df: pd.DataFrame
) -> pd.DataFrame
```

**Para qué sirve:**
- Une datos de DB con información de Coordinadores
- Agrega la columna `BILLING COORDINATORS` (mayúscula, con espacio)
- Usa INNER JOIN (solo mantiene registros con coordinador asignado)

**Entrada:**
- `db_df`: DataFrame limpio
- `coordinators_df`: DataFrame con Plant → Billing Coordinator

**Salida:**
- DataFrame enriquecido con columna `BILLING COORDINATORS`

**Lógica:**
```
    DB Data                  Coordinators Data
┌──────────────┐           ┌──────────────────────┐
│ Plant │ Ticket│    JOIN   │ Plant │ BILLING... │
├──────────────┤           ├──────────────────────┤
│ 3956  │ 12345│ ─────────→│ 3956  │ SRUGELES   │
│ 4298  │ 12346│           │ 4298  │ SRUGELES   │
│ 9999  │ 12347│ ✗ (NO MATCH)
└──────────────┘           └──────────────────────┘
                                    ↓
                           RESULTADO (INNER JOIN):
                           ┌──────────────────┐
                           │ Plant │ BILLING  │
                           ├──────────────────┤
                           │ 3956  │ SRUGELES │
                           │ 4298  │ SRUGELES │
                           │ 9999  │  (NADA)  │ ← Eliminado
                           └──────────────────┘
```

**Estadísticas reportadas:**
- Registros antes del merge
- Registros después del merge
- Porcentaje de coincidencias
- Plantas sin coordinador

---

#### 6️⃣ **filter_by_agents(df, agent_list)** 🎯
```python
def filter_by_agents(
    df: pd.DataFrame,
    agent_list: List[str] = None
) -> pd.DataFrame
```

**Para qué sirve:**
- Filtra el DataFrame para mantener solo agentes específicos
- Mantiene solo registros de los 8 agentes principales

**Entrada:**
- `df`: DataFrame enriquecido
- `agent_list`: Lista de agentes a mantener

**Salida:**
- DataFrame con solo los agentes especificados

**Agentes por defecto:**
```python
['SRUGELES', 'CAMVELEZ', 'JUAHENA', 'JUANRUIZ',
 'REGARCI1', 'SPINEDAA', 'MPEREZPA', 'CHREVANS']
```

**Lógica:**
```
1. Valida que exista columna 'Actual (last) agent'
2. Filtra df['Actual (last) agent'].isin(agent_list)
3. Reporta:
   - Registros antes/después
   - Registros eliminados
   - Agentes encontrados con sus conteos
```

**Ejemplo:**
```
Registros antes: 700,000
Registros después: 200,000
Registros filtrados: 500,000

Agentes encontrados: 8
SRUGELES: 50,000 registros
CAMVELEZ: 45,000 registros
JUAHENA: 35,000 registros
... etc
```

---

#### 7️⃣ **validate_data_quality(df)** ✅
```python
def validate_data_quality(df: pd.DataFrame) -> Dict[str, any]
```

**Para qué sirve:**
- Valida la calidad de datos procesados
- Genera métricas de integridad

**Entrada:**
- DataFrame final

**Salida:**
- Diccionario con métricas:
  - `total_records`: Total de registros
  - `null_counts`: Nulos por columna
  - `duplicate_count`: Registros duplicados
  - `unique_plants`: Plantas únicas
  - `unique_tasks`: Tipos de tareas únicas

---

## transformation.py

### Propósito General
**Transformación, categorización y agregación de datos**
- Categoriza incidentes
- Calcula métricas por agente/planta
- Agrega datos para análisis
- Genera pivots y porcentajes

---

### Funciones Principales

#### 1️⃣ **categorize_incidents(df)** 🏷️
```python
def categorize_incidents(df: pd.DataFrame) -> pd.DataFrame
```

**Para qué sirve:**
- Mapea cada "Task text" a una categoría de incidente
- Clasifica automáticamente los tipos de problemas

**Entrada:**
- DataFrame con columna 'Task text'

**Salida:**
- DataFrame con nueva columna 'Category'

**Categorías:**
```
Contract         → Errores de contrato
Pricing          → Errores de precio
Interface        → Errores de interfaz
Incomplete       → Órdenes/entregas incompletas
STPO             → Errores STPO
Inventory        → Problemas de inventario
Other            → Otros incidentes no categorizados
```

**Lógica:**
```
1. Lee config.TASK_TO_CATEGORY (mapeo de categorías)
2. Mapea Task text → Categoría
3. Rellena no categorizados con "Other"
4. Reporta:
   - Registros categorizados
   - Registros sin categoría (Other)
```

---

#### 2️⃣ **calculate_billing_coordinator_performance(df)** 📊 COMPLEJA
```python
def calculate_billing_coordinator_performance(
    df: pd.DataFrame,
    agent_column: str = 'Actual (last) agent'
) -> pd.DataFrame
```

**Para qué sirve:**
- Calcula desempeño de cada coordinador
- Genera métricas de productividad y eficiencia

**Entrada:**
- DataFrame categorizado

**Salida:**
- DataFrame con métricas por coordinador (8 filas aprox.)

**Columnas de salida:**
| Columna | Qué Es | Cómo Se Calcula |
|---------|--------|-----------------|
| Billing_Coordinator | Nombre del agente | Agrupado |
| Average_Days_Spent | Promedio de días en resolver | (End Date - Start Date).mean() |
| Tickets_Processed | IDs únicos procesados | count(ID unique) |
| Associated_Plants | Plantas diferentes | count(Plant unique) |
| Main_Category | Categoría más frecuente (sin Inventory) | mode, excluyendo Inventory |
| Category_Count | Cuántos incidentes de esa categoría | count |
| Category_Percentage | % que representa esa categoría | (count/total) * 100 |
| Issue | Problema más frecuente | mode de Work item text |
| Occurrences | Cuántas veces aparece ese Issue | count |

**Ejemplo:**
```
SRUGELES:
├─ Average_Days_Spent: 3.5 días
├─ Tickets_Processed: 50,000
├─ Associated_Plants: 45 plantas
├─ Main_Category: Contract
├─ Category_Count: 30,000
├─ Category_Percentage: 60%
├─ Issue: Error Shipto related to Contract
└─ Occurrences: 15,000
```

---

#### 3️⃣ **aggregate_by_plant(df, agent_column)** 🏭
```python
def aggregate_by_plant(
    df: pd.DataFrame,
    agent_column: str = 'Actual (last) agent'
) -> pd.DataFrame
```

**Para qué sirve:**
- Agrupa incidentes por Agente y Planta
- Muestra Top 3 plantas por cada agente

**Entrada:**
- DataFrame categorizado

**Salida:**
- DataFrame con Top 3 plantas por agente (~24 filas)

**Columnas de salida:**
| Columna | Contenido |
|---------|-----------|
| Biller | Nombre del agente |
| Plants | Código de la planta |
| Category | Categoría de incidente |
| Porcentaje | % respecto al total del agente |
| N-veces | Cantidad de incidentes |

**Lógica:**
```
1. Agrupa por: Agent, Plant, Category
2. Cuenta incidentes
3. Calcula porcentaje respecto al total del agente
4. Ordena descendente por N-veces
5. Toma solo Top 3 plantas por agente
```

**Ejemplo:**
```
SRUGELES:
├─ Plant 3956: Contract - 15.5% (7,750 incidentes)
├─ Plant 4298: Pricing - 12.3% (6,150 incidentes)
└─ Plant 1234: Interface - 8.9% (4,450 incidentes)

CAMVELEZ:
├─ Plant 5678: Incomplete - 20% (4,000 incidentes)
├─ Plant 9101: Contract - 15% (3,000 incidentes)
└─ Plant 1121: STPO - 10% (2,000 incidentes)
```

---

#### 4️⃣ **aggregate_by_issue(df, agent_column)** 📈
```python
def aggregate_by_issue(
    df: pd.DataFrame,
    agent_column: str = 'Actual (last) agent'
) -> pd.DataFrame
```

**Para qué sirve:**
- Agrupa incidentes por categoría
- Muestra distribución de problemas por agente

**Entrada:**
- DataFrame categorizado

**Salida:**
- Pivot table (~8 filas, 1 por agente)

**Columnas de salida:**
```
Biller | Contract% | Interface% | Inventory% | Pricing% | STPO% | Incomplete% | Total%
-------|-----------|-----------|-----------|---------|-------|------------|-------
SRUGELES| 60% | 15% | 10% | 5% | 0% | 10% | 100%
CAMVELEZ| 45% | 20% | 15% | 10% | 0% | 10% | 100%
...
```

**Lógica:**
```
1. Agrupa por: Agent, Category
2. Cuenta incidentes por categoría
3. Calcula porcentaje respecto al total del agente
4. Crea pivot table (agentes en filas, categorías en columnas)
5. Calcula total (suma de todas las categorías = 100%)
```

---

#### 5️⃣ **aggregate_by_inventory(df, agent_column)** 📦 ACTUALIZADO
```python
def aggregate_by_inventory(
    df: pd.DataFrame,
    agent_column: str = 'Actual (last) agent'
) -> pd.DataFrame
```

**Para qué sirve:**
- Agrupa inventario por región, planta y unidad de medida
- Calcula porcentajes **por biller** (cada biller suma 100%)

**Entrada:**
- DataFrame categorizado

**Salida:**
- DataFrame con inventario agregado (~300+ filas)

**Filtros:**
- Solo registros con Task text = "COMMAND - Ticket not Goods Issued" o "JWS/APEX - Ticket not Goods Issued"
- Solo unidades: TON, TO, YD3

**Columnas de salida:**
| Columna | Contenido | Ejemplo |
|---------|-----------|---------|
| Region | Región | TX-LA |
| Plant | Planta | 3956 |
| Biller | Agente | SRUGELES |
| Ton | Toneladas | 136.71 |
| To | Toneladas otra unidad | 45.30 |
| YD3 | Yardas cúbicas | 250.00 |
| Ton% | % de TON por biller | 23.31% |
| To% | % de TO por biller | 18.50% |
| YD3% | % de YD3 por biller | 25.00% |

**Lógica de Porcentajes:**
```
Porcentaje = (Valor Individual / Total del Biller) × 100

SRUGELES:
├─ Total TON: 586.71
├─ Plant 3956: 136.71 TON → 23.31%
├─ Plant 4298: 250.00 TON → 42.61%
└─ Plant 5678: 200.00 TON → 34.08% (suma 100%)

CAMVELEZ:
├─ Total TON: 300.00
├─ Plant 1234: 150.00 TON → 50.00%
└─ Plant 5678: 150.00 TON → 50.00% (suma 100%)
```

---

#### 6️⃣ **create_category_subsets(df)** 📦
```python
def create_category_subsets(df: pd.DataFrame) -> Dict[str, pd.DataFrame]
```

**Para qué sirve:**
- Divide los datos por categoría en subconjuntos separados

**Entrada:**
- DataFrame categorizado

**Salida:**
- Diccionario: {categoria: dataframe}

**Ejemplo:**
```python
subsets = {
    'Contract': DataFrame con 300,000 registros,
    'Pricing': DataFrame con 250,000 registros,
    'Interface': DataFrame con 50,000 registros,
    ...
}
```

---

#### 7️⃣ **add_calculated_fields(df)** ➕
```python
def add_calculated_fields(df: pd.DataFrame) -> pd.DataFrame
```

**Para qué sirve:**
- Agrega campos calculados útiles para análisis

**Campos agregados:**
```
Year            → Año de la fecha
Month           → Mes (1-12)
Month_Name      → Nombre del mes
Week            → Número de semana ISO
Days_Since_Ticket → Días desde que se creó el ticket
Is_Completed    → 1 si Object Type = 'COMPLETED', 0 si no
Incident_ID     → ID único: Plant_Ticket
```

---

#### 8️⃣ **create_pivot_analysis(df)** 🔀
```python
def create_pivot_analysis(df: pd.DataFrame) -> pd.DataFrame
```

**Para qué sirve:**
- Crea tabla pivote multidimensional

**Entrada:**
- DataFrame categorizado

**Salida:**
- Pivot table con índices: Billing Coordinators, Plant
- Columnas: Categorías
- Valores: Conteo de incidentes

---

## Comparación

### Responsabilidades

| Aspecto | processing.py | transformation.py |
|---------|---------------|-------------------|
| **Enfoque** | Limpieza y Preparación | Transformación y Análisis |
| **Entrada** | Datos crudos | Datos limpios y enriquecidos |
| **Salida** | Datos listos para analizar | Datos agregados/categorizados |
| **Velocidad** | Rápido (usa chunks) | Más lento (cálculos complejos) |
| **Volumen** | Maneja 800k registros | Maneja 200k registros |
| **Complejidad** | Baja (filtros vectorizados) | Alta (aggregaciones) |

### Operaciones

| Operación | Módulo | Función |
|-----------|--------|---------|
| Filtrar BATCHMAN | processing | filter_batchman_vectorized |
| INNER JOIN | processing | merge_with_billing_coordinators |
| Filtrar agentes | processing | filter_by_agents |
| Categorizar | transformation | categorize_incidents |
| Agregar por categoría | transformation | aggregate_by_issue |
| Agregar por planta | transformation | aggregate_by_plant |
| Agregar por inventario | transformation | aggregate_by_inventory |
| Calcular desempeño | transformation | calculate_billing_coordinator_performance |

---

## Flujo Integrado

```
                    PROCESSING.PY
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
clean_data()    merge_with_           filter_by_
                billing_coord()        agents()
    │                    │                    │
    └────────────────────┼────────────────────┘
                         │
              (Datos limpios y filtrados)
                         │
                    TRANSFORMATION.PY
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
categorize_         aggregate_by_       aggregate_by_
incidents()         plant()             inventory()
    │                    │                    │
    └────────────────────┼────────────────────┘
                         │
                         ▼
             (Datos categorizados y agregados)
                         │
                    OUTPUT.PY
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
     Pestaña 1-3   Pestaña 4-6      Pestaña 7
     (Raw Data)   (Aggregations)  (Inventory)
```

---

## Resumen Rápido

### processing.py: "Limpiadores de datos"
- 🧹 Elimina basura (BATCHMAN)
- 🔗 Une tablas (INNER JOIN)
- 🎯 Filtra por criterios
- ⚡ Eficiente con memoria
- 📈 Maneja volúmenes grandes

### transformation.py: "Analistas de datos"
- 🏷️ Categoriza incidentes
- 📊 Calcula métricas
- 📈 Agrupa y pivota
- 💯 Genera porcentajes
- 📦 Prepara para exportar

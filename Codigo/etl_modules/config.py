"""
Config Module
=============
Configuración y constantes de la pipeline ETL
"""

# Definición de categorías de incidentes
INCIDENT_CATEGORIES = {
    'Contrato': [
        'Error Shipto related to Contract',
        'JWS/APEX - Assign Contract',
        'COMMAND - Assign Contract',
        'COMMAND - Process Error Shipto/Contract',
        'JWS/APEX - Process Error Shipto/Contract'
    ],
    'Pricing': [
        'COMMAND - Pricing Incomplete',
        'JWS/APEX - Pricing Incomplete',
        'JWS/APEX - Shipment cost not transferred',
        'COMMAND - Shipment cost not transferred',
        'No Accounting Document for Billing Doc' 
    ],
    'Interface': [
        'JWS/APEX - Interface Errors',
        'COMMAND - Interface Errors',
        'JWS/APEX - Process Valuation type error' 
    ],
    'Incomplete': [
        'JWS/APEX - Incomplete Deliveries',
        'JWS/APEX - Incomplete Orders',
        'JWS/APEX - Ticket Inco Terms',
        'COMMAND - Incomplete Orders',
        'COMMAND - Ticket Inco Terms',
        'COMMAND - Incomplete Deliveries',
    ],
    'STPO': [
        'JWS/APEX - STPO Errors'
    ],
    'Inventory': [
        'COMMAND - Ticket not Goods Issued',
        'JWS/APEX - Ticket not Goods Issued'
    ]
}

# Mapeo inverso para búsqueda rápida
TASK_TO_CATEGORY = {}
for category, tasks in INCIDENT_CATEGORIES.items():
    for task in tasks:
        TASK_TO_CATEGORY[task] = category

# Columnas de la hoja DB
DB_COLUMNS = {
    'TASK_TEXT': 0,  # Columna A
    'SALES_OFFICE': 1,
    'SALES_GROUP': 2,
    'SALES_DISTRICT': 3,
    'PLANT': 4,  # Columna E
    'SOLD_TO_PARTY': 5,
    'NAME_1': 6,
    'SHIP_TO_PARTY': 7,
    'TICKET': 8,
    'IDOC_SD_DOCUMENT': 9,
    'WORK_ITEM_TEXT': 10,  # Columna K (para filtro BATCHMAN)
    'ID': 11,
    'PRODUCT_CODE': 12,
    'COMMAND_ORDER_NO': 13,
    'TRUCK_TYPE': 14,
    'DATE': 15,
    'DELIVERY_QUANTITY': 16,
    'BASE_UNIT_OF_MEASURE': 17,
    'TICKET_DATE': 18,
    'ACTUAL_LAST_AGENT': 19,
    'OBJECT_TYPE': 20,
    'OK_ACTUAL_END_DATE': 21,
    'STRONGHOLD': 22
}

# Columnas de la hoja Parametros (US section)
PARAM_COLUMNS_US = {
    'PLANTS': 0,
    'BILLING_COORDINATORS': 1,
    'MARKET_NAME': 2,
    'REGION': 3,
    'STRONGHOLD': 4
}

# Columnas de la hoja Parametros (CA section)
PARAM_COLUMNS_CA = {
    'PLANTS': 6,
    'BILLING_COORDINATORS': 7,
    'COUNTRY': 8,
    'REGION': 9
}

# Texto a filtrar en columna K
FILTER_TEXT = "is currently being processed"

# Configuración de procesamiento por chunks
CHUNK_SIZE = 10000  # Procesar 10k registros a la vez

# Configuración de salida
OUTPUT_DIR = "output"
OUTPUT_FILENAME = "processed_data"
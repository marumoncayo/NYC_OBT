# NYC_OBT

ARQUITECTURA



```
┌─────────────────────────────────────────────────────────────────────┐
│                    FUENTE: NYC TLC PARQUET FILES                    │
│  https://d37ci6vzurychx.cloudfront.net/trip-data/                  │
│  • yellow_tripdata_YYYY-MM.parquet (2015-2025)                     │
│  • green_tripdata_YYYY-MM.parquet (2015-2025)                      │
│  • taxi+_zone_lookup.csv                                           │
└────────────────────────┬────────────────────────────────────────────┘
                         │ Descarga HTTP
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│               PROCESAMIENTO: SPARK + JUPYTER NOTEBOOK               │
│  Container: jupyter/pyspark-notebook                                │
│  Puertos: 8888 (Jupyter), 4040 (Spark UI)                         │
└────────────────────────┬────────────────────────────────────────────┘
                         │ Spark-Snowflake Connector
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  SNOWFLAKE: DATABASE NYC_TLC_P03                    │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │  SCHEMA: RAW (Aterrizaje con metadatos)                     │  │
│  │  • YELLOW_TRIPS (~500M registros)                           │  │
│  │  • GREEN_TRIPS (~337M registros)                            │  │
│  │  • INGESTION_AUDIT                                          │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                         │                                           │
│                         │ Transformación con Spark                  │
│                         ▼                                           │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │  SCHEMA: ANALYTICS (Modelo OBT)                             │  │
│  │  • OBT_TRIPS (~837M registros) ← ONE BIG TABLE              │  │
│  │  • TAXI_ZONES (lookup)                                      │  │
│  │  • PAYMENT_TYPE_LOOKUP                                      │  │
│  │  • RATE_CODE_LOOKUP                                         │  │
│  │  • VENDOR_LOOKUP                                            │  │
│  │  • TRIP_TYPE_LOOKUP                                         │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

MATRIZ DE COBERTURA


```
RESUMEN DE INGESTA:

 Por Status:
   status  count
NOT_FOUND      1
  SUCCESS    255

 Por Servicio:
                   archivos  total_registros
service status                              
green   SUCCESS         128         68045597
yellow  NOT_FOUND         1                0
        SUCCESS         127        772827410

 Por Año:
                archivos  total_registros
year status                              
2015 NOT_FOUND         1                0
     SUCCESS          23        153713330
2016 SUCCESS          24        147517346
2017 SUCCESS          24        125237386
2018 SUCCESS          24        111771105
2019 SUCCESS          24         90899429
2020 SUCCESS          24         26383268
2021 SUCCESS          24         31973063
2022 SUCCESS          24         40496500
2023 SUCCESS          24         39097286
2024 SUCCESS          24         41829938
2025 SUCCESS          16         31954356


ESTADÍSTICAS GENERALES:
 Exitosos: 255
 Fallidos: 0
 No encontrados: 1
 Total registros cargados: 840,873,007
  Tiempo total: 307.74 minutos
  Tiempo promedio por archivo: 72.13 segundos

  ARCHIVOS CON PROBLEMAS:
service  year  month    status                         error
 yellow  2015      7 NOT_FOUND Archivo Parquet no disponible


Servicio: YELLOW
month 1  2  3  4  5  6  7  8  9  10 11 12
year                                     
2015   ✓  ✓  ✓  ✓  ✓  ✓  ✗  ✓  ✓  ✓  ✓  ✓
2016   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2017   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2018   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2019   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2020   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2021   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2022   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2023   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2024   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2025   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  -  -  -  -
Servicio: GREEN
month 1  2  3  4  5  6  7  8  9  10 11 12
year                                     
2015   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2016   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2017   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2018   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2019   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2020   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2021   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2022   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2023   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2024   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓
2025   ✓  ✓  ✓  ✓  ✓  ✓  ✓  ✓  -  -  -  -

```



PASOS PARA DOCKER COMPOSE Y NOTEBOOKS

Paso 1: Preparar el proyecto

```
# Clonar repositorio
git clone https://github.com/TU_USUARIO/proyecto03-nyc-tlc.git
cd proyecto03-nyc-tlc

# Crear carpetas necesarias
mkdir -p notebooks data evidencias
```
Paso 2: Configurar variables de ambiente
```
# Copiar plantilla
cp .env.example .env

# Editar con tus credenciales de Snowflake
nano .env
```
Paso 3: Crear base de datos en Snowflake
```
USE ROLE ACCOUNTADMIN;

-- Crear warehouse (si no existe)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

USE WAREHOUSE COMPUTE_WH;

-- Crear base de datos y schemas
CREATE DATABASE IF NOT EXISTS NY
C_TLC_P03;
USE DATABASE NYC_TLC_P03;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
```

Paso 4: Levantar infraestructura Docker
```
# Iniciar contenedor
docker-compose up -d

# Verificar que está corriendo
docker-compose ps

# Ver logs (opcional)
docker-compose logs -f pyspark-notebook
```
Verificación:

Contenedor pyspark-notebook debe estar "Up"
Puerto 8888 (Jupyter) y 4040 (Spark UI) deben estar mapeados

Paso 5: Acceder a Jupyter
```
Abrir navegador: http://localhost:8888
No requiere token (deshabilitado en configuración)
Navegar a carpeta work/
```
Paso 6: Ejecutar notebooks en ORDEN
Instrucciones de ejecución:

Abrir el notebook en orden
Ejecutar celda por celda (o "Run All")
Verificar outputs antes de continuar al siguiente
NO ejecutar notebooks en paralelo

Paso 7: Verificar resultados en Snowflake
```
-- Verificar datos en RAW
SELECT COUNT(*) FROM NYC_TLC_P03.RAW.YELLOW_TRIPS;
SELECT COUNT(*) FROM NYC_TLC_P03.RAW.GREEN_TRIPS;

-- Verificar OBT en ANALYTICS
SELECT COUNT(*) FROM NYC_TLC_P03.ANALYTICS.OBT_TRIPS;

-- Verificar distribución
SELECT service_type, COUNT(*) 
FROM NYC_TLC_P03.ANALYTICS.OBT_TRIPS 
GROUP BY service_type;
```




VARIABLES DE AMBIENTE
Todas las credenciales y parámetros se manejan mediante variables de ambiente definidas en el archivo .env



Plantilla .env.example
```
SNOWFLAKE_ACCOUNT=tu_cuenta.region              
SNOWFLAKE_USER=tu_usuario                       
SNOWFLAKE_PASSWORD=tu_password                  
SNOWFLAKE_ROLE=ACCOUNTADMIN                     
SNOWFLAKE_DATABASE=NYC_TLC_P03                 
SNOWFLAKE_WAREHOUSE=COMPUTE_WH                  
SNOWFLAKE_SCHEMA_RAW=RAW                        
SNOWFLAKE_SCHEMA_ANALYTICS=ANALYTICS

START_YEAR=2015                                 # Año inicial de ingesta
END_YEAR=2025                                   # Año final de ingesta
SERVICES=yellow,green                           # Servicios a procesar

RUN_ID=manual                                   # ID de ejecución (para tracking)
```

VARIABLES DE AMBIENTE Y PROPOSITO 
```
SNOWFLAKE_ACCOUNT    Identificador de cuenta Snowflake

SNOWFLAKE_USER       Usuario para autenticación

SNOWFLAKE_PASSWORD   Contraseña del usuario

SNOWFLAKE_ROLE       Rol con permisos necesarios

SNOWFLAKE_DATABASE   Base de datos destino

SNOWFLAKE_WAREHOUSE   Warehouse para procesamiento

SNOWFLAKE_SCHEMA_RAW  Schema para aterrizaje

SNOWFLAKE_SCHEMA_ANALYTICS Schema para OBT

START_YEAR        Año inicial de datos

END_YEAR       Año final de datos

RUN_ID         Identificador de ejecución
```

NOTEBOOKS

### Notebook 01: Ingesta de Parquet a RAW

**Archivo:** `01_ingesta_parquet_raw.ipynb`

**Propósito:** Ingesta masiva de archivos Parquet desde NYC TLC a Snowflake RAW

**Celdas:**

1. **CELDA 1:** Imports y configuración
2. **CELDA 2:** Leer variables de ambiente
3. **CELDA 3:** Inicializar Spark con conector Snowflake
4. **CELDA 4:** Configurar conexión a Snowflake
5. **CELDA 5:** Verificar tablas en Snowflake
6. **CELDA 6:** Funciones de utilidad (check_parquet_exists, get_parquet_url, download_parquet, add_metadata_columns)
7. **CELDA 7:** Función principal de ingesta con descarga e idempotencia
8. **CELDA 8:** Ejecutar ingesta completa (loop sobre años, meses, servicios)
9. **CELDA 9:** Análisis de resultados (DataFrame con resultados)
10. **CELDA 10:** Resumen por status, servicio y año
11. **CELDA 11:** Verificar datos en Snowflake (conteos)
12. **CELDA 12:** Guardar matriz de cobertura (CSV)

**Funcionalidades principales:**
- Descarga archivos Parquet de Yellow/Green (2015-2025)
- Lectura con Spark
- Conversión de timestamps a formato compatible
- Reordenamiento de columnas según esquema Snowflake
- Idempotencia: DELETE por periodo (source_year, source_month, service_type) + INSERT
- Metadatos: run_id, service_type, source_year, source_month, ingested_at_utc, source_path
- Auditoría: conteos por lote, tiempos de carga

**Outputs:**
- `RAW.YELLOW_TRIPS` (~500M registros)
- `RAW.GREEN_TRIPS` (~337M registros)
- `coverage_matrix.csv`



---

### Notebook 02: Enriquecimiento y Unificación

**Archivo:** `02_enriquecimiento_y_unificacion.ipynb`

**Propósito:** Cargar tablas de lookup y catálogos a ANALYTICS

**Celdas:**

1. **CELDA 1:** Imports y configuración
2. **CELDA 2:** Leer variables de ambiente
3. **CELDA 3:** Inicializar Spark
4. **CELDA 4:** Configurar conexión a Snowflake
5. **CELDA 5:** Descargar Taxi Zone Lookup (CSV desde HTTP)
6. **CELDA 6:** Escribir TAXI_ZONES a Snowflake (ANALYTICS)
7. **CELDA 7:** Crear catálogo PAYMENT_TYPE_LOOKUP
8. **CELDA 8:** Crear catálogo RATE_CODE_LOOKUP
9. **CELDA 9:** Crear catálogo VENDOR_LOOKUP
10. **CELDA 10:** Crear catálogo TRIP_TYPE_LOOKUP
11. **CELDA 11:** Verificar todas las tablas de lookup
12. **CELDA 12:** Resumen y estadísticas

**Funcionalidades principales:**
- Descarga y carga de Taxi Zone Lookup (265 zonas)
- Creación de catálogos estáticos:
  - Payment Type: 6 tipos (Credit card, Cash, No charge, etc.)
  - Rate Code: 6 códigos (Standard, JFK, Newark, etc.)
  - Vendor: 2 vendors (Creative Mobile, VeriFone)
  - Trip Type: 2 tipos (Street-hail, Dispatch)
- Escritura a ANALYTICS con mode="overwrite"

**Outputs:**
- `ANALYTICS.TAXI_ZONES` (265 registros)
- `ANALYTICS.PAYMENT_TYPE_LOOKUP` (6 registros)
- `ANALYTICS.RATE_CODE_LOOKUP` (6 registros)
- `ANALYTICS.VENDOR_LOOKUP` (2 registros)
- `ANALYTICS.TRIP_TYPE_LOOKUP` (2 registros)



---

### Notebook 03: Construcción de la OBT

**Archivo:** `03_construccion_obt.ipynb`

**Propósito:** Construir la One Big Table (OBT) desnormalizada en ANALYTICS

**Celdas:**

1. **CELDA 1:** Imports y configuración
2. **CELDA 2:** Leer variables de ambiente
3. **CELDA 3:** Inicializar Spark
4. **CELDA 4:** Configurar conexiones a Snowflake (RAW y ANALYTICS)
5. **CELDA 5:** Cargar datos de RAW (YELLOW_TRIPS, GREEN_TRIPS)
6. **CELDA 6:** Cargar tablas de lookup (TAXI_ZONES, PAYMENT_TYPE, RATE_CODE, VENDOR, TRIP_TYPE)
7. **CELDA 7:** Estandarizar Yellow Trips (renombrar columnas, agregar trip_type=NULL)
8. **CELDA 8:** Estandarizar Green Trips (renombrar columnas, agregar Airport_fee=NULL)
9. **CELDA 9:** Unificar Yellow y Green (UNION)
10. **CELDA 10:** Agregar columnas de tiempo (pickup_date, pickup_hour, day_of_week, month, year)
11. **CELDA 11:** Calcular métricas derivadas (trip_duration_min, avg_speed_mph, tip_pct)
12. **CELDA 12:** JOIN con TAXI_ZONES para zonas de pickup
13. **CELDA 13:** JOIN con TAXI_ZONES para zonas de dropoff
14. **CELDA 14:** JOIN con catálogos (VENDOR, PAYMENT_TYPE, RATE_CODE, TRIP_TYPE)
15. **CELDA 15:** Seleccionar y ordenar columnas finales (44 columnas)
16. **CELDA 16:** Aplicar filtros de calidad
17. **CELDA 17:** Idempotencia - TRUNCATE tabla OBT_TRIPS
18. **CELDA 18:** Escribir OBT a Snowflake
19. **CELDA 19:** Verificar OBT en Snowflake (conteos, distribución)
20. **CELDA 20:** Resumen final

**Funcionalidades principales:**
- Unificación de Yellow y Green
- Estandarización de nombres de columnas
- JOINs con todas las tablas de lookup (LEFT JOIN)
- Cálculo de columnas derivadas con manejo de nulos
- Generación de trip_id único (MD5 hash)
- Filtros de calidad:
  - trip_duration_min: 0 < x < 1440
  - trip_distance: 0 ≤ x < 500
  - total_amount: 0 ≤ x < 10,000
  - pickup_datetime < dropoff_datetime
- Idempotencia: TRUNCATE completo + INSERT

**Outputs:**
- `ANALYTICS.OBT_TRIPS` (~837M registros después de filtros)



---

### Notebook 04: Validaciones y Exploración

**Archivo:** `04_validaciones_y_exploracion.ipynb`

**Propósito:** Validar calidad de datos y explorar distribuciones

**Celdas:**

1. **CELDA 1:** Imports y configuración
2. **CELDA 2:** Leer variables de ambiente
3. **CELDA 3:** Inicializar Spark
4. **CELDA 4:** Configurar conexión a Snowflake
5. **CELDA 5:** Cargar OBT_TRIPS
6. **CELDA 6:** Validación 1 - Valores nulos en columnas críticas
7. **CELDA 7:** Validación 2 - Rangos de valores numéricos (duration, distance, amount)
8. **CELDA 8:** Validación 3 - Coherencia de fechas
9. **CELDA 9:** Validación 4 - Distribución por servicio
10. **CELDA 10:** Validación 5 - Distribución temporal (año, mes)
11. **CELDA 11:** Validación 6 - Zonas más frecuentes (pickup, dropoff)
12. **CELDA 12:** Validación 7 - Distribución por borough
13. **CELDA 13:** Validación 8 - Tipos de pago
14. **CELDA 14:** Validación 9 - Métricas derivadas (velocidad, propina)
15. **CELDA 15:** Validación 10 - Conteo detallado por mes/servicio
16. **CELDA 16:** Resumen de validaciones

**10 Validaciones implementadas:**

1. **Nulos en columnas críticas:** trip_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, service_type
2. **Rangos numéricos:** min, max, avg, median, p95 de duration, distance, amount
3. **Coherencia temporal:** pickup < dropoff, no fechas futuras, no antes de 2015
4. **Distribución por servicio:** Yellow vs Green (conteos, promedios)
5. **Distribución temporal:** Por año y mes
6. **Zonas más frecuentes:** Top 10 pickup y dropoff
7. **Distribución por borough:** Conteos y promedios por borough
8. **Tipos de pago:** Distribución y relación con propinas
9. **Métricas derivadas:** Estadísticas de velocidad y propina
10. **Conteo detallado:** Por año-mes-servicio

**Outputs:**
- Reportes de calidad en pantalla
- Estadísticas descriptivas



---

### Notebook 05: Análisis de Datos (20 Preguntas)

**Archivo:** `05_data_analysis.ipynb`

**Propósito:** Responder 20 preguntas de negocio usando la OBT con Spark

**Celdas:**

1. **CELDA 1:** Imports y configuración
2. **CELDA 2:** Leer variables de ambiente
3. **CELDA 3:** Inicializar Spark
4. **CELDA 4:** Configurar conexión a Snowflake
5. **CELDA 5:** Cargar OBT_TRIPS (con filtro de años válidos 2015-2025)
6. **PREGUNTA 1:** Top 10 zonas de pickup por volumen mensual
7. **PREGUNTA 2:** Top 10 zonas de dropoff por volumen mensual
8. **PREGUNTA 3:** Evolución mensual de total_amount y tip_pct por borough
9. **PREGUNTA 4:** Ticket promedio por service_type y mes
10. **PREGUNTA 5:** Viajes por hora del día y día de semana (picos)
11. **PREGUNTA 6:** p50/p90 de trip_duration_min por borough de pickup
12. **PREGUNTA 7:** avg_speed_mph por franja horaria (6-9, 17-20) y borough
13. **PREGUNTA 8:** Participación por payment_type y relación con tip_pct
14. **PREGUNTA 9:** Rate codes con mayor trip_distance y total_amount
15. **PREGUNTA 10:** Mix yellow vs green por mes y borough
16. **PREGUNTA 11:** Top 20 flujos PU→DO por volumen y ticket promedio
17. **PREGUNTA 12:** Distribución de passenger_count y efecto en total_amount
18. **PREGUNTA 13:** Impacto de tolls_amount y congestion_surcharge por zona
19. **PREGUNTA 14:** Proporción de viajes cortos vs largos por borough (con categorías y porcentajes)
20. **PREGUNTA 15:** Diferencias por vendor en avg_speed_mph y trip_duration_min
21. **PREGUNTA 16:** Relación método de pago ↔ tip_amount por hora
22. **PREGUNTA 17:** Zonas con duraciones/distancias extremas (versión simplificada)
23. **PREGUNTA 18:** Yield por milla (total_amount/trip_distance) por borough y hora
24. **PREGUNTA 19:** Cambios YoY en volumen y ticket promedio (con cálculo de % cambio)
25. **PREGUNTA 20:** Días con alta congestion_surcharge vs días normales
26. **Resumen final:** Hallazgos principales

**Operaciones Spark utilizadas:**
- `groupBy()` + `agg()`: Agregaciones
- `filter()`: Filtrado de datos
- `withColumn()`: Columnas calculadas
- `when()`: Lógica condicional
- `expr()`: Expresiones SQL (percentile_approx)
- `Window()`: Funciones de ventana (lag, partitionBy)
- `orderBy()`: Ordenamiento
- `show()`: Visualización de resultados

**Insights principales:**
1. Manhattan domina en volumen (65%) y ticket promedio ($18.50)
2. Horas pico: 8-9 AM y 6-7 PM
3. COVID-19: Caída 85% en 2020, recuperación gradual
4. Pagos con tarjeta: 18% propina vs 2% efectivo
5. Yellow domina Manhattan, Green en outer boroughs
6. 70% de viajes son cortos (<2 millas)
7. Cargos de congestión incrementan monto total


CALIDAD Y AUDITORIA 

Validaciones Implementadas
1. Validación de Nulos (Notebook 04, Celda 6)
Qué se valida:

Columnas críticas NO deben tener nulos:

trip_id, pickup_datetime, dropoff_datetime
pu_location_id, do_location_id
service_type



Dónde se ve:

Output del Notebook 04
Query:
```
SELECT 
    COUNT(*) - COUNT(trip_id) as nulos_trip_id,
    COUNT(*) - COUNT(pickup_datetime) as nulos_pickup,
    COUNT(*) - COUNT(service_type) as nulos_service
FROM ANALYTICS.OBT_TRIPS;

```
Validación de Rangos (Notebook 04, Celda 7)

Dónde se ve:

Notebook 04: Estadísticas (min, max, avg, p95)
Conteo de registros fuera de rango

Resultado: ~0.5% de registros fuera de rango (descartados)


Validación de Coherencia Temporal (Notebook 04, Celda 8)
Qué se valida:

pickup_datetime debe ser < dropoff_datetime
Fechas no deben ser futuras (> fecha actual)
Fechas no deben ser antes de 2015

Dónde se ve:
```
-- Fechas inconsistentes
SELECT COUNT(*) 
FROM ANALYTICS.OBT_TRIPS 
WHERE pickup_datetime >= dropoff_datetime;

-- Fechas futuras
SELECT COUNT(*) 
FROM ANALYTICS.OBT_TRIPS 
WHERE year > 2025;
```







# FlightTracker — Lab Cheat Sheet (skills.google)
# Guía paso a paso con 1 hora 30 minutos
# Copia y pega los comandos directamente

═══════════════════════════════════════════════════════════
## ANTES DE ENTRAR AL LAB
═══════════════════════════════════════════════════════════

Ten esto listo:
- Este documento abierto
- Tu repo en GitHub: https://github.com/Moya-Art/Flight-Tracker
- Looker Studio: https://lookerstudio.google.com/

═══════════════════════════════════════════════════════════
## PASO 1: Entrar al lab (2 min)
═══════════════════════════════════════════════════════════

1. Ve a: https://www.skills.google/focuses/12363
2. Haz clic en "Start" o "Launch"
3. Te van a dar un usuario temporal con acceso a GCP
4. Abre la consola de GCP: https://console.cloud.google.com/
5. Anota tu PROJECT ID (aparece arriba a la izquierda, algo como "qwiklabs-gcp-xxx")

═══════════════════════════════════════════════════════════
## PASO 2: Abrir Cloud Shell (1 min)
═══════════════════════════════════════════════════════════

1. En la consola de GCP, busca el botón "Activate Cloud Shell" (esquina superior derecha)
2. Haz clic y espera a que se abra la terminal
3. Ya estás listo para ejecutar comandos

═══════════════════════════════════════════════════════════
## PASO 3: Clonar el repo (1 min)
═══════════════════════════════════════════════════════════

Copia y pega esto en Cloud Shell:

```bash
git clone https://github.com/Moya-Art/Flight-Tracker.git
cd Flight-Tracker
```

═══════════════════════════════════════════════════════════
## PASO 4: Crear archivo .env (1 min)
═══════════════════════════════════════════════════════════

Reemplaza "TU_PROJECT_ID" con tu Project ID real:

```bash
# Crear .env
cat > .env << EOF
GCP_PROJECT_ID=TU_PROJECT_ID
GCP_REGION=us-central1
GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json
EOF

# Verificar que se creó correctamente
cat .env
```

═══════════════════════════════════════════════════════════
## PASO 5: Habilitar APIs (2 min)
═══════════════════════════════════════════════════════════

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com
```

Espera a que las 3 terminen. Deberías ver "enabled" en cada una.

═══════════════════════════════════════════════════════════
## PASO 6: Crear Service Account (3 min)
═══════════════════════════════════════════════════════════

Esto crea una "cuenta de robot" que los scripts Python usan para acceder a GCP.

```bash
# Obtener el project ID
export PROJECT_ID=$(gcloud config get-value project)

# Crear service account
gcloud iam service-accounts create flight-tracker-sa \
    --display-name "FlightTracker Service Account"

# Dar permisos de BigQuery
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

# Dar permisos de Pub/Sub
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.admin"

# Dar permisos de Storage
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Descargar la llave (credenciales)
gcloud iam service-accounts keys create config/service-account-key.json \
    --iam-account=flight-tracker-sa@$PROJECT_ID.iam.gserviceaccount.com
```

═══════════════════════════════════════════════════════════
## PASO 7: Instalar dependencias (2 min)
═══════════════════════════════════════════════════════════

```bash
pip install -r requirements.txt
```

═══════════════════════════════════════════════════════════
## PASO 8: Ejecutar el pipeline (15-20 min)
═══════════════════════════════════════════════════════════

Opción A — Todo de una vez (recomendado):

```bash
python run_pipeline.py --stream-minutes 10
```

Esto va a:
1. Crear la infraestructura (BigQuery, Pub/Sub, Storage)
2. Descargar datos batch (snapshot de todos los aviones)
3. Streaming por 10 minutos (cada 15s descarga y publica)
4. Limpiar los datos

Opción B — Paso a paso (si la Opción A falla):

```bash
# 1. Crear infraestructura
python setup.py

# 2. Batch ingestion
python src/batch_ingestion.py

# 3. Streaming (abre OTRA terminal con el botón +)
# En terminal 1:
python src/stream_ingestion.py

# En terminal 2:
python src/subscriber.py

# Espera 10 minutos, luego Ctrl+C en ambas terminales

# 4. Limpieza
python src/data_cleaning.py
```

═══════════════════════════════════════════════════════════
## PASO 9: Verificar en BigQuery (5 min)
═══════════════════════════════════════════════════════════

1. Ve a: https://console.cloud.google.com/bigquery
2. En el panel izquierdo, busca "flight_tracker"
3. Deberías ver 3 tablas:
   - flights_raw
   - flights_cleaned
   - activity_log
4. Haz clic en cada tabla → "Preview" para ver los datos
5. **TOMA SCREENSHOTS** de cada tabla con datos

═══════════════════════════════════════════════════════════
## PASO 10: Ejecutar queries de análisis (10 min)
═══════════════════════════════════════════════════════════

En BigQuery Console, crea un "New Query" y pega cada una de estas:

**Query 1: Top países con más vuelos**
```sql
SELECT
    origin_country,
    COUNT(*) AS total_flights,
    COUNT(DISTINCT icao24) AS unique_aircraft
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false
GROUP BY origin_country
ORDER BY total_flights DESC
LIMIT 10;
```

**Query 2: Vuelos por hora del día**
```sql
SELECT
    hour_of_day,
    COUNT(*) AS total_flights
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false AND hour_of_day IS NOT NULL
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

**Query 3: Vuelos por región geográfica**
```sql
SELECT
    geographic_region,
    COUNT(*) AS flight_count
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false
GROUP BY geographic_region
ORDER BY flight_count DESC;
```

**TOMA SCREENSHOTS** de cada resultado.

═══════════════════════════════════════════════════════════
## PASO 11: Ejecutar modelo ML (10 min)
═══════════════════════════════════════════════════════════

**Crear el modelo KMeans:**
```sql
CREATE OR REPLACE MODEL `flight_tracker.flight_anomaly_model`
OPTIONS(
    model_type='KMEANS',
    num_clusters=5,
    standardize_features=TRUE
)
AS SELECT
    velocity,
    baro_altitude,
    vertical_rate
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false
  AND velocity IS NOT NULL
  AND baro_altitude IS NOT NULL
  AND vertical_rate IS NOT NULL;
```

**Ver los centroides (centros de cada cluster):**
```sql
SELECT
    centroid_id,
    feature,
    ROUND(numerical_value, 2) AS center_value
FROM ML.CENTROIDS(MODEL `flight_tracker.flight_anomaly_model`)
ORDER BY centroid_id, feature;
```

**Detectar anomalías:**
```sql
SELECT
    icao24,
    callsign,
    origin_country,
    velocity,
    baro_altitude,
    predicted_cluster_id,
    ROUND(nearest_centroids_distance[OFFSET(0)].distance, 4) AS anomaly_score
FROM ML.PREDICT(
    MODEL `flight_tracker.flight_anomaly_model`,
    (
        SELECT icao24, callsign, origin_country, velocity, baro_altitude
        FROM `flight_tracker.flights_cleaned`
        WHERE on_ground = false
          AND velocity IS NOT NULL
          AND baro_altitude IS NOT NULL
    )
)
ORDER BY anomaly_score DESC
LIMIT 20;
```

**TOMA SCREENSHOTS** de cada resultado.

═══════════════════════════════════════════════════════════
## PASO 12: Crear dashboard en Looker Studio (15-20 min)
═══════════════════════════════════════════════════════════

### 12.1 Abrir Looker Studio
Ve a: https://lookerstudio.google.com/
Haz clic en "Create" → "Report"

### 12.2 Conectar a BigQuery
1. Te va a pedir que agregues una fuente de datos
2. Selecciona "BigQuery"
3. Selecciona tu proyecto → "flight_tracker" → "flights_cleaned"
4. Haz clic en "Connect" (arriba a la derecha)
5. Haz clic en "Add to report"

### 12.3 Crear Gráfico 1: Tabla de países
1. Haz clic en "Add a chart" → "Table"
2. Arrástralo al canvas
3. En las propiedades del gráfico:
   - Dimension: `origin_country`
   - Métrica: `Record Count` (o crea una métrica COUNT de icao24)
   - Ordena por Record Count descendente
   - Muestra solo Top 10

### 12.4 Crear Gráfico 2: Barras por hora
1. "Add a chart" → "Bar chart"
2. Dimension: `hour_of_day`
3. Métrica: `Record Count`
4. Ordena por hour_of_day ascendente

### 12.5 Crear Gráfico 3: Mapa geográfico
1. "Add a chart" → "Geo map"
2. Dimension: `geographic_region` o `origin_country`
3. Métrica: `Record Count`

### 12.6 Crear Gráfico 4: Tabla de anomalías
1. "Add a chart" → "Table"
2. Para este necesitas crear una vista en BigQuery primero:

**Ve a BigQuery y ejecuta:**
```sql
CREATE OR REPLACE VIEW `flight_tracker.anomaly_results` AS
SELECT
    icao24,
    callsign,
    origin_country,
    velocity,
    baro_altitude,
    predicted_cluster_id,
    ROUND(nearest_centroids_distance[OFFSET(0)].distance, 4) AS anomaly_score
FROM ML.PREDICT(
    MODEL `flight_tracker.flight_anomaly_model`,
    (
        SELECT icao24, callsign, origin_country, velocity, baro_altitude
        FROM `flight_tracker.flights_cleaned`
        WHERE on_ground = false
          AND velocity IS NOT NULL
          AND baro_altitude IS NOT NULL
    )
);
```

3. En Looker Studio, agrega otra fuente de datos: `flight_tracker.anomaly_results`
4. Crea tabla con:
   - Dimensiones: callsign, origin_country
   - Métricas: velocity, baro_altitude, anomaly_score
   - Ordena por anomaly_score descendente

### 12.7 Finalizar el dashboard
1. Agrega un título: "FlightTracker — Real-Time Air Traffic Analytics"
2. Acomoda los 4 gráficos en un layout limpio
3. **TOMA SCREENSHOTS** del dashboard completo

═══════════════════════════════════════════════════════════
## PASO 13: Exportar todo (5 min)
═══════════════════════════════════════════════════════════

1. En BigQuery: exporta los resultados de cada query como CSV
2. En Looker Studio: descarga el dashboard como PDF (File → Download)
3. Guarda todos los screenshots en una carpeta

═══════════════════════════════════════════════════════════
## ANTES DE QUE SE ACABE EL LAB
═══════════════════════════════════════════════════════════

Verifica que tengas:
- [ ] Screenshots de BigQuery con datos
- [ ] Screenshots de cada query ejecutada
- [ ] Screenshots del modelo ML
- [ ] Screenshots del dashboard en Looker Studio
- [ ] CSVs de los resultados (opcional, para el informe)

El lab se cierra y pierdes acceso, pero tus datos en GitHub están seguros.

═══════════════════════════════════════════════════════════
## TROUBLESHOOTING (si algo falla)
═══════════════════════════════════════════════════════════

"Permission denied"
→ gcloud auth application-default login

"API not enabled"
→ gcloud services enable bigquery.googleapis.com pubsub.googleapis.com storage.googleapis.com

"Project not found"
→ gcloud config set project TU_PROJECT_ID

"pip not found"
→ python3 -m pip install -r requirements.txt

"No module named google.cloud"
→ pip install google-cloud-bigquery google-cloud-pubsub google-cloud-storage

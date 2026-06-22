# FlightTracker — Crónica de la Sesión 2
## De OpenSky bloqueado a 442,000 vuelos en BigQuery

**Fecha:** 21-22 de junio de 2026
**Duración:** ~3 horas
**Resultado:** Pipeline completo funcionando con datos reales de OpenSky

---

## Contexto

En la sesión 1 intentamos correr todo desde GCP Cloud Shell, pero descubrimos que **OpenSky Network bloquea peticiones desde IPs de Google Cloud**. La API funciona desde computadores personales pero no desde ningún servicio de GCP (Cloud Shell, Cloud Run, Compute Engine).

Decidimos hacer una segunda sesión con un approach diferente: **punto medio entre local y cloud**.

---

## Arquitectura Final

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   OpenSky    │────▶│  Pub/Sub     │────▶│  Subscriber  │────▶│  BigQuery    │
│   API        │     │  (GCP)       │     │  (Cloud Shell)│     │  (GCP)       │
│              │     │              │     │              │     │              │
│  Tu PC       │     │  flight-     │     │  Python      │     │  flights_raw │
│  cada 15s    │     │  updates     │     │  subscriber  │     │  442K rows   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                                                                     │
                                                                     ▼
                                                              ┌──────────────┐
                                                              │  Looker      │
                                                              │  Studio      │
                                                              │  Dashboard   │
                                                              └──────────────┘
```

**¿Por qué este diseño?**
- OpenSky solo funciona desde IPs no-GCP → producer corre en tu PC
- Pub/Sub, BigQuery, Looker Studio → todo en GCP
- Subscriber corre en Cloud Shell (es un servicio GCP)
- La arquitectura demuestra el patrón recomendado por el profe

---

## Timeline de Eventos

### Fase 1: Configuración del Lab (21:50 - 22:00)

**Paso 1: Entrar al lab**
- Entramos a skills.google con el lab `qwiklabs-gcp-04-0ff42d7b1410`
- Obtenemos credenciales temporales de GCP

**Paso 2: Clonar el repo**
```bash
git clone https://github.com/Moya-Art/Flight-Tracker.git
cd Flight-Tracker
```

**Paso 3: Habilitar APIs**
```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
```

---

### Fase 2: Pub/Sub (22:00 - 22:05)

**Creado desde la consola GCP (click, no shell):**

1. Topic: `flight-updates`
2. Subscription: `flight-updates-sub` (ack deadline: 60s)

---

### Fase 3: BigQuery (22:05 - 22:10)

**Creado desde la consola GCP:**

1. Dataset: `flight_tracker`
2. Tabla `flights_raw` con schema:
```
icao24:STRING, callsign:STRING, origin_country:STRING,
time_position:TIMESTAMP, last_contact:TIMESTAMP,
longitude:FLOAT, latitude:FLOAT, baro_altitude:FLOAT,
on_ground:BOOLEAN, velocity:FLOAT, true_track:FLOAT,
vertical_rate:FLOAT, geo_altitude:FLOAT, squawk:STRING,
ingestion_source:STRING, ingestion_timestamp:TIMESTAMP
```
3. Tabla `activity_log`:
```
execution_timestamp:TIMESTAMP, pipeline:STRING,
record_count:INTEGER, status:STRING, error_message:STRING
```

**Error encontrado:** `FLOAT64` no es válido en el schema de BigQuery. Se usa `FLOAT` en su lugar.

---

### Fase 4: Cloud Run — Intento Fallido (22:10 - 22:30)

**Objetivo:** Desplegar el producer como servicio en Cloud Run.

**Paso 1: Construir la imagen Docker**
```bash
cd Flight-Tracker/cloud_run
gcloud builds submit --tag gcr.io/qwiklabs-gcp-04-0ff42d7b1410/flight-tracker-producer
```

**Paso 2: Desplegar en Cloud Run**
- Service name: `flight-tracker-producer`
- Region: `us-west1` (Oregon)
- Container image: `gcr.io/qwiklabs-gcp-04-0ff42d7b1410/flight-tracker-producer`
- Variables de entorno: `GCP_PROJECT_ID`, `PUBSUB_TOPIC`, `POLL_INTERVAL`
- Auth: Allow public access

**Resultado:** El servicio se desplegó correctamente, pero **todas las llamadas a OpenSky fallaban** (9 errores, 0 API calls).

**Causa:** OpenSky bloquea peticiones desde IPs de Google Cloud. Esto es un bloqueo a nivel de IP, no un error de código.

**URL del servicio:** `https://flight-tracker-producer-286921820307.us-west1.run.app`

---

### Fase 5: Dataflow — Intento Fallido (22:30 - 22:40)

**Objetivo:** Usar Dataflow template "Pub/Sub to BigQuery" para procesar mensajes.

**Error 1:** "Either input topic or input subscription must be provided, but not both"
- **Causa:** Llenamos ambos campos (topic Y subscription). Solo se debe llenar uno.
- **Solución:** Crear nuevo job solo con subscription.

**Error 2:** "ZONE_RESOURCE_POOL_EXHAUSTED"
- **Causa:** El lab no tiene recursos disponibles en `us-west1` para crear workers de Dataflow.
- **Solución:** Intentar con otra región, pero el lab solo permite `us-west1`.

**Conclusión:** Dataflow no es viable en este lab por restricciones de recursos.

---

### Fase 6: El Punto Medio — Producer Local + Subscriber en Cloud (22:40 - 22:50)

**Decisión:** El producer corre en tu PC (donde OpenSky funciona), todo lo demás en GCP.

**Paso 1: Crear service account**
```bash
gcloud iam service-accounts create flight-tracker-sa \
    --display-name "FlightTracker Service Account"

gcloud projects add-iam-policy-binding qwiklabs-gcp-04-0ff42d7b1410 \
    --member="serviceAccount:flight-tracker-sa@qwiklabs-gcp-04-0ff42d7b1410.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"

gcloud iam service-accounts keys create ~/key.json \
    --iam-account=flight-tracker-sa@qwiklabs-gcp-04-0ff42d7b1410.iam.gserviceaccount.com
```

**Paso 2: Copiar la key al proyecto local**
- Copiar el JSON de `~/key.json` a `config/service-account-key.json`
- Actualizar `.env` con el nuevo project ID

**Paso 3: Correr el producer desde tu PC**
```bash
python src/stream_ingestion.py
```

**Resultado:** ✅ Funcionó! 28 mensajes publicados a Pub/Sub con ~7,000 vuelos cada uno.

**Problema encontrado:** OpenSky rate limiting (HTTP 429) después de 28 mensajes. Esto es normal — el API gratuito limita peticiones.

---

### Fase 7: Subscriber en Cloud Shell (22:50 - 23:00)

**Objetivo:** Leer mensajes de Pub/Sub y escribir en BigQuery.

**Paso 1: Crear .env en Cloud Shell**
```bash
cat > .env << EOF
GCP_PROJECT_ID=qwiklabs-gcp-04-0ff42d7b1410
GCP_REGION=us-west1
EOF
```

**Paso 2: Crear carpeta de logs**
```bash
mkdir -p logs
```

**Paso 3: Correr el subscriber**
```bash
python src/subscriber.py
```

**Error encontrado:** `FileNotFoundError: logs/subscriber.log` — La carpeta `logs/` no existía. Se creó con `mkdir -p logs`.

**Resultado:** ✅ Funcionó! 442,555 filas escritas en BigQuery.

**Errores de rate limit:** BigQuery limitó las escrituras después de un tiempo (403 Exceeded rate limits). Esto es normal — ya teníamos suficientes datos.

---

### Fase 8: Data Cleaning (23:00 - 23:05)

**Objetivo:** Crear tabla limpia con columnas derivadas.

**Ejecutado en BigQuery Console:**
```sql
CREATE OR REPLACE TABLE `flight_tracker.flights_cleaned`
AS SELECT
    *,
    CASE WHEN velocity < 50 THEN 'SLOW' ... END AS speed_category,
    CASE WHEN baro_altitude < 3000 THEN 'LOW' ... END AS altitude_zone,
    CASE WHEN longitude BETWEEN -130 AND -60 ... END AS geographic_region,
    EXTRACT(HOUR FROM time_position) AS hour_of_day,
    CASE WHEN icao24 IS NULL THEN 'INVALID' ... END AS data_quality_flag
FROM `flight_tracker.flights_raw`
WHERE icao24 IS NOT NULL ...
```

**Resultado:** Tabla `flights_cleaned` creada con columnas derivadas para análisis.

---

### Fase 9: Queries de Análisis — Objetivo 1 (23:05 - 23:15)

**5 queries ejecutadas en BigQuery Console:**

1. **Top países con más vuelos** — US: 96K, Canada: 7K, Australia: 5K
2. **Vuelos por hora del día** — Pico a las 23:00 (154K vuelos)
3. **Vuelos por región y altitud** — Europa y Asia dominan en HIGH altitude
4. **Congestión por país/región** — US y Canada: VERY_HIGH
5. **Distribución de velocidad por altitud** — HIGH + FAST: 59K vuelos

**Screenshots tomados de cada resultado.**

---

### Fase 10: Modelo ML — Objetivo 2 (23:15 - 23:25)

**3 queries ejecutadas en BigQuery Console:**

1. **Crear modelo KMeans** (5 clusters, 3 features: velocity, baro_altitude, vertical_rate)
2. **Ver centroides** — 5 clusters identificados
3. **Detectar anomalías** — Top anomalías: EVA062 (Taiwan, score 33.76), QTR830 (Qatar, score 33.06)

**Error encontrado:** `predicted_cluster_id` no existe. BigQuery ML KMeans usa `centroid_id`. Se corrigió el query.

**Error 2:** `vertical_rate is not found in input data` — El modelo fue entrenado con 3 features pero el query de predicción solo enviaba 2. Se corrigió agregando `vertical_rate` al SELECT.

**Screenshots tomados.**

---

### Fase 11: Dashboard en Looker Studio (23:25 - 23:40)

**Pasos:**
1. Ir a https://lookerstudio.google.com
2. Create Report → BigQuery → `flight_tracker.flights_cleaned`
3. Crear 4 gráficos:
   - **Tabla** — Top países por vuelos
   - **Bar chart** — Vuelos por hora
   - **Geo map** — Vuelos por región geográfica
   - **Tabla de anomalías** — Conectada a view `anomaly_results`

**View creada en BigQuery:**
```sql
CREATE OR REPLACE VIEW `flight_tracker.anomaly_results` AS
SELECT icao24, callsign, origin_country, velocity, baro_altitude,
       centroid_id AS cluster_id,
       ROUND(nearest_centroids_distance[OFFSET(0)].distance, 4) AS anomaly_score
FROM ML.PREDICT(MODEL `flight_tracker.flight_anomaly_model`, ...)
```

**Resultado:** Dashboard creado con 4 gráficos. Se ve "horrible" pero funcional.

**Screenshots tomados.**

---

### Fase 12: Fin del Lab (23:40)

El lab se acabó justo cuando intentábamos descargar la tabla `flights_cleaned` como CSV.

**Lo que se perdió:**
- Acceso a BigQuery (datos ya no accesibles)
- Acceso al dashboard de Looker Studio (pero está guardado en la cuenta de Google)

**Lo que se conservó:**
- ✅ Screenshots de todo
- ✅ CSVs de la sesión anterior (en `sql/Queries_saved/`)
- ✅ Código en GitHub
- ✅ Dashboard en Looker Studio (accesible desde cuenta personal)

---

## Bugs Corregidos Durante las Sesiones

| Bug | Causa | Solución |
|-----|-------|----------|
| `BQ_TABLE_ACTIVITY_LOG is not defined` | Import faltante en batch_ingestion.py | Agregar al import |
| `GCS_PREFIX is not defined` | Variable incorrecta | Cambiar a `GCS_BATCH_PREFIX` |
| `Cloud Storage bucket not found` | Bucket no creado | Hacer Cloud Storage opcional (try/except) |
| `CREATE TABLE IF NOT EXISTS` | Tabla vacía si ya existía | Cambiar a `CREATE OR REPLACE TABLE` |
| `predicted_cluster_id` no existe | BigQuery ML usa `centroid_id` | Cambiar en los queries |
| `vertical_rate not found` | ML.PREDICT necesita todas las features | Agregar `vertical_rate` al SELECT |
| `FLOAT64` inválido | Schema de BigQuery usa `FLOAT` | Cambiar en el schema |
| `logs/ directory not found` | Carpeta no creada | `mkdir -p logs` |
| `429 rate limit` de OpenSky | API gratuito limita peticiones | Normal, se acepta |
| `403 rate limit` de BigQuery | Muchas escrituras simultáneas | Normal, ya hay suficientes datos |
| Dataflow `ZONE_RESOURCE_POOL_EXHAUSTED` | Sin recursos en us-west1 | Usar subscriber directo |
| Dataflow `topic or subscription, not both` | Se llenaron ambos campos | Solo llenar subscription |

---

## Resultados Finales

| Métrica | Valor |
|---------|-------|
| Registros en BigQuery | 442,555 |
| Mensajes publicados a Pub/Sub | 28 |
| Vuelos por mensaje | ~7,000 |
| Queries de análisis ejecutadas | 5 |
| Modelo ML entrenado | KMeans (5 clusters) |
| Gráficos en dashboard | 4 |
| Screenshots tomados | ~15 |
| Bugs corregidos | 12 |

---

## Archivos Creados/Modificados

| Archivo | Qué hace |
|---------|----------|
| `cloud_run/Dockerfile` | Contenedor Docker para Cloud Run |
| `cloud_run/cloud_run_producer.py` | Producer para Cloud Run |
| `cloud_run/requirements.txt` | Dependencias para Cloud Run |
| `src/cloud_run_producer.py` | Producer con Flask health check |
| `sql/data_cleaning.sql` | Query de limpieza para BigQuery Console |
| `sql/ml_model.sql` | Corregido: `centroid_id` |
| `src/batch_ingestion.py` | Corregido: imports, GCS optional |
| `src/data_cleaning.py` | Corregido: CREATE OR REPLACE |
| `.env` | Actualizado con nuevo project ID |
| `config/service-account-key.json` | Nueva key para este lab |

---

## Lecciones Aprendidas

1. **OpenSky bloquea IPs de GCP** — No se puede usar desde Cloud Shell, Cloud Run, etc.
2. **El punto medio funciona** — Producer local + servicios GCP es una arquitectura válida
3. **Dataflow no siempre está disponible** — Restricciones de recursos en labs
4. **El subscriber directo es más simple** — No necesita Dataflow, solo Python + Pub/Sub
5. **BigQuery tiene rate limits** — No se puede escribir demasiado rápido
6. **FLOAT64 vs FLOAT** — El schema de BigQuery usa FLOAT, no FLOAT64
7. **ML.PREDICT necesita todas las features** — Si el modelo se entrena con 3 features, el query debe enviar las 3
8. **centroid_id, no predicted_cluster_id** — BigQuery ML KMeans usa este nombre
9. **Siempre crear la carpeta logs/** — Los scripts fallan si no existe
10. **Los labs tienen límites de tiempo** — Tener screenshots como backup

---

## Próximos Pasos

1. **Informe PDF** — Usar la plantilla del profe, insertar screenshots
2. **Presentación PPT** — 10 láminas, estructura lógica
3. **Subir a GitHub** — Código actualizado con todos los fixes
4. **Practicar presentación** — 10 min + 5 min preguntas

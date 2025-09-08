# Vehicle Telematics Data Pipeline

## Project Overview
This project implements a scalable data pipeline to ingest high-frequency vehicle telematics data. The system enriches this data in real-time with contextual information to enable advanced security monitoring, anomaly detection, and operational analytics.

### Goals
- Ingest a continuous stream of simulated vehicle data into Kafka.
- Enrich raw telemetry with driver and geofence data in real-time.
- Store raw and enriched data in MongoDB and ClickHouse.
- Execute advanced analytical queries in ClickHouse to detect multiple types of security anomalies.

---

## Prerequisites and Setup

### Docker
- Ensure Docker and Docker Compose are running.
- Your `docker-compose.yml` should include services for:
  - Kafka & Zookeeper/KRaft
  - MongoDB
  - ClickHouse

### Python
- Create a dedicated Python virtual environment.
- Install the required libraries:
```bash
pip install kafka-python pymongo clickhouse-driver jsonschema faker
```

### Kafka Topics
- `vehicle_telemetry_raw`: Raw high-frequency vehicle data.
- `vehicle_events_enriched`: Processed data after contextual enrichment.

### Base Data
- Run `setup_data.py` to populate MongoDB `drivers` and `geofences` collections with sample data (one-time setup).

---

## Phase 1: High-Velocity Ingestion & Data Modeling

### Step 1.1: Model Data in MongoDB
- **Time Series Collection**: Create `vehicle_telemetry` collection optimized for time-series data.
```python
db.createCollection(
    "vehicle_telemetry",
    timeseries={
        "timeField": "timestamp",
        "metaField": "vehicle_id",
        "granularity": "seconds"
    }
)
```
- **Geospatial Index**: On `geofences` collection for GeoJSON polygons.
```python
db.geofences.create_index([("geometry", "2dsphere")])
```

### Step 1.2: Develop the Data Producer
- `producer.py` simulates 15 vehicles.
- Generates JSON messages every 5 seconds per vehicle with:
  - `vehicle_id`, `timestamp`, `latitude`, `longitude`, `speed`, `engine_status`.
- Publishes messages to `vehicle_telemetry_raw` Kafka topic.

### Step 1.3: Configure Real-time Ingestion into ClickHouse
1. **Destination Table**:
```sql
CREATE TABLE live_telemetry_queue (
    vehicle_id String,
    timestamp DateTime,
    latitude Float64,
    longitude Float64,
    speed UInt8
) ENGINE = MergeTree() PARTITION BY toYYYYMM(timestamp) ORDER BY (vehicle_id, timestamp);
```
2. **Kafka Engine Table**:
```sql
CREATE TABLE live_telemetry_stream (
    vehicle_id String,
    timestamp DateTime,
    latitude Float64,
    longitude Float64,
    speed UInt8
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'vehicle_telemetry_raw',
    kafka_group_name = 'clickhouse_telemetry_consumer',
    kafka_format = 'JSONEachRow';
```
3. **Materialized View**:
```sql
CREATE MATERIALIZED VIEW telemetry_consumer_mv TO live_telemetry_queue AS
SELECT vehicle_id, timestamp, latitude, longitude, speed
FROM live_telemetry_stream;
```

---

## Phase 2: Real-time Contextual Enrichment

### Step 2.1: Develop the Enrichment Consumer
- `enrichment_consumer.py` consumes messages from `vehicle_telemetry_raw`.
- Enriches each message with:
  - **Driver Lookup**: Retrieve `driver_name` from MongoDB `drivers`.
  - **Geofence Lookup**: Determine `current_zone` using `$geoIntersects`.
- Publishes enriched JSON to `vehicle_events_enriched`.

---

## Phase 3: Advanced Analytics & Anomaly Detection

### Step 3.1: Historical Data Ingestion
- `sync_to_clickhouse.py` consumes messages from `vehicle_events_enriched` and stores them in `enriched_telemetry_history` ClickHouse table for long-term analysis.

### Step 3.2: Threat Hunting Queries
1. **Geofence Breach Analysis**
```sql
SELECT vehicle_id, driver_name, timestamp, current_zone
FROM enriched_telemetry_history
WHERE current_zone = 'Restricted Area'
  AND (toHour(timestamp) < 9 OR toHour(timestamp) > 17);
```

2. **GPS Spoofing / Teleport Detection**
```sql
SELECT
    vehicle_id,
    timestamp,
    prev_timestamp,
    speed_kmh
FROM (
    SELECT
        vehicle_id,
        timestamp,
        neighbor(timestamp, -1) AS prev_timestamp,
        geoDistance(longitude, latitude, neighbor(longitude, -1), neighbor(latitude, -1)) / 1000 AS distance_km,
        (distance_km / (toUnixTimestamp(timestamp) - toUnixTimestamp(prev_timestamp))) * 3600 AS speed_kmh
    FROM enriched_telemetry_history
    ORDER BY vehicle_id, timestamp
)
WHERE speed_kmh > 400;
```

3. **Unusual Stop Detection**
```sql
SELECT
    vehicle_id,
    driver_name,
    stop_start,
    stop_end,
    duration_minutes
FROM (
    SELECT
        vehicle_id,
        driver_name,
        min(timestamp) as stop_start,
        max(timestamp) as stop_end,
        (toUnixTimestamp(max(timestamp)) - toUnixTimestamp(min(timestamp))) / 60 as duration_minutes
    FROM enriched_telemetry_history
    WHERE speed < 5 AND current_zone NOT IN ('Warehouse', 'Rest Area', 'Service Center')
    GROUP BY vehicle_id, driver_name
)
WHERE duration_minutes > 30;
```

---

## Security Focus
- Real-time enrichment transforms raw data into actionable intelligence.
- Time series and geospatial indexing ensures timely alerts.
- Kafka + ClickHouse architecture preserves high-fidelity immutable records for forensic analysis.
- Advanced queries enable proactive detection of behavioral anomalies.

---

## Usage
1. Start Docker services.
2. Run `setup_data.py` to initialize MongoDB.
3. Start `producer.py` to stream telemetry.
4. Start `enrichment_consumer.py` to enrich data.
5. Run `sync_to_clickhouse.py` for historical ingestion.
6. Execute analytical queries in ClickHouse for anomaly detection.

CREATE TABLE live_telemetry_queue ( 
    vehicle_id String, 
    timestamp DateTime, 
    latitude Float64, 
    longitude Float64, 
    speed UInt8 
) ENGINE = MergeTree() PARTITION BY toYYYYMM(timestamp) ORDER BY (vehicle_id, timestamp); 

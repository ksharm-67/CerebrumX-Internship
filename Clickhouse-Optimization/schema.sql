CREATE TABLE logs(
    event_date   Date DEFAULT today(),
    event_time   DateTime,
    level        String,           
    service      LowCardinality(String),
    host         String,
    user_id      UInt64,
    session_id   UUID,
    request_id   UUID,
    message      String,
    http_status  UInt16,
    latency_ms   UInt32,
    ip_address   IPv4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)  
ORDER BY (event_date, service, level, event_time)
SETTINGS index_granularity = 8192;

SELECT *
FROM logs
WHERE latency_ms > 500
ORDER BY latency_ms DESC
LIMIT 50;

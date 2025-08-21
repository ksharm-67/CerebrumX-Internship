SELECT *
FROM logs
WHERE event_date >= today() - 7
  AND level = 'ERROR'
ORDER BY event_time DESC
LIMIT 100;

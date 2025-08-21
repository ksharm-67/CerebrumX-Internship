SELECT service, count(*) AS errors
FROM logs
WHERE level = 'ERROR'
GROUP BY service
ORDER BY errors DESC;

SELECT 
    strftime('%H:%M', created_at) as time,
    AVG(entry) as avg_price
FROM signals 
WHERE symbol = 'ETHUSDT'
GROUP BY strftime('%H', created_at)
ORDER BY created_at;
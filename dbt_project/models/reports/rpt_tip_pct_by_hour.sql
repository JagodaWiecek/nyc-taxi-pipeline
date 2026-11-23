SELECT pickup_hour,
       SUM(trips) AS trips,
       ROUND(SUM(total_tip) / SUM(total_fare) * 100, 1) AS tip_pct_of_fare
FROM workspace.gold.agg_revenue_and_tips
WHERE payment_type = 1
GROUP BY pickup_hour
ORDER BY pickup_hour;
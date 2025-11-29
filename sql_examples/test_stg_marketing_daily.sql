-- Test query pour vérifier stg_marketing_daily
-- Date : 2024-11-25

SELECT  
    marketing_source,
    COUNT(DISTINCT date) AS days,
    SUM(sessions) AS total_sessions,
    SUM(conversions) AS total_conversions,
    ROUND(SUM(revenue), 2) AS total_revenue,
    ROUND(SUM(spend), 2) AS total_spend,
    ROUND(AVG(roas), 2) AS avg_roas,
    ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_rate_pct,
    COUNTIF(is_weekend) AS weekend_days,
    COUNTIF(is_organic_channel) AS organic_days
FROM `active-smile-479206-g4.marketing_analytics.stg_marketing_daily`
GROUP BY marketing_source
ORDER BY total_revenue DESC;
{{
    config(
        materialized='view'
    )
}}

-- Staging model : transforme les données brutes en format analytique

WITH source_data AS (
    SELECT *
    FROM {{ source('marketing_raw', 'daily_performance') }}
),

transformed AS (
    SELECT
        -- Dates
        PARSE_DATE('%Y-%m-%d', source_data.date) AS date,
        DATE_TRUNC(PARSE_DATE('%Y-%m-%d', source_data.date), WEEK) AS week_start_date,
        DATE_TRUNC(PARSE_DATE('%Y-%m-%d', source_data.date), MONTH) AS month_start_date,
        
        -- Dimensions
        source_data.source AS marketing_source,
        
        -- Métriques brutes
        source_data.sessions,
        source_data.conversions,
        ROUND(source_data.revenue, 2) AS revenue,
        ROUND(source_data.spend, 2) AS spend,
        
        -- KPIs calculés
        SAFE_DIVIDE(source_data.revenue, source_data.spend) AS roas,
        SAFE_DIVIDE(source_data.spend, source_data.conversions) AS cost_per_conversion,
        SAFE_DIVIDE(source_data.conversions, source_data.sessions) AS conversion_rate,
        SAFE_DIVIDE(source_data.revenue, source_data.conversions) AS revenue_per_conversion,
        SAFE_DIVIDE(source_data.revenue, source_data.sessions) AS revenue_per_session,
        
        -- Flags de contexte
        CASE 
            WHEN source_data.spend = 0 THEN TRUE 
            ELSE FALSE 
        END AS is_organic_channel,
        
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y-%m-%d', source_data.date)) IN (1, 7) 
            THEN TRUE 
            ELSE FALSE 
        END AS is_weekend
        
    FROM source_data
    WHERE source_data.sessions > 0  -- Filtrer lignes invalides
)

SELECT * FROM transformed
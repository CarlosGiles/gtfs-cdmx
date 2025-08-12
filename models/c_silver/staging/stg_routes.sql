{{ config(materialized='view', schema='SILVER') }}

SELECT
  route_id,
  agency_id,
  route_short_name,
  route_long_name,
  route_type,
  route_color,
  route_text_color
FROM {{ source('raw_bronze','routes') }}

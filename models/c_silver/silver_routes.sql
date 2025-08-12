{{ config(materialized='table', tags=['silver','gtfs','routes']) }}

with routes as (
  select
      route_id,
      agency_id,
      route_short_name,
      route_long_name,
      route_type::int as route_type,
      nullif(route_color, '') as route_color,
      nullif(route_text_color, '') as route_text_color
  from {{ ref('stg_routes') }}
),
only_metro_mb as (
  -- Filtra solo Metro (METRO) y Metrobús (MB)
  select *
  from routes
  where agency_id in ('METRO','MB')
)
select * from only_metro_mb;

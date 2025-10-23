-- Bronze layer: Raw schedule data with minimal transformation
-- This model takes the raw schedule data and applies basic cleaning

{{ config(
    materialized='table',
    description='Raw schedule data with basic cleaning applied'
) }}

select
   parent_id,
   calendar,
   created_at,
   updated_at,
   current_timestamp as _dbt_loaded_at,
   'bronze' as _dbt_layer
from {{ source('raw_postgres', 'raw_schedules') }}

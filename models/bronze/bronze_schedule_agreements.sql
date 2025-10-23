-- Bronze layer: Raw schedule agreements data with minimal transformation
-- This model takes the raw schedule agreements data and applies basic cleaning

{{ config(
    materialized='table',
    description='Raw schedule agreements data with basic cleaning applied'
) }}

select
   start_at,
   end_at,
   schedule_id,
   _dlt_load_id,
   _dlt_id,
   current_timestamp as _dbt_loaded_at,
   'bronze' as _dbt_layer
from {{ source('raw_postgres', 'raw_schedule_agreements') }}

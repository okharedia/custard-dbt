-- Bronze layer: Raw data with minimal transformation
-- This model takes the raw household data and applies basic cleaning

{{ config(
    materialized='table',
    description='Raw household data with basic cleaning applied'
) }}

-- For now, using sample data until source database is available
-- TODO: Replace with actual source connection when database is unlocked
select
   id as household_id,
   name as household_name,
   created_at,
   updated_at,
   current_timestamp as _dbt_loaded_at,
   'bronze' as _dbt_layer
from {{ source('raw_postgres', 'raw_households') }}
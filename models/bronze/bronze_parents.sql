-- Bronze layer: Raw parent data with minimal transformation
-- This model takes the raw parent data and applies basic cleaning

{{ config(
    materialized='table',
    description='Raw parent data with basic cleaning applied'
) }}

select
   id as parent_id,
   name as parent_name,
   household_id,
   created_at,
   updated_at,
   current_timestamp as _dbt_loaded_at,
   'bronze' as _dbt_layer
from {{ source('raw_postgres', 'raw_parents') }}

-- Bronze layer: Raw additional agreements details data with minimal transformation
-- This model takes the raw additional agreements details data and applies basic cleaning

{{ config(
    materialized='table',
    description='Raw additional agreements details data with basic cleaning applied'
) }}

select
   additional_agreements_id,
   reason,
   created_at,
   updated_at,
   current_timestamp as _dbt_loaded_at,
   'bronze' as _dbt_layer
from {{ source('raw_postgres', 'raw_additional_agreements_details') }}

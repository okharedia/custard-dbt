-- Bronze layer: Raw additional agreements data with minimal transformation
-- This model takes the raw additional agreements data and applies basic cleaning

{{ config(
    materialized='table',
    description='Raw additional agreements data with basic cleaning applied'
) }}

select
   id as agreement_id,
   from_parent_id,
   to_parent_id,
   during,
   -- Parse the range string to extract start and end times
   -- Simple approach: split by comma and clean brackets
   case 
     when during like '[%' then
       -- Format: [start,end) - remove opening bracket and extract first part
       trim(both '[]()' from split_part(during, ',', 1))::timestamp
     when during like '(%' then
       -- Format: (start,end] - remove opening parenthesis and extract first part
       trim(both '[]()' from split_part(during, ',', 1))::timestamp
     else
       -- Fallback: extract first timestamp
       trim(both '[]()' from split_part(during, ',', 1))::timestamp
   end as start_time,
   case 
     when during like '%)' then
       -- Format: [start,end) - remove closing parenthesis and extract second part
       trim(both '[]()' from split_part(during, ',', 2))::timestamp
     when during like '%]' then
       -- Format: (start,end] - remove closing bracket and extract second part
       trim(both '[]()' from split_part(during, ',', 2))::timestamp
     else
       -- Fallback: extract second timestamp
       trim(both '[]()' from split_part(during, ',', 2))::timestamp
   end as end_time,
   created_at,
   updated_at,
   current_timestamp as _dbt_loaded_at,
   'bronze' as _dbt_layer
from {{ source('raw_postgres', 'raw_additional_agreements') }}

-- Bronze layer: Raw additional agreements data with minimal transformation
-- This model takes the raw additional agreements data and applies basic cleaning
-- All timestamps are converted to UTC for consistency

{{ config(
    materialized='table',
    description='Raw additional agreements data with basic cleaning applied and converted to UTC'
) }}

select
   id as agreement_id,
   from_parent_id,
   to_parent_id,
   during,
   -- Parse the range string to extract start and end times
   -- Note: Data is already in UTC (has +00:00 offset), so we just parse and cast
   -- Simple approach: split by comma and clean brackets
   case 
     when during like '[%' then
       -- Format: [start,end) - remove opening bracket and extract first part
       CAST(RTRIM(LTRIM(SPLIT(during, ',')[OFFSET(0)],'[]()'),'[]()') as timestamp)
     when during like '(%' then
       -- Format: (start,end] - remove opening parenthesis and extract first part
       CAST(RTRIM(LTRIM(SPLIT(during, ',')[OFFSET(0)],'[]()'),'[]()') as timestamp)
     else
       -- Fallback: extract first timestamp
       CAST(RTRIM(LTRIM(SPLIT(during, ',')[OFFSET(0)],'[]()'),'[]()') as timestamp)
   end as start_time,
   case 
     when during like '%)' then
       -- Format: [start,end) - remove closing parenthesis and extract second part
       CAST(RTRIM(LTRIM(SPLIT(during, ',')[OFFSET(1)],'[]()'),'[]()') as timestamp)
     when during like '%]' then
       -- Format: (start,end] - remove closing bracket and extract second part
       CAST(RTRIM(LTRIM(SPLIT(during, ',')[OFFSET(1)],'[]()'),'[]()') as timestamp)
     else
       -- Fallback: extract second timestamp
       CAST(RTRIM(LTRIM(SPLIT(during, ',')[OFFSET(1)],'[]()'),'[]()') as timestamp)
   end as end_time,
   created_at,
   updated_at,
   current_timestamp as _dbt_loaded_at,
   'bronze' as _dbt_layer
from {{ source('raw_postgres', 'raw_additional_agreements') }}

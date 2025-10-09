-- models/datamarts/daily_additional_agreements_report.sql
{{ config(
    materialized='view'
) }}

{#----------------------------
Vars (with sensible defaults)
-----------------------------#}
{% set start_date = var('start_date', (modules.datetime.date.today() - modules.datetime.timedelta(days=365 * 10)).isoformat()) %}
{% set end_date   = var('end_date',   (modules.datetime.date.today() + modules.datetime.timedelta(days=365 * 10)).isoformat()) %}

with
-- 1) Get detailed additional agreements from staging
additional_agreements_detailed as (
  select * from {{ ref('stg_additional_agreements_detailed') }}
),

-- 2) Get parents from staging
parents as (
  select * from {{ ref('stg_parents') }}
),

-- 3) Join with parent names and calculate derived values
calculated_values as (
  select
    a.day,
    a.reason,
    a.from_parent_id,
    a.to_parent_id,
    p.name as from_parent_name,
    p2.name as to_parent_name,
    a.agreements_count,
    a.agreement_id,
    a.duration_seconds
  from additional_agreements_detailed a
  left join parents p on p.parent_id = a.from_parent_id
  left join parents p2 on p2.parent_id = a.to_parent_id
)

-- 4) Final projection with calculated fields
, final_values as (
select
  day,
  reason,
  from_parent_id,
  to_parent_id,
  from_parent_name,
  to_parent_name,
  agreements_count,
  agreement_id,
  duration_seconds,
  -- convenient time unit views
  ROUND(CAST(duration_seconds AS FLOAT64) / 3600.0, 2) as duration_hours,
  ROUND(CAST(duration_seconds AS FLOAT64) / 86400.0, 2) as duration_days
from calculated_values
order by day, reason
)

select 
  agreement_id,
  from_parent_name,
  to_parent_name,
  min(reason) as reason,
  min(day) as day,
  sum(duration_seconds) as duration_seconds,
  sum(duration_hours) as duration_hours,
  sum(duration_days) as duration_days
from final_values
group by 1,2,3
order by day


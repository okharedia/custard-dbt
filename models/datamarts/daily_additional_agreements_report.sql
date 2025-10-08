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
-- 1) Day spine in timestamps [day_start, day_end)
date_spine as (
  select
    date as day,
    TIMESTAMP(date) as day_start,
    TIMESTAMP(DATE_ADD(date, INTERVAL 1 DAY)) as day_end
  from UNNEST(GENERATE_DATE_ARRAY(
    DATE('{{ start_date }}'),
    DATE_SUB(DATE('{{ end_date }}'), INTERVAL 1 DAY)
  )) as date
),

-- 2) Additional agreements overlap with each day
add_overlap as (
  select
    d.day,
    a.from_parent_id as from_parent_id,
    a.to_parent_id as to_parent_id,
    a.id as agreement_id,
    ad.reason as reason,
    GREATEST(a.start_time, d.day_start) as seg_start,
    LEAST(a.end_time, d.day_end) as seg_end
  from date_spine d
  join {{ source('public', 'additional_agreements') }} a 
    on a.start_time < d.day_end 
    and a.end_time > d.day_start
  join {{ source('public', 'additional_agreements_details') }} ad
    on ad.additional_agreements_id = a.id
),

-- 3) Aggregate per day x reason
agg as (
  select
    day,
    agreement_id,
    reason,
    from_parent_id,
    to_parent_id,
    COUNT(DISTINCT agreement_id) as agreements_count,
    CAST(SUM(GREATEST(0, TIMESTAMP_DIFF(seg_end, seg_start, SECOND))) AS INT64) as duration_seconds
  from add_overlap
  group by 1,2,3,4,5
)

-- 4) Final projection
select
  DATE(day) as day,
  reason,
  from_parent_id,
  to_parent_id,
  p.name as from_parent_name,
  p2.name as to_parent_name,
  agreements_count,
  agreement_id,
  duration_seconds,
  ROUND(CAST(duration_seconds AS FLOAT64) / 3600.0, 2) as duration_hours,
  ROUND(CAST(duration_seconds AS FLOAT64) / 86400.0, 2) as duration_days
from agg
left join {{ source('public', 'parents') }} p
  on p.id = from_parent_id
left join {{ source('public', 'parents') }} p2
  on p2.id = to_parent_id
order by day, reason



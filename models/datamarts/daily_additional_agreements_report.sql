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
-- 1) Day spine in tz-aware timestamps [day_start, day_end)
date_spine as (
  select
    dd::date as day,
    dd::timestamptz as day_start,
    (dd::date + interval '1 day')::timestamptz as day_end,
    tstzrange(dd::timestamptz, (dd::date + interval '1 day')::timestamptz, '[)') as day_range
  from generate_series(
    '{{ start_date }}'::date,
    ('{{ end_date }}'::date - 1),
    interval '1 day'
  ) as dd
),

-- 2) Additional agreements overlap with each day
add_overlap as (
  select
    d.day,
    a.from_parent_id as from_parent_id,
    a.to_parent_id as to_parent_id,
    a.id as agreement_id,
    ad.reason as reason,
    greatest(lower(a.during), d.day_start) as seg_start,
    least(upper(a.during), d.day_end)       as seg_end
  from date_spine d
  join {{ source('public', 'additional_agreements') }} a 
    on a.during && d.day_range
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
    count(distinct agreement_id) as agreements_count,
    sum(greatest(0, extract(epoch from (seg_end - seg_start))))::bigint as duration_seconds
  from add_overlap
  group by 1,2,3,4,5
)

-- 4) Final projection
select
  day::date                             as day,
  reason,
  from_parent_id,
  to_parent_id,
  p.name as from_parent_name,
  p2.name as to_parent_name,
  agreements_count,
  agreement_id,
  duration_seconds,
  round(duration_seconds / 3600.0, 2)   as duration_hours,
  round(duration_seconds / 86400.0, 2)  as duration_days
from agg
left join {{ source('public', 'parents') }} p
  on p.id = from_parent_id
left join {{ source('public', 'parents') }} p2
  on p2.id = to_parent_id
order by day, reason



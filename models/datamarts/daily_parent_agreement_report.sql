-- models/daily_parent_agreement_report.sql
{{ config(
    materialized='view'
) }}

{#----------------------------
Vars (with sensible defaults)
-----------------------------#}
{% set start_date = var('start_date', (modules.datetime.date.today() - modules.datetime.timedelta(days=365 * 10)).isoformat()) %}
{% set end_date   = var('end_date',   (modules.datetime.date.today() + modules.datetime.timedelta(days=365 * 10)).isoformat()) %}
{% set household_id = var('household_id', none) %}

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

-- 2) Parents (optionally restricted to a household)
parents as (
  select p.id as parent_id, p.name, p.household_id
  from {{ source('public', 'parents') }} p
  {% if household_id %}
  where p.household_id = {{ "'" ~ household_id ~ "'" }}::uuid
  {% endif %}
),

-- 3) Base agreement overlap (per parent x day)
base_overlap as (
  select
    d.day,
    b.parent_id,
    greatest(b.start_time, d.day_start) as seg_start,
    least(b.end_time, d.day_end) as seg_end
  from date_spine d
  join {{ source('public', 'base_agreements') }} b
    on b.start_time < d.day_end 
    and b.end_time > d.day_start
),
base_durations as (
  select
    day,
    parent_id,
    sum(greatest(0, CAST(TIMESTAMP_DIFF(seg_end, seg_start, SECOND) AS INT64))) as base_seconds
  from base_overlap
  group by 1,2
),

-- 4) Additional agreements RECEIVED (to_parent) overlap
add_rcv_overlap as (
  select
    d.day,
    a.to_parent_id as parent_id,
    greatest(a.start_time, d.day_start) as seg_start,
    least(a.end_time, d.day_end) as seg_end
  from date_spine d
  join {{ source('public', 'additional_agreements') }} a
    on a.start_time < d.day_end 
    and a.end_time > d.day_start
),
add_rcv as (
  select
    day,
    parent_id,
    sum(greatest(0, CAST(TIMESTAMP_DIFF(seg_end, seg_start, SECOND) AS INT64))) as add_received_seconds
  from add_rcv_overlap
  group by 1,2
),

-- 5) Additional agreements GIVEN (from_parent) overlap
add_gvn_overlap as (
  select
    d.day,
    a.from_parent_id as parent_id,
    greatest(a.start_time, d.day_start) as seg_start,
    least(a.end_time, d.day_end) as seg_end
  from date_spine d
  join {{ source('public', 'additional_agreements') }} a
    on a.start_time < d.day_end 
    and a.end_time > d.day_start
),
add_gvn as (
  select
    day,
    parent_id,
    sum(greatest(0, CAST(TIMESTAMP_DIFF(seg_end, seg_start, SECOND) AS INT64))) as add_given_seconds
  from add_gvn_overlap
  group by 1,2
)

-- 6) Final rollup per parent x day
select
  CAST(d.day AS DATE)                                  as day,
  p.parent_id,
  p.name                                               as parent_name,
  p.household_id,
  coalesce(b.base_seconds, 0)                          as base_seconds,
  coalesce(r.add_received_seconds, 0)                  as additional_received_seconds,
  coalesce(g.add_given_seconds, 0)                     as additional_given_seconds,
  (coalesce(r.add_received_seconds,0) - coalesce(g.add_given_seconds,0)) as additional_net_seconds,
  -- convenient hour views
  round(coalesce(b.base_seconds,0) / 3600.0, 2)        as base_hours,
  round(coalesce(r.add_received_seconds,0) / 3600.0, 2) as additional_received_hours,
  round(coalesce(g.add_given_seconds,0) / 3600.0, 2)    as additional_given_hours,
  round(
    (coalesce(r.add_received_seconds,0) - coalesce(g.add_given_seconds,0)) / 3600.0
  , 2)                                                 as additional_net_hours
from date_spine d
cross join parents p
left join base_durations b on b.day = d.day and b.parent_id = p.parent_id
left join add_rcv        r on r.day = d.day and r.parent_id = p.parent_id
left join add_gvn        g on g.day = d.day and g.parent_id = p.parent_id
order by day, parent_name
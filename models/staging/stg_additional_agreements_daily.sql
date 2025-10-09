-- models/staging/stg_additional_agreements_daily.sql
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['day', 'parent_id'], 'type': 'btree'},
      {'columns': ['parent_id'], 'type': 'btree'},
      {'columns': ['agreement_type'], 'type': 'btree'}
    ]
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

-- 2) Additional agreements RECEIVED (to_parent) overlap
add_rcv_overlap as (
  select
    d.day,
    a.to_parent_id as parent_id,
    'received' as agreement_type,
    greatest(a.start_time, d.day_start) as seg_start,
    least(a.end_time, d.day_end) as seg_end
  from date_spine d
  join {{ source('public', 'additional_agreements') }} a
    on a.start_time < d.day_end 
    and a.end_time > d.day_start
),

-- 3) Additional agreements GIVEN (from_parent) overlap
add_gvn_overlap as (
  select
    d.day,
    a.from_parent_id as parent_id,
    'given' as agreement_type,
    greatest(a.start_time, d.day_start) as seg_start,
    least(a.end_time, d.day_end) as seg_end
  from date_spine d
  join {{ source('public', 'additional_agreements') }} a
    on a.start_time < d.day_end 
    and a.end_time > d.day_start
),

-- 4) Union all additional agreement overlaps
all_additional_overlaps as (
  select * from add_rcv_overlap
  union all
  select * from add_gvn_overlap
)

-- 5) Final aggregation per parent x day x agreement type
select
  CAST(day AS DATE) as day,
  parent_id,
  agreement_type,
  sum(greatest(0, CAST(TIMESTAMP_DIFF(seg_end, seg_start, SECOND) AS INT64))) as seconds
from all_additional_overlaps
group by 1, 2, 3

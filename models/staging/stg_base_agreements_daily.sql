-- models/staging/stg_base_agreements_daily.sql
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['day', 'parent_id'], 'type': 'btree'},
      {'columns': ['parent_id'], 'type': 'btree'}
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

-- 2) Base agreement overlap (per parent x day)
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
)

-- 3) Final aggregation per parent x day
select
  CAST(day AS DATE) as day,
  parent_id,
  sum(greatest(0, CAST(TIMESTAMP_DIFF(seg_end, seg_start, SECOND) AS INT64))) as base_seconds
from base_overlap
group by 1, 2

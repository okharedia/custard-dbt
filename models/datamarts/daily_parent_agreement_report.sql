-- models/datamarts/daily_parent_agreement_report.sql
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

-- 2) Get parents from staging
parents as (
  select * from {{ ref('stg_parents') }}
),

-- 3) Get base agreement durations from staging
base_durations as (
  select * from {{ ref('stg_base_agreements_daily') }}
),

-- 4) Get additional agreement durations from staging and pivot
additional_agreements_pivoted as (
  select
    day,
    parent_id,
    sum(case when agreement_type = 'received' then seconds else 0 end) as add_received_seconds,
    sum(case when agreement_type = 'given' then seconds else 0 end) as add_given_seconds
  from {{ ref('stg_additional_agreements_daily') }}
  group by 1, 2
),

-- 5) Calculate coalesced values once to avoid repetition
calculated_values as (
  select
    CAST(d.day AS DATE) as day,
    p.parent_id,
    p.name as parent_name,
    p.household_id,
    coalesce(b.base_seconds, 0) as base_seconds,
    coalesce(a.add_received_seconds, 0) as additional_received_seconds,
    coalesce(a.add_given_seconds, 0) as additional_given_seconds
  from date_spine d
  cross join parents p
  left join base_durations b on b.day = d.day and b.parent_id = p.parent_id
  left join additional_agreements_pivoted a on a.day = d.day and a.parent_id = p.parent_id
)

-- 6) Final rollup per parent x day with calculated fields
select
  day,
  parent_id,
  parent_name,
  household_id,
  base_seconds,
  additional_received_seconds,
  additional_given_seconds,
  (additional_received_seconds - additional_given_seconds) as additional_net_seconds,
  (base_seconds + (additional_received_seconds - additional_given_seconds)) as net_seconds,
  -- convenient hour views
  round(base_seconds / 3600.0, 2) as base_hours,
  round(additional_received_seconds / 3600.0, 2) as additional_received_hours,
  round(additional_given_seconds / 3600.0, 2) as additional_given_hours,
  round((additional_received_seconds - additional_given_seconds) / 3600.0, 2) as additional_net_hours,
  round((base_seconds + (additional_received_seconds - additional_given_seconds)) / 3600.0, 2) as net_hours,
  -- convenient day views
  round(base_seconds / 86400.0, 2) as base_days,
  round(additional_received_seconds / 86400.0, 2) as additional_received_days,
  round(additional_given_seconds / 86400.0, 2) as additional_given_days,
  round((additional_received_seconds - additional_given_seconds) / 86400.0, 2) as additional_net_days,
  round((base_seconds + (additional_received_seconds - additional_given_seconds)) / 86400.0, 2) as net_days
from calculated_values
order by day, parent_name
-- models/monthly/monthly_parent_agreement_report.sql
{{ config(
    materialized='view'
) }}

with daily as (
  select * from {{ ref('daily_parent_agreement_report') }}
)

select
  date_trunc('month', day)::date as month,
  parent_id,
  parent_name,
  household_id,
  -- seconds
  sum(base_seconds)                          as base_seconds,
  sum(additional_received_seconds)           as additional_received_seconds,
  sum(additional_given_seconds)              as additional_given_seconds,
  sum(additional_net_seconds)                as additional_net_seconds,
  -- days (seconds converted to days)
  round(sum(base_seconds) / 86400.0, 2)                    as base_days,
  round(sum(additional_received_seconds) / 86400.0, 2)     as additional_received_days,
  round(sum(additional_given_seconds) / 86400.0, 2)        as additional_given_days,
  -- hours
  round(sum(base_seconds) / 3600.0, 2)                     as base_hours,
  round(sum(additional_received_seconds) / 3600.0, 2)      as additional_received_hours,
  round(sum(additional_given_seconds) / 3600.0, 2)         as additional_given_hours,
  round(sum(additional_net_seconds) / 3600.0, 2)           as additional_net_hours,
  round(sum(additional_net_seconds) / 86400.0, 2)          as additional_net_days,
  -- totals
  (sum(base_seconds) + sum(additional_received_seconds) - sum(additional_given_seconds)) as total_seconds,
  round((sum(base_seconds) + sum(additional_received_seconds) - sum(additional_given_seconds)) / 3600.0, 2)  as total_hours,
  round((sum(base_seconds) + sum(additional_received_seconds) - sum(additional_given_seconds)) / 86400.0, 2) as total_days,
  -- calendar month totals (length of the month)
  (((date_trunc('month', day))::date + interval '1 month')::date - (date_trunc('month', day))::date)::int as month_total_days,
  ((((date_trunc('month', day))::date + interval '1 month')::date - (date_trunc('month', day))::date) * 24)::int as month_total_hours,
  ((((date_trunc('month', day))::date + interval '1 month')::date - (date_trunc('month', day))::date) * 86400)::int as month_total_seconds,
  -- simplified totals
  (((((date_trunc('month', day))::date + interval '1 month')::date - (date_trunc('month', day))::date) * 86400)::int / 2
    + sum(additional_net_seconds))                                                       as total_simplified_seconds,
  round(
    ((((date_trunc('month', day))::date + interval '1 month')::date - (date_trunc('month', day))::date) * 86400.0) / 2.0
    + sum(additional_net_seconds)
  , 2) / 3600.0                                                                          as total_simplified_hours,
  round(
    ((((date_trunc('month', day))::date + interval '1 month')::date - (date_trunc('month', day))::date) * 86400.0) / 2.0
    + sum(additional_net_seconds)
  , 2) / 86400.0                                                                         as total_simplified_days
from daily
group by 1,2,3,4
order by month, parent_name



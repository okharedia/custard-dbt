-- Gold layer: Daily agreements report
-- This model creates a daily report showing each parent's time with child
-- Combines schedule time and additional time transfers (both pre-split by day)
-- Calculates net time in seconds and hours
-- Ready for business consumption and API integration

{{ config(
    materialized='table',
    description='Daily report of parent-child time including schedule and additional agreements - ready for consumption'
) }}

with date_spine as (
    -- Generate dates based on actual schedule agreements
    select distinct 
        report_date
    from {{ ref('silver_daily_schedule_time') }}
),

all_parents as (
    -- Get all parents for complete reporting
    select distinct
        p.parent_id,
        p.parent_name,
        p.household_id,
        h.household_name
    from {{ ref('bronze_parents') }} p
    join {{ ref('bronze_households') }} h on p.household_id = h.household_id
),

final_report as (
    -- Combine schedule and additional time for each parent-day combination
    select 
        ds.report_date,
        ap.parent_id,
        ap.parent_name,
        ap.household_id,
        ap.household_name,
        -- Schedule time (from silver_daily_schedule_time)
        coalesce(sdt.total_schedule_seconds, 0) as schedule_time_seconds,
        coalesce(sdt.total_schedule_hours, 0) as schedule_time_hours,
        coalesce(sdt.num_schedule_blocks, 0) as num_schedule_blocks,
        -- Additional time transfers (from silver_daily_additional_time)
        coalesce(adt.total_transfer_seconds, 0) as additional_time_seconds,
        coalesce(adt.total_transfer_hours, 0) as additional_time_hours,
        coalesce(adt.num_agreements, 0) as num_additional_agreements,
        adt.transfer_reasons as additional_agreement_reasons,
        -- Net time calculations
        coalesce(sdt.total_schedule_seconds, 0) + coalesce(adt.total_transfer_seconds, 0) as net_time_seconds,
        round((coalesce(sdt.total_schedule_seconds, 0) + coalesce(adt.total_transfer_seconds, 0)) / 3600.0, 2) as net_time_hours
    from date_spine ds
    cross join all_parents ap
    left join {{ ref('silver_daily_schedule_time') }} sdt
        on ds.report_date = sdt.report_date 
        and ap.parent_id = sdt.parent_id
    left join {{ ref('silver_daily_additional_time') }} adt
        on ds.report_date = adt.report_date 
        and ap.parent_id = adt.parent_id
)

select 
    report_date,
    parent_id,
    parent_name,
    household_id,
    household_name,
    -- Schedule time
    schedule_time_seconds,
    schedule_time_hours,
    num_schedule_blocks,
    -- Additional time
    additional_time_seconds,
    additional_time_hours,
    num_additional_agreements,
    additional_agreement_reasons,
    -- Net time
    net_time_seconds,
    net_time_hours,
    -- Metadata
    current_timestamp as _dbt_loaded_at,
    'gold' as _dbt_layer
from final_report
order by report_date desc, household_id, parent_id


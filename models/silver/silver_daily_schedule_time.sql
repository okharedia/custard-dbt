-- Silver layer: Daily schedule time per parent
-- This model splits multi-day schedule agreements across the actual dates they span
-- Ensures each day shows only the hours that actually occur on that day (max 24)

{{ config(
    materialized='table',
    description='Schedule time split by actual date per parent with household information'
) }}

with schedule_events as (
    -- Get schedule events with dates from the silver model
    select 
        schedule_id,
        event_date,
        start_at,
        end_at
    from {{ ref('silver_daily_schedule_dates') }}
),

schedule_events_with_details as (
    -- Join with parent and household information
    select
        se.schedule_id as parent_id,
        p.parent_name,
        p.household_id,
        h.household_name,
        se.event_date as report_date,
        se.start_at,
        se.end_at
    from schedule_events se
    join {{ ref('bronze_parents') }} p on se.schedule_id = p.parent_id
    join {{ ref('bronze_households') }} h on p.household_id = h.household_id
),

daily_time_calculation as (
    -- Calculate actual hours for each date
    -- Handle partial days at start and end of agreement
    select 
        parent_id,
        parent_name,
        household_id,
        household_name,
        report_date::date as report_date,
        start_at,
        end_at,
        -- Calculate the portion of the agreement that falls on this specific date
        -- Start time for this date: max of (agreement start, start of day)
        -- End time for this date: min of (agreement end, end of day)
        extract(epoch from (
            least(
                end_at,
                (report_date::date + interval '1 day')::timestamp
            ) - greatest(
                start_at,
                report_date::date::timestamp
            )
        )) as time_seconds_on_date
    from schedule_events_with_details
),

aggregated_daily_time as (
    -- Aggregate time per parent per day
    -- (handles cases where parent has multiple schedule blocks on same day)
    select 
        parent_id,
        parent_name,
        household_id,
        household_name,
        report_date,
        sum(time_seconds_on_date) as total_schedule_seconds,
        round(sum(time_seconds_on_date) / 3600.0, 2) as total_schedule_hours,
        count(*) as num_schedule_blocks
    from daily_time_calculation
    where time_seconds_on_date > 0  -- Only include dates where there's actual time
    group by parent_id, parent_name, household_id, household_name, report_date
)

select 
    report_date,
    parent_id,
    parent_name,
    household_id,
    household_name,
    total_schedule_seconds,
    total_schedule_hours,
    num_schedule_blocks,
    current_timestamp as _dbt_loaded_at,
    'silver' as _dbt_layer
from aggregated_daily_time
order by report_date desc, household_id, parent_id


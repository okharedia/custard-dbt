-- Silver layer: Daily schedule time per parent
-- This model splits multi-day schedule agreements across the actual dates they span
-- Ensures each day shows only the hours that actually occur on that day (max 24)

{{ config(
    materialized='table',
    description='Schedule time split by actual date per parent with household information'
) }}

with schedule_agreements_with_dates as (
    -- Join schedules with agreements and parent/household info
    select 
        sa.start_at,
        sa.end_at,
        sa.schedule_id,
        s.parent_id,
        p.parent_name,
        p.household_id,
        h.household_name
    from {{ ref('bronze_schedule_agreements') }} sa
    join {{ ref('bronze_schedules') }} s on sa.schedule_id = s.parent_id
    join {{ ref('bronze_parents') }} p on s.parent_id = p.parent_id
    join {{ ref('bronze_households') }} h on p.household_id = h.household_id
    where sa.start_at is not null 
      and sa.end_at is not null
),

date_series as (
    -- Generate a series of dates for each agreement
    -- This handles agreements that span multiple days
    select 
        parent_id,
        parent_name,
        household_id,
        household_name,
        start_at,
        end_at,
        -- Generate all dates from start to end (inclusive)
        unnest(
            generate_series(
                date(start_at),
                date(end_at),
                interval '1 day'
            )
        ) as report_date
    from schedule_agreements_with_dates
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
    from date_series
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


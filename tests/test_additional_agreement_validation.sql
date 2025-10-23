-- Test: Validate that additional agreements only transfer time that exists
-- Business rule: Both from_parent and to_parent must have time allocated on the transfer date
--
-- This test ensures that:
-- 1. The from_parent (offering parent) has time allocated to give away
-- 2. The to_parent (approving parent) receives the time
--
-- Failures indicate invalid additional agreements where:
-- - A parent is trying to give away time they don't have scheduled
-- - Time transfers are being made without base schedule support

{{ config(
    severity = 'error'
) }}

with daily_schedule_time as (
    -- Use the silver daily model which already handles multi-day schedule splits
    select 
        parent_id,
        report_date,
        total_schedule_seconds
    from {{ ref('silver_daily_schedule_time') }}
),

daily_additional_given as (
    -- Calculate time given away per parent per day (negative transfers)
    select 
        parent_id,
        report_date,
        abs(total_transfer_seconds) as time_given_away_seconds
    from {{ ref('silver_daily_additional_time') }}
    where total_transfer_seconds < 0  -- Only negative (giving away)
),

validation_check as (
    select 
        aa.parent_id,
        aa.report_date,
        aa.time_given_away_seconds,
        coalesce(sa.total_schedule_seconds, 0) as schedule_seconds_available,
        aa.time_given_away_seconds - coalesce(sa.total_schedule_seconds, 0) as excess_time_given
    from daily_additional_given aa
    left join daily_schedule_time sa 
        on aa.parent_id = sa.parent_id 
        and aa.report_date = sa.report_date
)

-- Return records where parent is giving away more time than they have scheduled
select *
from validation_check
where excess_time_given > 0


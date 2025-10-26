-- Silver layer: Daily additional time transfers per parent
-- This model splits multi-day additional agreements across the actual dates they span
-- Handles time transfers between parents (negative for giver, positive for receiver)

{{ config(
    materialized='table',
    description='Additional time transfers split by actual date per parent with reasons'
) }}

with additional_agreements_expanded as (
    -- Get additional agreements with parent info and reasons
    select 
        aa.agreement_id,
        aa.from_parent_id,
        aa.to_parent_id,
        aa.start_time,
        aa.end_time,
        coalesce(aad.reason, 'No reason provided') as reason
    from {{ ref('bronze_additional_agreements') }} aa
    left join {{ ref('bronze_additional_agreements_details') }} aad 
        on aa.agreement_id = aad.additional_agreements_id
    where aa.start_time is not null 
      and aa.end_time is not null
),

date_series as (
    select 
        agreement_id,
        from_parent_id,
        to_parent_id,
        start_time,
        end_time,
        reason,
        report_date
    from additional_agreements_expanded,
    unnest(GENERATE_DATE_ARRAY(date(start_time), date(end_time), INTERVAL 1 DAY)) as report_date
),

unioned_parents as (
    -- create a row for each parent involved in the agreement for each day
    select 
        agreement_id,
        from_parent_id as parent_id,
        start_time,
        end_time,
        reason,
        'given' as transfer_type,
        report_date
    from date_series
    union all
    select
        agreement_id,
        to_parent_id as parent_id,
        start_time,
        end_time,
        reason,
        'received' as transfer_type,
        report_date
    from date_series
),

daily_time_calculation as (
    -- Calculate actual hours for each date
    select 
        parent_id,
        report_date,
        agreement_id,
        transfer_type,
        reason,
        -- Calculate the portion of the agreement that falls on this specific date
        DATETIME_DIFF(
            least(
                DATETIME(end_time),
                DATETIME(TIMESTAMP(DATE_ADD(report_date, INTERVAL 1 DAY)))
            ),
            greatest(
                DATETIME(start_time),
                DATETIME(TIMESTAMP(report_date))
            ),
            SECOND
        ) as time_seconds_on_date
    from unioned_parents
),

daily_transfers as (
    -- Apply the correct sign: negative for given, positive for received
    select 
        parent_id,
        report_date,
        agreement_id,
        transfer_type,
        reason,
        case 
            when transfer_type = 'given' then -time_seconds_on_date
            when transfer_type = 'received' then time_seconds_on_date
        end as net_transfer_seconds
    from daily_time_calculation
    where time_seconds_on_date > 0
),

aggregated_daily_transfers as (
    -- Aggregate all transfers per parent per day
    select 
        parent_id,
        report_date,
        sum(net_transfer_seconds) as total_transfer_seconds,
        round(sum(net_transfer_seconds) / 3600.0, 2) as total_transfer_hours,
        count(distinct agreement_id) as num_agreements,
        string_agg(distinct reason, '; ' order by reason) as transfer_reasons
    from daily_transfers
    group by parent_id, report_date
)

select 
    report_date,
    parent_id,
    total_transfer_seconds,
    total_transfer_hours,
    num_agreements,
    transfer_reasons,
    current_timestamp as _dbt_loaded_at,
    'silver' as _dbt_layer
from aggregated_daily_transfers
order by report_date desc, parent_id


import pandas as pd
from datetime import timedelta
from utils.get_events_between import get_events_between, Event

def model(dbt, session):
    dbt.config(
        materialized="table",
        description="Generates a daily series of dates from iCal strings in bronze_schedules.",
        packages=["pandas", "icalendar", "recurring_ical_events", "pydantic"]
    )

    # Read the bronze_schedules table as a pandas DataFrame
    schedules_df = dbt.ref("bronze_schedules").to_pandas()

    all_dates = []

    # Iterate over each row in the DataFrame
    for index, row in schedules_df.iterrows():
        ical_string = row["CALENDAR"]
        schedule_id = row["PARENT_ID"]
        
        # We need to handle potential errors with invalid iCal strings
        try:
            events = get_events_between(ical_string)
            
            for event in events:
                # Generate a date range for each event
                current_date = event.start.date()
                end_date = event.end.date()
                while current_date <= end_date:
                    all_dates.append({
                        "schedule_id": schedule_id,
                        "event_date": current_date,
                        "start_at": event.start,
                        "end_at": event.end
                    })
                    current_date += timedelta(days=1)
        except Exception as e:
            # You can handle logging here if you have a logger configured
            print(f"Skipping schedule_id {schedule_id} due to an error: {e}")
            continue

    # Create a new DataFrame from the list of dates
    if not all_dates:
        # If there are no dates, return an empty DataFrame with the correct columns
        return pd.DataFrame(columns=["schedule_id", "event_date", "start_at", "end_at"])

    result_df = pd.DataFrame(all_dates)

    # Remove duplicate dates for each schedule_id
    result_df.drop_duplicates(inplace=True)
    
    # Sort the DataFrame for consistency
    result_df.sort_values(by=["schedule_id", "event_date"], inplace=True)

    return result_df

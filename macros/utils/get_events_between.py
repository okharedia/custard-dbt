"""Simple and robust iCal processing using ical-library."""

from datetime import datetime, timedelta
from typing import List
from icalendar import Calendar
from recurring_ical_events import of as recurring_ical_events_of
from pydantic import BaseModel

class Event(BaseModel):
    start: datetime
    end: datetime

def get_events_between(
    ical_string: str, range_start: datetime = None, range_end: datetime = None
) -> List[Event]:
    """
    Get all events from an iCal string that occur between a start and end date.
    If start_date or end_date are not provided, they default to 10 years in the past
    and 10 years in the future, respectively.
    """
    calendar = Calendar.from_ical(ical_string)

    if range_start is None:
        range_start = datetime.now() - timedelta(days=365 * 10)

    if range_end is None:
        range_end = datetime.now() + timedelta(days=365 * 10)

    events = recurring_ical_events_of(calendar).between(range_start, range_end)
    date_ranges = []

    for event in events:
        date_ranges.append(Event(start=event.start, end=event.end))

    return date_ranges

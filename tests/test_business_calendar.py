from pathlib import Path

from time_query_service.business_calendar import JsonBusinessCalendar


def test_calendar_loads_cn_fixture_root():
    calendar = JsonBusinessCalendar.from_root(root=Path("config/business_calendar"))

    assert calendar.calendar_version_for_schedule_year(region="CN", schedule_year=2026) is not None

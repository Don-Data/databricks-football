"""Unit tests for the pure business rules in src/football/ingest.py. No API, no Databricks."""

import itertools
import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from football.ingest import (
    DEFAULT_LOOKAHEAD_DAYS,
    DEFAULT_LOOKBACK_DAYS,
    Json,
    LandedRequest,
    check_for_new_competitions,
    date_windows,
    first_unsettled_day,
    land,
    landed_match_requests,
    latest_landed,
    load_tracked,
    matches_range,
    tracked_leagues,
)

TRACKED_FILE = Path(__file__).resolve().parents[1] / "src" / "football" / "tracked_competitions.yml"
TODAY = date(2026, 11, 10)
HISTORY_START = date(2025, 8, 1)


def daily_run(on: date) -> LandedRequest:
    """The request a normal daily run on `on` lands: lookback days back, lookahead ahead."""
    return LandedRequest(
        fetched_on=on,
        date_from=on - timedelta(days=DEFAULT_LOOKBACK_DAYS),
        date_to=on + timedelta(days=DEFAULT_LOOKAHEAD_DAYS + 1),
    )


def full_load(through: date) -> LandedRequest:
    """One big request that settled everything from HISTORY_START up to and including `through`."""
    return LandedRequest(fetched_on=through + timedelta(days=1), date_from=HISTORY_START, date_to=through + timedelta(days=1))


def next_range(landed: list[LandedRequest], history_start: date = HISTORY_START) -> tuple[date, date]:
    first = first_unsettled_day(landed, history_start, TODAY)
    return matches_range(first, history_start, TODAY, DEFAULT_LOOKBACK_DAYS, DEFAULT_LOOKAHEAD_DAYS)


def test_single_day_is_one_window_with_exclusive_date_to() -> None:
    assert list(date_windows(date(2026, 8, 28), date(2026, 8, 28))) == [(date(2026, 8, 28), date(2026, 8, 29))]


def test_long_range_is_split_into_ten_day_windows_without_gaps() -> None:
    windows = list(date_windows(date(2026, 8, 1), date(2026, 8, 25)))
    assert windows == [
        (date(2026, 8, 1), date(2026, 8, 11)),
        (date(2026, 8, 11), date(2026, 8, 21)),
        (date(2026, 8, 21), date(2026, 8, 26)),
    ]
    # each window's exclusive dateTo is the next window's dateFrom: no day is skipped or fetched twice
    for (_, to), (next_from, _) in itertools.pairwise(windows):
        assert to == next_from


def test_daily_default_window_is_a_single_request() -> None:
    today = date(2026, 11, 10)
    start, end = today - timedelta(days=DEFAULT_LOOKBACK_DAYS), today + timedelta(days=DEFAULT_LOOKAHEAD_DAYS)
    assert len(list(date_windows(start, end))) == 1


def test_drift_check_passes_when_every_competition_is_decided() -> None:
    payload: Json = {"competitions": [{"id": 1}, {"id": 2}, {"id": 3}]}
    check_for_new_competitions(payload, tracked=[1, 2], ignored=[3])


def test_drift_check_fails_loudly_on_an_undecided_competition() -> None:
    payload: Json = {"competitions": [{"id": 1}, {"id": 99}]}
    with pytest.raises(ValueError, match=r"\[99\]"):
        check_for_new_competitions(payload, tracked=[1], ignored=[])


def test_tracked_file_has_no_overlap_between_lists_and_a_history_start() -> None:
    tracked = load_tracked(str(TRACKED_FILE))
    assert tracked.competition_ids and not set(tracked.competition_ids) & set(tracked.ignored_competition_ids)
    assert isinstance(tracked.history_start, date)


def test_land_writes_an_envelope_with_the_unchanged_payload(tmp_path: Path) -> None:
    payload: Json = {"matches": [{"id": 1, "score": {"fullTime": {"home": 2, "away": 1}}}]}
    target = Path(land(str(tmp_path), "matches", "/matches", {"dateFrom": "2026-08-28"}, payload))
    envelope: Json = json.loads(target.read_text(encoding="utf-8"))
    assert envelope["response"] == payload
    assert envelope["request"] == {"path": "/matches", "params": {"dateFrom": "2026-08-28"}}
    assert target.relative_to(tmp_path).parts[0] == "matches"


# --- Which days to fetch: derived from the landing, no separate watermark ----


def test_landed_windows_are_read_from_file_names_and_unnamed_files_are_ignored(tmp_path: Path) -> None:
    params = {"dateFrom": "2026-11-07", "dateTo": "2026-11-17"}
    land(str(tmp_path), "matches", "/matches", params, {"matches": []}, name_suffix="2026-11-07_2026-11-17")
    land(str(tmp_path), "matches", "/matches", params, {"matches": []})  # an old file without the window in its name
    [request] = landed_match_requests(str(tmp_path))
    assert (request.date_from, request.date_to) == (date(2026, 11, 7), date(2026, 11, 17))


def test_first_run_is_the_full_load_from_history_start() -> None:
    assert next_range([]) == (HISTORY_START, TODAY + timedelta(days=DEFAULT_LOOKAHEAD_DAYS))


def test_normal_day_fetches_only_lookback_and_lookahead() -> None:
    yesterday = TODAY - timedelta(days=1)
    landed = [full_load(through=TODAY - timedelta(days=5)), daily_run(TODAY - timedelta(days=2)), daily_run(yesterday)]
    assert next_range(landed) == (TODAY - timedelta(days=DEFAULT_LOOKBACK_DAYS), TODAY + timedelta(days=DEFAULT_LOOKAHEAD_DAYS))


def test_after_an_outage_the_run_catches_up_from_the_first_unsettled_day() -> None:
    last_run = TODAY - timedelta(days=8)
    landed = [full_load(through=last_run - timedelta(days=4)), daily_run(last_run)]
    # the last run settled the days before it; its own day was still running when it was fetched
    assert next_range(landed)[0] == last_run


def test_a_gap_in_the_middle_is_fetched_again() -> None:
    # e.g. a full load whose request for the first week of October failed
    landed = [
        LandedRequest(fetched_on=TODAY, date_from=HISTORY_START, date_to=date(2025, 10, 1)),
        LandedRequest(fetched_on=TODAY, date_from=date(2025, 10, 8), date_to=TODAY),
    ]
    assert next_range(landed)[0] == date(2025, 10, 1)


def test_a_day_fetched_before_it_ended_is_not_settled() -> None:
    # fixtures fetched ahead of time say nothing about results
    landed = [full_load(through=TODAY - timedelta(days=10)), LandedRequest(fetched_on=TODAY - timedelta(days=9), date_from=TODAY - timedelta(days=9), date_to=TODAY)]
    assert first_unsettled_day(landed, HISTORY_START, TODAY) == TODAY - timedelta(days=9)


def test_lookback_never_reaches_before_history_start() -> None:
    recent_start = TODAY - timedelta(days=1)
    assert next_range([], history_start=recent_start)[0] == recent_start


def test_standings_are_fetched_for_tracked_leagues_only() -> None:
    payload: Json = {
        "competitions": [
            {"id": 2021, "type": "LEAGUE"},  # tracked league
            {"id": 2001, "type": "CUP"},  # tracked cup: no single table
            {"id": 2000, "type": "CUP"},  # not tracked
            {"id": 2002, "type": "LEAGUE"},  # tracked league
            {"id": 9999, "type": "LEAGUE"},  # a league we don't track
        ]
    }
    assert tracked_leagues(payload, [2021, 2001, 2002]) == [2002, 2021]


def test_latest_landed_is_the_newest_file_by_name(tmp_path: Path) -> None:
    folder = tmp_path / "competitions"
    for day, stamp in [("2026/11/09", "20261109T040000"), ("2026/11/11", "20261111T050000"), ("2026/11/10", "20261110T060000")]:
        (folder / day).mkdir(parents=True)
        (folder / day / f"{stamp}_abcd1234.json").write_text(json.dumps({"fetched_at": stamp}), encoding="utf-8")
    assert latest_landed(str(tmp_path), "competitions") == {"fetched_at": "20261111T050000"}
    with pytest.raises(FileNotFoundError):
        latest_landed(str(tmp_path), "standings")

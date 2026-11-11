"""Land football-data.org API responses in a Unity Catalog volume.

Every API response is stored unchanged, wrapped in a small envelope that records
when it was fetched and what was asked. One JSON file per request. Nothing here
parses or reshapes the payload: Auto Loader picks the files up for bronze, and
any later step can be replayed from the volume.

Which dates to fetch is derived from the landing itself, not from a separate
watermark: every matches file name carries the requested window, so the run can
see which days are already settled. The first run therefore is the full load
from history_start, a run after an outage catches up, and a deleted file is
simply fetched again.

Run as Lakeflow Job tasks (see resources/ingest.job.yml):
  ingest.py competitions --landing-root /Volumes/... --tracked .../tracked_competitions.yml
  ingest.py matches      --landing-root /Volumes/... --tracked ... [--date-from YYYY-MM-DD --date-to YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
import re
import time
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, NamedTuple

import yaml
from dlt.sources.helpers import (
    requests,  # drop-in for requests: retries 429, 5xx and timeouts with backoff
)

BASE_URL = "https://api.football-data.org/v4"
MAX_WINDOW_DAYS = 10  # /matches rejects date ranges longer than 10 days
DEFAULT_LOOKBACK_DAYS = 3  # re-fetch recent days: results and corrections arrive after kick-off
DEFAULT_LOOKAHEAD_DAYS = 6  # also land upcoming fixtures; 3 back + today + 6 ahead = 10 days = one request
SECRET_SCOPE, SECRET_KEY = "football", "fd_token"

Json = dict[str, Any]  # a parsed API response
Params = dict[str, str]  # query parameters as sent to the API

# <fetched at>_<dateFrom>_<dateTo>_<random>.json, with dateFrom/dateTo exactly as sent to the API
MATCHES_FILE_NAME = re.compile(r"^(\d{8})T\d{6}_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_[0-9a-f]{8}\.json$")


# --- API access -------------------------------------------------------------


def api_token() -> str:
    from databricks.sdk.runtime import dbutils  # only exists on Databricks compute

    return dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)


def api_get(path: str, params: Params, token: str) -> requests.Response:
    response = requests.get(f"{BASE_URL}{path}", headers={"X-Auth-Token": token}, params=params, timeout=30)
    response.raise_for_status()  # 4xx other than 429 is our mistake: fail instead of retrying
    _respect_rate_limit(response)
    return response


def _respect_rate_limit(response: requests.Response) -> None:
    """The free tier allows 10 calls per minute and reports the remaining budget.
    When it's used up, wait for the reset instead of provoking a 429."""
    remaining = response.headers.get("X-Requests-Available-Minute")
    reset_seconds = response.headers.get("X-RequestCounter-Reset")
    if remaining is not None and int(remaining) <= 0 and reset_seconds:
        wait = int(reset_seconds) + 1
        print(f"Rate-limit budget used up, waiting {wait}s")
        time.sleep(wait)


# --- Landing ----------------------------------------------------------------


def land(landing_root: str, resource: str, path: str, params: Params, payload: Json, name_suffix: str = "") -> str:
    """Write one envelope file: {fetched_at, request, response}.

    The metadata lives inside the file rather than in a sidecar, because Auto Loader
    reads every JSON file in the folder, and a sidecar would end up in bronze.
    name_suffix puts facts into the file name that later runs need without opening
    the file (for matches: the requested window)."""
    fetched_at = datetime.now(UTC)
    envelope: Json = {
        "fetched_at": fetched_at.isoformat(),
        "request": {"path": path, "params": params},
        "response": payload,
    }
    target_dir = Path(landing_root) / resource / fetched_at.strftime("%Y/%m/%d")
    target_dir.mkdir(parents=True, exist_ok=True)
    middle = f"_{name_suffix}" if name_suffix else ""
    target = target_dir / f"{fetched_at:%Y%m%dT%H%M%S}{middle}_{uuid.uuid4().hex[:8]}.json"
    target.write_text(json.dumps(envelope), encoding="utf-8")
    print(f"Landed {target}")
    return str(target)


class LandedRequest(NamedTuple):
    fetched_on: date
    date_from: date
    date_to: date  # exclusive, as sent to the API


def landed_match_requests(landing_root: str) -> list[LandedRequest]:
    """Read the requested windows from the matches file names. Only lists, never opens files.
    Files whose names don't carry a window are ignored, so their days count as not fetched."""
    found: list[LandedRequest] = []
    for file in (Path(landing_root) / "matches").rglob("*.json"):
        match = MATCHES_FILE_NAME.match(file.name)
        if match:
            fetched_on, date_from, date_to = match.groups()
            found.append(
                LandedRequest(
                    datetime.strptime(fetched_on, "%Y%m%d").date(),
                    date.fromisoformat(date_from),
                    date.fromisoformat(date_to),
                )
            )
    return found


# --- Business rules ---------------------------------------------------------


@dataclass(frozen=True)
class Tracked:
    competition_ids: list[int]
    ignored_competition_ids: list[int]
    history_start: date


def load_tracked(path: str) -> Tracked:
    config: Json = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    history_start = config["history_start"]  # YAML already parses 2025-08-01 as a date
    return Tracked(
        competition_ids=config["competition_ids"],
        ignored_competition_ids=config.get("ignored_competition_ids") or [],
        history_start=history_start if isinstance(history_start, date) else date.fromisoformat(str(history_start)),
    )


def check_for_new_competitions(payload: Json, tracked: list[int], ignored: list[int]) -> None:
    """Fail loudly if the API offers competitions we've never made a decision about.

    A competition is decided if it's tracked (we ingest it) or ignored (we saw it
    and chose not to). Only genuinely unseen ones raise."""
    competitions: list[Json] = payload["competitions"]
    live_ids: set[int] = {c["id"] for c in competitions}
    undecided = live_ids - set(tracked) - set(ignored)
    if undecided:
        raise ValueError(
            f"Competitions that are neither tracked nor ignored: {sorted(undecided)}. "
            "Edit src/football/tracked_competitions.yml deliberately."
        )


def first_unsettled_day(landed: Iterable[LandedRequest], history_start: date, today: date) -> date:
    """The earliest day since history_start whose results we don't have yet.

    A day is settled once a landed request covered it and was fetched after the day
    had ended. A day fetched while it was still running, or only as an upcoming
    fixture, doesn't count. Returns today if everything before today is settled."""
    settled: set[date] = set()
    for request in landed:
        day = request.date_from
        while day < request.date_to and day < request.fetched_on:
            settled.add(day)
            day += timedelta(days=1)
    day = history_start
    while day < today and day in settled:
        day += timedelta(days=1)
    return day


def matches_range(
    first_unsettled: date, history_start: date, today: date, lookback_days: int, lookahead_days: int
) -> tuple[date, date]:
    """The inclusive date range to fetch: from the first unsettled day, but always at
    least lookback_days back for late corrections, and never before history_start."""
    date_from = max(history_start, min(first_unsettled, today - timedelta(days=lookback_days)))
    return date_from, today + timedelta(days=lookahead_days)


def date_windows(start: date, end: date, max_days: int = MAX_WINDOW_DAYS) -> Iterator[tuple[date, date]]:
    """Split the inclusive range [start, end] into API-sized windows.

    Yields (dateFrom, dateTo) as the API wants them. dateTo is exclusive, so it's
    the window's last day plus one: the off-by-one that once dropped matches."""
    window_start = start
    while window_start <= end:
        window_end = min(window_start + timedelta(days=max_days - 1), end)
        yield window_start, window_end + timedelta(days=1)
        window_start = window_end + timedelta(days=1)


# --- Extract steps ----------------------------------------------------------


def extract_competitions(landing_root: str, tracked: Tracked, token: str) -> str:
    payload: Json = api_get("/competitions", {}, token).json()
    landed = land(landing_root, "competitions", "/competitions", {}, payload)
    # Land first, check second: if the check fails, the payload is there to inspect.
    check_for_new_competitions(payload, tracked.competition_ids, tracked.ignored_competition_ids)
    return landed


def extract_matches(landing_root: str, tracked: Tracked, token: str, date_from: date, date_to: date) -> list[str]:
    competitions = ",".join(str(c) for c in tracked.competition_ids)
    landed: list[str] = []
    for window_from, api_date_to in date_windows(date_from, date_to):
        params: Params = {"dateFrom": window_from.isoformat(), "dateTo": api_date_to.isoformat(), "competitions": competitions}
        payload: Json = api_get("/matches", params, token).json()
        suffix = f"{params['dateFrom']}_{params['dateTo']}"
        landed.append(land(landing_root, "matches", "/matches", params, payload, name_suffix=suffix))
    print(f"Landed {len(landed)} file(s) for {date_from} .. {date_to}")
    return landed


# --- Entry point ------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("resource", choices=["competitions", "matches"])
    parser.add_argument("--landing-root", required=True, help="/Volumes/<catalog>/<schema>/<volume>")
    parser.add_argument("--tracked", required=True, help="path to tracked_competitions.yml")
    parser.add_argument("--date-from", default="", help="YYYY-MM-DD; empty = first unsettled day (see matches_range)")
    parser.add_argument("--date-to", default="", help="YYYY-MM-DD; empty = today plus lookahead")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--lookahead-days", type=int, default=DEFAULT_LOOKAHEAD_DAYS)
    args = parser.parse_args(argv)

    tracked = load_tracked(args.tracked)
    token = api_token()
    if args.resource == "competitions":
        extract_competitions(args.landing_root, tracked, token)
        return

    today = datetime.now(UTC).date()
    first_unsettled = first_unsettled_day(landed_match_requests(args.landing_root), tracked.history_start, today)
    auto_from, auto_to = matches_range(first_unsettled, tracked.history_start, today, args.lookback_days, args.lookahead_days)
    print(f"History since {tracked.history_start}, first unsettled day: {first_unsettled}")

    # Explicit dates override the derived range, e.g. to re-fetch a period on purpose.
    date_from = date.fromisoformat(args.date_from) if args.date_from else auto_from
    date_to = date.fromisoformat(args.date_to) if args.date_to else auto_to
    if date_from > date_to:
        parser.error(f"date_from {date_from} is after date_to {date_to}")
    extract_matches(args.landing_root, tracked, token, date_from, date_to)


if __name__ == "__main__":
    main()

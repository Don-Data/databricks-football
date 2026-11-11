"""Compare gold.standings with the league tables football-data.org publishes.

gold.standings computes every table itself, from the matches (src/pipeline/gold/standings.sql).
football-data.org also publishes the tables, at /v4/competitions/{code}/standings. This check
fetches the published table for every competition and season in gold, lands each response like
every other API response (in standings/, which bronze doesn't read), and compares team by team.

Two kinds of difference:
- In the match data: games played, won, drawn, lost or goals differ, or a team is only on one
  side. Our matches and the API's disagree, so the run fails. Right after an ingest run the two
  should agree; a match that finished since then shows up here too.
- In the ranking rules: the same games but other points (a deduction, which isn't in the match
  data), or the same points but another position (a head-to-head tie-break, which standings.sql
  doesn't use). Known simplifications, so they're reported but don't fail the run.

Sometimes the API's own table is the one that's wrong. Those cases are checked by hand and
listed in KNOWN_API_TABLE_ERRORS, with the exact difference: reported, but not fatal.

Run as a Lakeflow Job task (see resources/check_standings.job.yml):
  check_standings.py --landing-root /Volumes/... --standings-table <catalog>.<schema>.standings
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from typing import Any, NamedTuple

from requests import HTTPError

Json = dict[str, Any]

# What the match data decides. Points and position follow from these and the league's rules.
MATCH_STATS = ("played", "won", "drawn", "lost", "goals_for", "goals_against")

MATCH_DATA = "match data"
POINTS_ONLY = "points only"  # same games, other points: a deduction
POSITION_ONLY = "position only"  # same points, other position: a tie-break, or a deduction elsewhere
KNOWN_API_ERROR = "known"  # checked by hand: gold is right, the API's table isn't

# Differences checked by hand, where gold is right and the API's own table isn't. Keyed by
# competition, season and team, with the exact difference: anything else still fails.
KNOWN_API_TABLE_ERRORS: dict[tuple[str, str, int], str] = {
    # Nantes - Toulouse on 2026-05-17 (match 542704) was abandoned at 0-0 after a pitch invasion.
    # The LFP's disciplinary commission confirmed the 0-0 on 2026-05-27, and the API has the
    # match as AWARDED 0-0. Gold counts it as a draw; the API's table leaves it out.
    ("FL1", "2025/26", 511): "played 34 vs 33, drawn 9 vs 8",  # Toulouse FC
    ("FL1", "2025/26", 543): "played 34 vs 33, drawn 9 vs 8",  # FC Nantes
}


class Difference(NamedTuple):
    team_id: int
    team: str
    kind: str
    detail: str


def season_param(season: str) -> str:
    """The API's ?season= is the year the season starts: '2025/26' -> '2025', '2026' -> '2026'."""
    return season[:4]


def published_table(payload: Json) -> list[Json]:
    """The regular-season total table of a /standings response, in gold.standings' column names."""
    for standing in payload["standings"]:
        if standing["type"] == "TOTAL" and standing["stage"] == "REGULAR_SEASON":
            return [
                {
                    "team_id": row["team"]["id"],
                    "team_name": row["team"]["name"],
                    "position": row["position"],
                    "played": row["playedGames"],
                    "won": row["won"],
                    "drawn": row["draw"],
                    "lost": row["lost"],
                    "goals_for": row["goalsFor"],
                    "goals_against": row["goalsAgainst"],
                    "points": row["points"],
                }
                for row in standing["table"]
            ]
    return []


def compare(ours: list[Json], published: list[Json]) -> list[Difference]:
    """One Difference per team that differs, with the most basic cause: if the games differ,
    points and position differ as a consequence, so only the games are reported."""
    ours_by_team = {row["team_id"]: row for row in ours}
    published_by_team = {row["team_id"]: row for row in published}
    differences: list[Difference] = []
    for team_id in sorted(ours_by_team.keys() | published_by_team.keys()):
        mine, theirs = ours_by_team.get(team_id), published_by_team.get(team_id)
        if mine is None:
            name = published_by_team[team_id]["team_name"]
            differences.append(Difference(team_id, name, MATCH_DATA, "only in the API"))
            continue
        if theirs is None:
            differences.append(Difference(team_id, mine["team_name"], MATCH_DATA, "only in gold"))
            continue
        name = mine["team_name"]
        stats = [f"{stat} {mine[stat]} vs {theirs[stat]}" for stat in MATCH_STATS if mine[stat] != theirs[stat]]
        if stats:
            differences.append(Difference(team_id, name, MATCH_DATA, ", ".join(stats)))
        elif mine["points"] != theirs["points"]:
            detail = f"points {mine['points']} vs {theirs['points']}"
            differences.append(Difference(team_id, name, POINTS_ONLY, detail))
        elif mine["position"] != theirs["position"]:
            detail = f"position {mine['position']} vs {theirs['position']}"
            differences.append(Difference(team_id, name, POSITION_ONLY, detail))
    return differences


def mark_known(competition_code: str, season: str, differences: list[Difference]) -> list[Difference]:
    """Mark the differences that KNOWN_API_TABLE_ERRORS explains exactly."""
    return [
        d._replace(kind=KNOWN_API_ERROR)
        if KNOWN_API_TABLE_ERRORS.get((competition_code, season, d.team_id)) == d.detail
        else d
        for d in differences
    ]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--landing-root", required=True, help="/Volumes/<catalog>/<schema>/<volume>")
    parser.add_argument("--standings-table", required=True, help="<catalog>.<schema>.standings")
    args = parser.parse_args(argv)

    # Imported here, not at the top: both exist only on Databricks compute. ingest.py sits next
    # to this file, and the job runs this file as a script, so its folder is on sys.path.
    import ingest
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    tables: dict[tuple[str, str, int], list[Json]] = defaultdict(list)
    for row in spark.read.table(args.standings_table).collect():
        tables[(row["competition_code"], row["season"], row["season_id"])].append(row.asDict())

    token = ingest.api_token()
    failed = 0
    print(f"Ours: {args.standings_table}. Published: /v4/competitions/<code>/standings. Values: ours vs the API's.")
    for (code, season, season_id), ours in sorted(tables.items(), key=lambda item: (item[0][1], item[0][0])):
        path, params = f"/competitions/{code}/standings", {"season": season_param(season)}
        try:
            payload: Json = ingest.api_get(path, params, token).json()
        except HTTPError as error:
            if error.response is None or error.response.status_code not in (403, 404):
                raise
            print(f"{code} {season}: not served by the API ({error.response.status_code})")
            continue
        ingest.land(args.landing_root, "standings", path, params, payload)

        if payload["season"]["id"] != season_id:
            print(f"{code} {season}: the API served season {payload['season']['id']}, not {season_id}")
            failed += 1
            continue
        differences = mark_known(code, season, compare(ours, published_table(payload)))
        print(f"{code} {season}: {len(ours)} teams, {len(differences) or 'no'} difference(s)")
        for difference in differences:
            print(f"    {difference.kind:13}  {difference.team}: {difference.detail}")
        failed += any(d.kind == MATCH_DATA for d in differences)

    if failed:
        raise SystemExit(f"{failed} table(s) disagree with the API in the match data (see above)")


if __name__ == "__main__":
    main()

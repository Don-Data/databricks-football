"""Tests for the standings check: reading the published table and classifying differences."""

from football.check_standings import (
    KNOWN_API_ERROR,
    MATCH_DATA,
    POINTS_ONLY,
    POSITION_ONLY,
    Difference,
    compare,
    mark_known,
    published_table,
    season_param,
)


def team(team_id: int, position: int = 1, won: int = 2, drawn: int = 1, lost: int = 0, **overrides: int) -> dict:
    row = {
        "team_id": team_id,
        "team_name": f"Team {team_id}",
        "position": position,
        "played": won + drawn + lost,
        "won": won,
        "drawn": drawn,
        "lost": lost,
        "goals_for": 5,
        "goals_against": 2,
        "points": 3 * won + drawn,
    }
    return row | overrides


def test_season_param_is_the_start_year():
    assert season_param("2025/26") == "2025"
    assert season_param("2026") == "2026"  # calendar-year seasons, e.g. Brazil


def test_published_table_takes_the_regular_season_total_and_renames():
    api_row = {
        "position": 1,
        "team": {"id": 57, "name": "Arsenal FC"},
        "playedGames": 3,
        "won": 2,
        "draw": 1,
        "lost": 0,
        "points": 7,
        "goalsFor": 5,
        "goalsAgainst": 2,
        "goalDifference": 3,
    }
    payload = {
        "standings": [
            {"stage": "REGULAR_SEASON", "type": "HOME", "table": []},
            {"stage": "REGULAR_SEASON", "type": "TOTAL", "table": [api_row]},
            {"stage": "REGULAR_SEASON", "type": "AWAY", "table": []},
        ]
    }
    assert published_table(payload) == [team(57, team_name="Arsenal FC")]


def test_identical_tables_have_no_differences():
    table = [team(1), team(2, position=2, won=1)]
    assert compare(table, [dict(row) for row in table]) == []


def test_other_games_are_match_data_and_hide_the_points_and_position_they_cause():
    ours = [team(1, won=2)]
    published = [team(1, won=3, position=4)]
    [difference] = compare(ours, published)
    assert difference.kind == MATCH_DATA
    assert difference.detail == "played 3 vs 4, won 2 vs 3"


def test_same_games_but_other_points_is_a_deduction():
    [difference] = compare([team(1)], [team(1, points=4)])
    assert (difference.kind, difference.detail) == (POINTS_ONLY, "points 7 vs 4")


def test_same_points_but_another_position_is_a_tie_break():
    ours = [team(1, position=1), team(2, position=1)]  # tied: standings.sql gives both the same rank
    published = [team(1, position=1), team(2, position=2)]  # the API breaks the tie, e.g. head-to-head
    [difference] = compare(ours, published)
    assert (difference.team, difference.kind, difference.detail) == ("Team 2", POSITION_ONLY, "position 1 vs 2")


def test_a_team_on_one_side_only_is_match_data():
    differences = compare([team(1), team(2)], [team(1), team(3)])
    assert [(d.team, d.kind, d.detail) for d in differences] == [
        ("Team 2", MATCH_DATA, "only in gold"),
        ("Team 3", MATCH_DATA, "only in the API"),
    ]


def test_known_api_errors_are_marked_only_with_the_exact_difference():
    toulouse = Difference(511, "Toulouse FC", MATCH_DATA, "played 34 vs 33, drawn 9 vs 8")
    lorient = Difference(525, "FC Lorient", POSITION_ONLY, "position 10 vs 9")
    [marked, untouched] = mark_known("FL1", "2025/26", [toulouse, lorient])
    assert marked.kind == KNOWN_API_ERROR
    assert untouched == lorient


def test_a_known_team_with_another_difference_still_fails():
    other = Difference(511, "Toulouse FC", MATCH_DATA, "played 35 vs 33, drawn 10 vs 8")
    assert mark_known("FL1", "2025/26", [other]) == [other]
    assert mark_known("FL1", "2026/27", [other._replace(detail="played 34 vs 33, drawn 9 vs 8")])[0].kind == MATCH_DATA

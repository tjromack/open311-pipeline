"""End-to-end: does a 311 payload change fail the dbt build, or pass silently?

Loads the committed replay fixture into a throwaway DuckDB file and runs the
real dbt project against it twice — once as-is, once with `updated_datetime`
renamed the way an upstream schema change would. Without the tripwire test,
the drifted run passes every other test on an empty SLA mart.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

dbt_main = pytest.importorskip("dbt.cli.main")

from classifier.replay_classifier import DEFAULT_FIXTURE  # noqa: E402
from warehouse.duckdb_writer import DuckDBWriter  # noqa: E402
from ingestion.schemas import EnrichedRequest  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO / "dbt_project"
TRIPWIRE = "assert_closed_requests_have_close_time"


def _load(db_path: Path, rename_close_time: bool) -> None:
    records = []
    for line in (REPO / DEFAULT_FIXTURE).read_text(encoding="utf-8").splitlines():
        rec = json.loads(line)
        if rename_close_time:
            rec["raw_payload"]["closed_datetime"] = rec["raw_payload"].pop("updated_datetime")
        records.append(EnrichedRequest.model_validate(rec))
    DuckDBWriter(path=str(db_path)).upsert_batch(records)


def _dbt_build(tmp_path: Path, monkeypatch, db_path: Path):
    profiles = tmp_path / "profiles"
    profiles.mkdir(exist_ok=True)
    shutil.copy(PROJECT_DIR / "profiles.yml.example", profiles / "profiles.yml")
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_TARGET", "local")
    return dbt_main.dbtRunner().invoke([
        "build",
        "--project-dir", str(PROJECT_DIR),
        "--profiles-dir", str(profiles),
        "--target-path", str(tmp_path / "target"),
        "--log-path", str(tmp_path / "logs"),
        "--quiet",
    ])


def _failed_nodes(result) -> set[str]:
    return {r.node.name for r in result.result if str(r.status) in ("fail", "error")}


def test_clean_fixture_builds_green(tmp_path, monkeypatch):
    db = tmp_path / "clean.duckdb"
    _load(db, rename_close_time=False)
    result = _dbt_build(tmp_path, monkeypatch, db)
    assert result.success, _failed_nodes(result)


def test_renamed_close_time_fails_the_build_loudly(tmp_path, monkeypatch):
    db = tmp_path / "drifted.duckdb"
    _load(db, rename_close_time=True)
    result = _dbt_build(tmp_path, monkeypatch, db)
    assert not result.success
    assert _failed_nodes(result) == {TRIPWIRE}

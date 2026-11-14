import pytest

import app.db as db
from app import seed
from app.repositories import canonical as canon_repo
from app.repositories import runs as runs_repo
from app.repositories import sources as sources_repo
from app.services.ingest_service import run_ingest


@pytest.fixture()
def fresh_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "app.db")
    seed.init_db()
    yield


def _canonical(source_id: int, key: str):
    conn = db.connect()
    try:
        return canon_repo.get_by_key(conn, key)
    finally:
        conn.close()


def test_seeded_sources_have_authority_ranks(fresh_db):
    conn = db.connect()
    try:
        sources = {s["name"]: s for s in sources_repo.list_sources(conn)}
    finally:
        conn.close()
    assert sources["Alpha Catalog"]["authority_rank"] == 100
    assert sources["Beta Offers"]["authority_rank"] == 10
    assert sources["Gamma Shelf"]["authority_rank"] == 50


def test_authority_rank_is_editable(fresh_db):
    conn = db.connect()
    try:
        sources_repo.update_authority(conn, 2, 200)
        conn.commit()
        assert sources_repo.get_source(conn, 2)["authority_rank"] == 200
    finally:
        conn.close()


def test_lower_authority_is_isolated_and_canonical_unchanged(fresh_db):
    run_ingest(1, "full")  # Alpha owns SKU-1001 at rank 100
    beta = run_ingest(2, "incremental")  # Beta rank 10 sends a divergent price

    report = beta["report"]
    assert report["parsed_count"] == 3
    assert report["upserted_count"] == 2
    assert report["conflict_count"] == 0
    assert report["authority_isolated_count"] == 1
    assert report["disposition_counts"]["authority_lost"] == 1
    assert all(report["reconcile"].values())

    conn = db.connect()
    try:
        conflicts = runs_repo.list_conflicts(conn, beta["id"])
        assert len(conflicts) == 1
        row = conflicts[0]
        assert row["business_key"] == "SKU-1001"
        assert row["reason"] == "authority_lost"
        assert row["owner_source_id"] == 1
        assert row["owner_authority_rank"] == 100
        assert row["incoming_authority_rank"] == 10
        staging = runs_repo.list_staging(conn, beta["id"])
        assert {r["disposition"] for r in staging} == {"authority_lost", "insert"}
        assert runs_repo.count_staging(conn, beta["id"]) == 3
    finally:
        conn.close()

    owner = _canonical(1, "SKU-1001")
    assert owner["source_id"] == 1
    assert owner["price"] == 129.0


@pytest.mark.parametrize("alpha_mode", ["full", "incremental"])
def test_higher_authority_overrides_and_records_evidence(fresh_db, alpha_mode):
    run_ingest(2, "full")  # Beta owns SKU-1001 at rank 10
    alpha = run_ingest(1, alpha_mode)  # Alpha rank 100 sends divergent price

    report = alpha["report"]
    assert report["parsed_count"] == 3
    assert report["upserted_count"] == 3
    assert report["authority_override_count"] == 1
    assert report["conflict_count"] == 0
    assert report["authority_isolated_count"] == 0
    assert report["canonical_count"] == 5
    assert all(report["reconcile"].values())

    conn = db.connect()
    try:
        changes = runs_repo.list_authority_changes(conn, alpha["id"])
        assert len(changes) == 1
        row = changes[0]
        assert row["business_key"] == "SKU-1001"
        assert row["previous_source_id"] == 2
        assert row["previous_authority_rank"] == 10
        assert row["incoming_authority_rank"] == 100
        assert row["previous"]["price"] == 99.0
        assert row["incoming"]["price"] == 129.0
        staging = runs_repo.list_staging(conn, alpha["id"])
        assert "authority_override" in {r["disposition"] for r in staging}
    finally:
        conn.close()

    owner = _canonical(1, "SKU-1001")
    assert owner["source_id"] == 1
    assert owner["price"] == 129.0
    assert owner["source_name"] == "Alpha Catalog"
    assert owner["source_authority_rank"] == 100


def test_equal_authority_stays_conflict(fresh_db):
    conn = db.connect()
    try:
        sources_repo.update_authority(conn, 2, 100)
        conn.commit()
    finally:
        conn.close()

    run_ingest(1, "full")
    beta = run_ingest(2, "incremental")

    report = beta["report"]
    assert report["conflict_count"] == 1
    assert report["authority_isolated_count"] == 0
    assert report["disposition_counts"]["conflict"] == 1

    conn = db.connect()
    try:
        conflicts = runs_repo.list_conflicts(conn, beta["id"])
        assert conflicts[0]["reason"] == "authority_equal"
        assert conflicts[0]["owner_authority_rank"] == 100
        assert conflicts[0]["incoming_authority_rank"] == 100
    finally:
        conn.close()

    owner = _canonical(1, "SKU-1001")
    assert owner["source_id"] == 1
    assert owner["price"] == 129.0


def test_quarantine_never_touches_canonical_payload(fresh_db):
    """A lost/equal-rank row must leave every canonical column untouched."""

    conn = db.connect()
    try:
        sources_repo.update_authority(conn, 2, 100)
        conn.commit()
    finally:
        conn.close()

    run_ingest(1, "full")
    before = _canonical(1, "SKU-1001")
    beta = run_ingest(2, "incremental")
    after = _canonical(1, "SKU-1001")

    assert after["source_id"] == before["source_id"] == 1
    assert after["price"] == before["price"] == 129.0
    assert after["source_revision"] == before["source_revision"] == 3
    assert after["payload"] == before["payload"]
    assert beta["report"]["disposition_counts"]["conflict"] == 1


def test_report_matches_staging_and_conflict_records(fresh_db):
    """Report counters must be the same numbers the UI reads back."""

    conn = db.connect()
    try:
        sources_repo.update_authority(conn, 2, 100)
        conn.commit()
    finally:
        conn.close()

    run_ingest(1, "full")
    beta = run_ingest(2, "incremental")
    report = beta["report"]

    conn = db.connect()
    try:
        staging = runs_repo.count_staging_by_disposition(conn, beta["id"])
        reasons = runs_repo.count_conflicts_by_reason(conn, beta["id"])
        conflict_rows = runs_repo.count_conflicts(conn, beta["id"])
    finally:
        conn.close()

    assert report["disposition_counts"] == {
        name: staging.get(name, 0)
        for name in report["disposition_counts"]
    }
    assert report["conflict_reason_counts"] == reasons
    assert report["conflict_rows_count"] == conflict_rows
    assert report["conflict_count"] == conflict_rows - reasons.get("authority_lost", 0)
    assert report["authority_isolated_count"] == reasons.get("authority_lost", 0)
    assert all(report["reconcile"].values())


def test_override_refreshes_canonical_owner_source(fresh_db):
    """Higher authority must move canonical ownership, not just the payload."""

    run_ingest(2, "full")  # Beta (rank 10) owns SKU-1001 with price 99
    alpha = run_ingest(1, "full")  # Alpha (rank 100) overrides SKU-1001

    owner = _canonical(1, "SKU-1001")
    assert owner["source_id"] == 1
    assert owner["source_name"] == "Alpha Catalog"
    assert owner["source_authority_rank"] == 100
    assert owner["price"] == 129.0
    assert owner["source_revision"] == 3
    assert alpha["report"]["authority_override_count"] == 1
    # Rows newly inserted by the override run are owned by Alpha as well.
    assert _canonical(1, "SKU-1002")["source_id"] == 1


def test_init_db_upgrades_pre_authority_schema(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "old.db")
    conn = db.connect()
    conn.executescript(
        """
        CREATE TABLE sources(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            format TEXT NOT NULL,
            fixture_path TEXT NOT NULL,
            watermark_ts TEXT,
            lookback_seconds INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        );
        CREATE TABLE runs(
            id INTEGER PRIMARY KEY,
            source_id INTEGER NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            parsed_count INTEGER NOT NULL,
            upserted_count INTEGER NOT NULL,
            conflict_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE conflicts(
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            business_key TEXT NOT NULL,
            existing_payload TEXT NOT NULL,
            incoming_payload TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        INSERT INTO sources(name, format, fixture_path, enabled)
        VALUES ('Alpha Catalog', 'json', 'alpha_catalog.json', 1);
        """
    )
    conn.commit()
    conn.close()

    seed.init_db()

    conn = db.connect()
    try:
        source = sources_repo.get_source(conn, 1)
        assert source["authority_rank"] == 100
        run_columns = {r["name"] for r in conn.execute("PRAGMA table_info(runs)")}
        assert "authority_isolated_count" in run_columns
        conflict_columns = {r["name"] for r in conn.execute("PRAGMA table_info(conflicts)")}
        assert {"owner_source_id", "owner_authority_rank", "incoming_authority_rank"} <= conflict_columns
        tables = {
            r["name"]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert "authority_changes" in tables
    finally:
        conn.close()


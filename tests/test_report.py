"""站日盐度冲突报告测试：去重、冲突、缺测/非有限、快照一致性与失败语义。"""

from __future__ import annotations

import datetime as dt
import pathlib
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq

from app import report as report_mod
from app.storage import blob_path
from conftest import declare_payload, make_parquet

DAY = "2026-11-15"


def _publish(client, helpers, tmp_path, rid, *, salt, station="BUOY-001", day=DAY,
             rows=3, extra=None):
    """生成并发布一个 Paraquet 原件，返回 (bytes, sha256)。"""
    data = make_parquet(tmp_path / f"{rid}.parquet", station_id=station, salt=salt,
                        rows=rows, date=dt.date.fromisoformat(day))
    r = client.put(f"/receipts/{rid}/declaration",
                   json=declare_payload(rid, data, station_id=station, obs_date=day))
    assert r.status_code == 200, r.text
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 200, r.text
    return data, helpers.sha256_of(data)


def _report(client, station="BUOY-001", day=DAY):
    return client.get("/reports/salinity-conflicts",
                       params={"station_id": station, "obs_date": day})


def test_report_lists_conflict_with_sha_and_receipts(client, tmp_path, helpers):
    _, sha_a = _publish(client, helpers, tmp_path, "R-A", salt=0.0)
    _, sha_b = _publish(client, helpers, tmp_path, "R-B", salt=1.0)

    r = _report(client)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["station_id"] == "BUOY-001"
    assert body["obs_date"] == DAY
    assert body["status"] == "conflict"
    assert body["has_conflicts"] is True
    assert body["has_value_conflicts"] is True
    assert body["clean"] is False
    assert body["conflict_count"] == 3
    assert body["source_count"] == 2
    assert body["receipt_count"] == 2

    first = body["conflicts"][0]
    assert first["timestamp_utc"] == "2026-11-15T00:00:00Z"
    assert set(first["sha256s"]) == {sha_a, sha_b}
    assert set(first["receipt_ids"]) == {"R-A", "R-B"}
    values = {v["salinity"]: v for v in first["values"]}
    assert set(values) == {35.0, 36.0}
    assert values[35.0]["sha256s"] == [sha_a]
    assert values[35.0]["receipt_ids"] == ["R-A"]
    assert values[36.0]["sha256s"] == [sha_b]


def test_duplicate_content_across_receipts_counts_once(client, tmp_path, helpers):
    data, sha = _publish(client, helpers, tmp_path, "R-1", salt=0.0)
    for rid in ("R-2", "R-3"):
        assert client.put(f"/receipts/{rid}/declaration",
                          json=declare_payload(rid, data)).status_code == 200
        assert client.post(f"/receipts/{rid}/content", content=data).status_code == 200

    body = _report(client).json()
    assert body["status"] == "ok"
    assert body["has_conflicts"] is False
    assert body["source_count"] == 1
    assert body["receipt_count"] == 3
    assert body["sources"] == [
        {"sha256": sha, "size": len(data), "receipt_ids": ["R-1", "R-2", "R-3"]}
    ]


def test_missing_and_non_finite_are_flagged_not_clean(client, tmp_path, helpers):
    ts = pa.array(
        [dt.datetime(2026, 11, 15, h, tzinfo=dt.timezone.utc) for h in range(4)],
        pa.timestamp("us", "UTC"),
    )
    table = pa.table({
        "station_id": ["BUOY-001"] * 4,
        "timestamp_utc": ts,
        "salinity": pa.array([None, float("nan"), float("inf"), 35.0], pa.float64()),
    })
    p = tmp_path / "anomaly.parquet"
    pq.write_table(table, p)
    data = p.read_bytes()
    rid = "R-ANOM"
    assert client.put(f"/receipts/{rid}/declaration",
                      json=declare_payload(rid, data)).status_code == 200
    assert client.post(f"/receipts/{rid}/content", content=data).status_code == 200

    body = _report(client).json()
    assert body["status"] == "incomplete"
    assert body["has_conflicts"] is True  # 缺测/非有限绝不当作“无冲突”
    assert body["has_value_conflicts"] is False
    assert body["has_anomalies"] is True
    assert body["clean"] is False
    assert body["missing_count"] == 1
    assert body["non_finite_count"] == 2
    assert [a["salinity_state"] for a in body["missing"]] == ["missing"]
    assert body["missing"][0]["missing"] is True
    assert body["missing"][0]["salinity"] is None
    assert body["missing"][0]["timestamp_utc"] == "2026-11-15T00:00:00Z"
    states = {a["salinity_state"] for a in body["non_finite"]}
    assert states == {"nan", "positive_infinity"}
    assert all(a["non_finite"] for a in body["non_finite"])


def test_empty_station_day_reports_no_data_not_clean(client):
    r = _report(client, station="NO-SUCH", day="2026-11-14")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "no_data"
    assert body["source_count"] == 0
    assert body["has_conflicts"] is False


def test_report_reflects_publish_and_delete(client, tmp_path, helpers):
    _publish(client, helpers, tmp_path, "R-P1", salt=0.0)
    assert _report(client).json()["source_count"] == 1

    _, sha2 = _publish(client, helpers, tmp_path, "R-P2", salt=2.0)
    body = _report(client).json()
    assert body["status"] == "conflict"
    assert body["source_count"] == 2
    assert sha2 in body["sources"][1]["sha256"] or sha2 in [
        s["sha256"] for s in body["sources"]
    ]

    assert client.delete("/receipts/R-P2").status_code == 204
    body = _report(client).json()
    assert body["source_count"] == 1
    assert body["status"] == "ok"
    assert client.delete("/receipts/R-P1").status_code == 204
    assert _report(client).json()["status"] == "no_data"


def test_unpublished_receipts_are_excluded(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "u.parquet")
    client.put("/receipts/R-UP/declaration", json=declare_payload("R-UP", data))
    # 只声明、不发布：不得进入报告；也不能算作“完整原件集合”的一部分
    body = _report(client).json()
    assert body["status"] == "no_data"
    assert body["source_count"] == 0


def test_missing_blob_fails_explicitly(client, tmp_path, helpers, settings):
    _, sha = _publish(client, helpers, tmp_path, "R-MISS", salt=0.0)
    pathlib.Path(blob_path(settings, sha)).unlink()

    r = _report(client)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "report_snapshot_incomplete"
    assert "缺失" in r.json()["error"]["message"]


def test_corrupted_blob_fails_explicitly(client, tmp_path, helpers, settings):
    _, sha = _publish(client, helpers, tmp_path, "R-CORRUPT", salt=0.0)
    blob = pathlib.Path(blob_path(settings, sha))
    blob.write_bytes(b"x" * len(blob.read_bytes()))

    r = _report(client)
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "report_source_unreadable"
    assert "摘要不符" in r.json()["error"]["message"]


def test_truncated_blob_fails_explicitly(client, tmp_path, helpers, settings):
    _, sha = _publish(client, helpers, tmp_path, "R-TRUNC", salt=0.0)
    blob = pathlib.Path(blob_path(settings, sha))
    blob.write_bytes(blob.read_bytes()[:100])
    r = _report(client)
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "report_source_unreadable"


def test_report_does_not_change_with_publication_during_slow_scan(
    client, tmp_path, helpers, monkeypatch
):
    """慢扫描与另一实例发布交错：报告只能包含快照时点的原件集合。"""
    _, sha_a = _publish(client, helpers, tmp_path, "R-SNAP-A", salt=0.0)

    scan_started = threading.Event()
    release_scan = threading.Event()
    original_scan = report_mod._scan_one_source

    def slow_scan(source, station_id, obs_date):
        scan_started.set()
        assert release_scan.wait(timeout=15)
        return original_scan(source, station_id, obs_date)

    monkeypatch.setattr(report_mod, "_scan_one_source", slow_scan)

    results: dict = {}

    def run_report():
        results["resp"] = _report(client)

    thread = threading.Thread(target=run_report)
    thread.start()
    assert scan_started.wait(timeout=10)

    # 扫描卡住期间发布第二份不同内容（快照之后的新原件）
    _, sha_b = _publish(client, helpers, tmp_path, "R-SNAP-B", salt=5.0)
    # 删除快照内的原件也不应影响本次报告
    assert client.delete("/receipts/R-SNAP-A").status_code == 204
    release_scan.set()
    thread.join(timeout=20)
    assert not thread.is_alive()

    resp = results["resp"]
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [s["sha256"] for s in body["sources"]] == [sha_a]
    assert sha_b not in [s["sha256"] for s in body["sources"]]
    assert body["status"] == "ok"

    # 新请求重新取快照，反映删除 A / 发布 B 后的变化
    body2 = _report(client).json()
    assert [s["sha256"] for s in body2["sources"]] == [sha_b]
    assert body2["status"] == "ok"


def test_slow_report_scan_does_not_block_receipts(client, tmp_path, helpers,
                                                  monkeypatch):
    _publish(client, helpers, tmp_path, "R-SLOW-1", salt=0.0)
    scan_started = threading.Event()
    release_scan = threading.Event()
    original_scan = report_mod._scan_one_source

    def slow_scan(source, station_id, obs_date):
        scan_started.set()
        assert release_scan.wait(timeout=15)
        return original_scan(source, station_id, obs_date)

    monkeypatch.setattr(report_mod, "_scan_one_source", slow_scan)

    results: dict = {}

    def run_report():
        results["resp"] = _report(client)

    thread = threading.Thread(target=run_report)
    thread.start()
    assert scan_started.wait(timeout=10)

    # 扫描被卡住时，另一份包仍可声明、上传、发布并下载
    data = make_parquet(tmp_path / "other.parquet", salt=9.0)
    t0 = time.monotonic()
    assert client.put("/receipts/R-OTHER/declaration",
                      json=declare_payload("R-OTHER", data)).status_code == 200
    up = client.post("/receipts/R-OTHER/content", content=data)
    elapsed = time.monotonic() - t0
    assert up.status_code == 200, up.text
    assert up.json()["state"] == "PUBLISHED"
    assert elapsed < 5
    assert client.get("/receipts/R-OTHER/download").status_code == 200

    release_scan.set()
    thread.join(timeout=20)
    assert results["resp"].status_code == 200


def test_report_endpoint_validation_and_swagger(client):
    assert client.get("/reports/salinity-conflicts").status_code == 422
    assert client.get(
        "/reports/salinity-conflicts",
        params={"station_id": "BUOY-001", "obs_date": "not-a-date"},
    ).status_code == 422
    spec = client.get("/openapi.json").json()
    assert "/reports/salinity-conflicts" in spec["paths"]
    assert spec["paths"]["/reports/salinity-conflicts"]["get"]["tags"] == ["质控"]


def test_string_and_naive_timestamps_match_same_instant(client, tmp_path, helpers):
    """同一时刻的不同合法编码应归并为同一时刻比对。"""
    string_data = make_parquet(tmp_path / "str.parquet", string_ts=True, salt=0.0)
    naive_data = make_parquet(tmp_path / "naive.parquet", tz=None, salt=1.0)
    for rid, data in (("R-STR", string_data), ("R-NAIVE", naive_data)):
        assert client.put(f"/receipts/{rid}/declaration",
                          json=declare_payload(rid, data)).status_code == 200
        assert client.post(f"/receipts/{rid}/content", content=data).status_code == 200
    body = _report(client).json()
    assert body["status"] == "conflict"
    assert body["conflict_count"] == 24
    assert body["conflicts"][0]["timestamp_utc"] == "2026-11-15T00:00:00Z"


def test_details_are_windowed_with_complete_totals(client, tmp_path, helpers):
    import io

    # 同一站号两份不同内容，逐秒 1200 个不同时刻
    station = "BUOY-W"

    def publish(rid, salt):
        count = 1200
        start = dt.datetime(2026, 11, 15, tzinfo=dt.timezone.utc)
        ts = pa.array([start + dt.timedelta(seconds=i) for i in range(count)],
                      pa.timestamp("us", "UTC"))
        table = pa.table({
            "station_id": [station] * count,
            "timestamp_utc": ts,
            "salinity": pa.array([salt] * count, pa.float64()),
        })
        sink = io.BytesIO()
        pq.write_table(table, sink)
        data = sink.getvalue()
        assert client.put(f"/receipts/{rid}/declaration", json={
            "receipt_id": rid, "station_id": station, "obs_date": DAY,
            "size": len(data), "sha256": helpers.sha256_of(data),
        }).status_code == 200
        assert client.post(f"/receipts/{rid}/content", content=data).status_code == 200

    publish("R-W1", 35.0)
    publish("R-W2", 40.0)

    full = client.get("/reports/salinity-conflicts",
                      params={"station_id": station, "obs_date": DAY}).json()
    assert full["conflict_count"] == 1200
    assert full["conflicts_returned"] == 1000
    assert full["conflicts_truncated"] is True
    assert len(full["conflicts"]) == 1000

    page = client.get(
        "/reports/salinity-conflicts",
        params={"station_id": station, "obs_date": DAY, "offset": 1000,
                "limit": 500},
    ).json()
    assert page["conflict_count"] == 1200  # 总数仍然是完整的
    assert page["detail_offset"] == 1000
    assert page["conflicts_returned"] == 200
    assert page["conflicts_truncated"] is False
    assert page["conflicts"][0]["timestamp_utc"] == "2026-11-15T00:16:40Z"


def _publish_table(client, helpers, rid, table, station="BUOY-001", day=DAY):
    import io

    import pyarrow.parquet as pq

    sink = io.BytesIO()
    pq.write_table(table, sink)
    data = sink.getvalue()
    assert client.put(f"/receipts/{rid}/declaration", json={
        "receipt_id": rid, "station_id": station, "obs_date": day,
        "size": len(data), "sha256": helpers.sha256_of(data),
    }).status_code == 200
    assert client.post(f"/receipts/{rid}/content", content=data).status_code == 200


def test_date32_and_offset_string_timestamps_compare_same_instant(
    client, helpers
):
    """date32 与带偏移的 ISO 字符串都必须归一到同一 UTC 时刻。"""
    date_table = pa.table({
        "station_id": ["BUOY-001", "BUOY-001"],
        "timestamp_utc": pa.array([dt.date(2026, 11, 15)] * 2, pa.date32()),
        "salinity": pa.array([35.0, 36.0], pa.float64()),
    })
    string_table = pa.table({
        "station_id": ["BUOY-001"],
        "timestamp_utc": pa.array(["2026-11-15T08:00:00+08:00"]),
        "salinity": pa.array([37.0], pa.float64()),
    })
    _publish_table(client, helpers, "R-D32", date_table)
    _publish_table(client, helpers, "R-OFF", string_table)

    body = _report(client).json()
    assert body["status"] == "conflict"
    assert body["conflict_count"] == 1
    assert body["conflicts"][0]["timestamp_utc"] == "2026-11-15T00:00:00Z"
    assert [v["salinity"] for v in body["conflicts"][0]["values"]] == [
        35.0, 36.0, 37.0,
    ]


def test_microsecond_precision_kept_and_negative_zero_merged(client, helpers):
    ts = pa.array([
        "2026-11-15T00:00:00.123456Z",
        "2026-11-15T00:00:00.123456Z",
        "2026-11-15T00:00:00.123456Z",
        "2026-11-15T00:00:00.123457Z",
    ])
    table = pa.table({
        "station_id": ["BUOY-001"] * 4,
        "timestamp_utc": ts,
        "salinity": pa.array([0.0, -0.0, 35.0, 35.0], pa.float64()),
    })
    _publish_table(client, helpers, "R-NS", table)
    body = _report(client).json()
    # 只有 .123456 一秒内存在不同取值；.123457 单独不构成冲突
    assert body["conflict_count"] == 1
    assert body["conflicts"][0]["timestamp_utc"] == (
        "2026-11-15T00:00:00.123456Z"
    )
    # 0.0 / -0.0 是同一取值，不应被当作冲突的两个值
    assert [v["salinity"] for v in body["conflicts"][0]["values"]] == [0.0, 35.0]

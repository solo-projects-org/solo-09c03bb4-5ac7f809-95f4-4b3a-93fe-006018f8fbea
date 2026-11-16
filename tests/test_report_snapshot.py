"""锁定接口从收到请求到开始扫描之间的行为。

这里用「快照私有目录里的文件不可读」来证明快照不可变：只要 _scan_one_source
看到的是快照私有路径，就说明扫描不是直接读原始 blob（原始 blob 可能在扫描
期间已被删除或改写）。
"""

from __future__ import annotations

import datetime as dt
import pathlib

from app.storage import blob_path
from conftest import declare_payload, make_parquet


def _publish(client, tmp_path, rid, *, salt, day="2026-11-15"):
    data = make_parquet(tmp_path / f"{rid}.parquet", salt=salt,
                        date=dt.date.fromisoformat(day))
    assert client.put(f"/receipts/{rid}/declaration",
                      json=declare_payload(rid, data, obs_date=day)).status_code == 200
    assert client.post(f"/receipts/{rid}/content", content=data).status_code == 200
    return data


def _report(client, day="2026-11-15"):
    return client.get("/reports/salinity-conflicts",
                       params={"station_id": "BUOY-001", "obs_date": day})


def test_snapshot_isolated_from_blob_replacement(client, tmp_path, helpers, settings):
    """快照后原始 blob 被删除并以新文件顶替：报告仍基于快照内容。"""
    import threading

    from app import report as report_mod

    _publish(client, tmp_path, "R-I1", salt=0.0)
    _publish(client, tmp_path, "R-I2", salt=1.0)

    # 在快照建立之后、扫描开始之前删除并顶替原始 blob（新 inode）
    original_scan = report_mod._scan_one_source
    done = threading.Event()

    def scan_after_replacing_originals(source, station_id, obs_date):
        for src in pathlib.Path(settings.blob_dir).rglob("*"):
            if src.is_file():
                src.unlink()
                src.write_bytes(b"replaced-after-snapshot")
        done.set()
        return original_scan(source, station_id, obs_date)

    report_mod._scan_one_source = scan_after_replacing_originals
    try:
        resp = _report(client)
    finally:
        report_mod._scan_one_source = original_scan
    assert done.is_set()
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "conflict"
    assert resp.json()["source_count"] == 2

    # 新请求重新取快照：此时原始 blob 已损坏，必须明确失败而不是给结论
    again = _report(client)
    assert again.status_code == 500
    assert again.json()["error"]["code"] == "report_source_unreadable"


def test_snapshot_files_live_in_private_directory_not_blob_dir(
    client, tmp_path, helpers, settings
):
    from app import report as report_mod

    _publish(client, tmp_path, "R-I3", salt=0.0)
    seen: list[str] = []
    original_scan = report_mod._scan_one_source

    def capture(source, station_id, obs_date):
        seen.append(source.path)
        return original_scan(source, station_id, obs_date)

    report_mod._scan_one_source = capture
    try:
        assert _report(client).status_code == 200
    finally:
        report_mod._scan_one_source = original_scan
    assert len(seen) == 1
    assert settings.blob_dir not in seen[0]
    assert settings.lock_dir in seen[0]


def test_report_scoped_to_station_and_day(client, tmp_path, helpers):
    _publish(client, tmp_path, "R-D1", salt=0.0)
    other_day = make_parquet(tmp_path / "d2.parquet", salt=4.0,
                             date=dt.date(2026, 11, 16))
    assert client.put("/receipts/R-D2/declaration",
                      json=declare_payload("R-D2", other_day,
                                           obs_date="2026-11-16")).status_code == 200
    assert client.post("/receipts/R-D2/content", content=other_day).status_code == 200

    assert _report(client).json()["source_count"] == 1
    assert _report(client, day="2026-11-16").json()["source_count"] == 1


def test_report_after_reconcile_drops_deleted_receipt(client, tmp_path, helpers):
    _publish(client, tmp_path, "R-RC1", salt=0.0)
    _publish(client, tmp_path, "R-RC2", salt=2.0)
    assert _report(client).json()["status"] == "conflict"
    assert client.delete("/receipts/R-RC2").status_code == 204
    assert client.post("/admin/reconcile").status_code == 200
    body = _report(client).json()
    assert body["status"] == "ok"
    assert body["source_count"] == 1


def test_blob_path_helper_matches_publish_layout(client, tmp_path, helpers, settings):
    data = _publish(client, tmp_path, "R-BP", salt=0.0)
    sha = helpers.sha256_of(data)
    assert pathlib.Path(blob_path(settings, sha)).is_file()


def test_report_snapshot_dirs_do_not_leak(client, tmp_path, helpers, settings):
    import os

    _publish(client, tmp_path, "R-LEAK", salt=0.0)
    for _ in range(3):
        assert _report(client).status_code == 200
    leftovers = [n for n in os.listdir(settings.lock_dir)
                 if n.startswith("obs-report-")]
    assert leftovers == []


def test_reconcile_removes_stale_report_snapshot(client, tmp_path, helpers, settings):
    import os
    import time

    _publish(client, tmp_path, "R-STALE", salt=0.0)
    stale = pathlib.Path(settings.lock_dir) / "obs-report-crashed"
    stale.mkdir()
    (stale / "scan.lock").write_bytes(b"")
    old = time.time() - 3600
    os.utime(stale, (old, old))

    body = client.post("/admin/reconcile").json()
    assert body["removed_report_snapshots"] >= 1
    assert not stale.exists()


def test_active_report_snapshot_not_removed_by_reconcile(
    client, tmp_path, helpers, settings
):
    """对账不得回收仍在扫描的报告快照目录（scan.lock 由活动报告持有）。"""
    import os

    from app import report as report_mod

    _publish(client, tmp_path, "R-ACTIVE", salt=0.0)
    seen: list[pathlib.Path] = []
    original_scan = report_mod._scan_one_source

    def scan_and_reconcile(source, station_id, obs_date):
        path = pathlib.Path(source.path)
        seen.append(path)
        os.utime(path.parent, (0, 0))  # 让目录看起来「陈旧」
        assert client.post("/admin/reconcile").status_code == 200
        assert path.exists(), "活动报告的快照被对账误删"
        return original_scan(source, station_id, obs_date)

    report_mod._scan_one_source = scan_and_reconcile
    try:
        assert _report(client).status_code == 200
    finally:
        report_mod._scan_one_source = original_scan
    assert seen and pathlib.Path(seen[0].parent).exists() is False

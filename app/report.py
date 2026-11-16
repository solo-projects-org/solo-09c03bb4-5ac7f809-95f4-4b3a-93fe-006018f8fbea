"""站日盐度冲突报告（质控）。

按「站号 + UTC 观测日期」读取**已发布**的 Parquet 原件，对同一 UTC
时刻的盐度做跨原件比对：

- 同一内容（原件 SHA-256 相同）无论被多少个接收号重送，只作为一份原件
  参与比对；报告在该原件名下列出全部对应接收号；
- 同一 UTC 时刻出现两个及以上不同的有限盐度 → 冲突明细（盐度值 +
  原件 SHA-256 + 对应接收号）；
- 盐度为 NULL（缺测）或 NaN/±Inf（非有限）→ 单列明确标出，且该站日
  不会被判定为「无冲突」；
- 报告基于一个**固定时点的快照**：快照创建时在全局锁内读取已发布接收
  集合并把原件硬链接到请求私有目录，随后释放锁、在报告专用线程池里
  慢扫描。发布/删除/对账与慢扫描交错时，报告不会拼入另一时点的原件；
  下一次查询重新取快照，自然反映发布或删除后的变化；
- 任一原件缺失、无法完整读取、摘要不符或内容不合法 → 明确失败，绝不
  跳过该原件后给出「无冲突」。

报告扫描使用独立线程池，不与接收流程共用 I/O 线程池，因此慢扫描不会
拖住其他包的接收。
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import os
import re
import shutil
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import Settings
from .db import Database
from .errors import ReportSourceUnreadable, ReportSnapshotIncomplete
from .storage import FileLock, blob_path, global_lock, hardlink_or_copy

REQUIRED_COLUMNS = ("station_id", "timestamp_utc", "salinity")

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)

# 单批读取行数：控制扫描内存峰值，不一次性把整包读进内存
SCAN_BATCH_ROWS = 64 * 1024

# 报告扫描专用线程池：与接收的 I/O 池隔离，避免慢扫描占满接收线程
_SCAN_WORKERS = 2
_scan_pool: ThreadPoolExecutor | None = None

# 缺测/非有限盐度的明确状态标记
FINITE = "finite"
MISSING = "missing"
NAN = "nan"
POSITIVE_INFINITY = "positive_infinity"
NEGATIVE_INFINITY = "negative_infinity"


def _get_scan_pool() -> ThreadPoolExecutor:
    global _scan_pool
    if _scan_pool is None:
        _scan_pool = ThreadPoolExecutor(
            max_workers=_SCAN_WORKERS, thread_name_prefix="obs-report"
        )
    return _scan_pool


def shutdown_scan_pool() -> None:
    """关闭报告扫描线程池（优雅停机/测试隔离）。"""
    global _scan_pool
    if _scan_pool is not None:
        _scan_pool.shutdown(wait=True)
        _scan_pool = None


async def offload_scan(func, *args, **kwargs):
    """把报告工作放到专用线程池，避免占用接收用的 I/O 线程池。"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_get_scan_pool(), partial(func, *args, **kwargs))


# ---------------------------------------------------------------------------
# 快照
# ---------------------------------------------------------------------------


@dataclass
class SourceFile:
    """快照中一份去重后的原件。"""

    sha256: str
    size: int
    path: str
    receipt_ids: list[str] = field(default_factory=list)


@dataclass
class Snapshot:
    station_id: str
    obs_date: dt.date
    sources: list[SourceFile]
    directory: str
    lock: FileLock

    def cleanup(self) -> None:
        # 先释放 scan.lock 再删目录：对账只在拿到同一把锁后才回收陈旧目录，
        # 因此活动报告不会被误删。
        self.lock.release()
        shutil.rmtree(self.directory, ignore_errors=True)


def _create_snapshot_sync(
    settings: Settings, db: Database, station_id: str, obs_date: dt.date
) -> Snapshot:
    """固定同一时点的已发布原件集合，并私有化到本次请求的临时目录。

    发布、删除、对账都会持有全局锁，因此快照读取期间不会插入这些变更。
    锁释放后原件即使被删除/回收，私有目录里的硬链接仍指向同一 inode，
    慢扫描看到的始终是快照那一刻的完整内容。
    """
    directory = tempfile.mkdtemp(prefix="obs-report-", dir=settings.lock_dir)
    # 目录归属锁：清理端通过对账在非阻塞拿到同一把锁后回收崩溃残留，
    # 活动报告持有期间不会被删。
    lock = FileLock(os.path.join(directory, "scan.lock"))
    if not lock.acquire(blocking=False):  # pragma: no cover - 私有新目录
        shutil.rmtree(directory, ignore_errors=True)
        raise ReportSnapshotIncomplete("无法为报告快照建立隔离锁，请稍后重试")
    date_text = obs_date.isoformat()
    try:
        with global_lock(settings):
            rows = db.query_all(
                "SELECT receipt_id, blob_sha256, declared_size FROM receipts "
                "WHERE station_id = ? AND obs_date = ? AND state = 'PUBLISHED' "
                "ORDER BY receipt_id",
                (station_id, date_text),
            )
            grouped: dict[str, list[str]] = defaultdict(list)
            sizes: dict[str, int] = {}
            for row in rows:
                sha = row["blob_sha256"]
                if not sha:
                    raise ReportSnapshotIncomplete(
                        f"接收 {row['receipt_id']} 已发布但缺少原件摘要，"
                        "无法生成完整报告"
                    )
                if not _SHA_RE.match(sha):
                    raise ReportSourceUnreadable(
                        f"接收 {row['receipt_id']} 的原件摘要 {sha!r} 不是合法 SHA-256"
                    )
                grouped[sha].append(row["receipt_id"])
                sizes.setdefault(sha, int(row["declared_size"]))

            sources: list[SourceFile] = []
            for sha in sorted(grouped):
                src = blob_path(settings, sha)
                if not os.path.isfile(src):
                    raise ReportSnapshotIncomplete(
                        f"站日 {station_id}/{date_text} 的原件 {sha}"
                        f"（接收 {', '.join(grouped[sha])}）在快照时缺失，"
                        "拒绝生成不完整报告"
                    )
                dst = os.path.join(directory, sha)
                # 硬链接到请求私有目录；跨卷时退化为拷贝。快照后原件被删
                # 不影响本次扫描。
                hardlink_or_copy(src, dst)
                sources.append(
                    SourceFile(
                        sha256=sha,
                        size=sizes[sha],
                        path=dst,
                        receipt_ids=sorted(grouped[sha]),
                    )
                )
        return Snapshot(
            station_id=station_id,
            obs_date=obs_date,
            sources=sources,
            directory=directory,
            lock=lock,
        )
    except BaseException:
        lock.release()
        shutil.rmtree(directory, ignore_errors=True)
        raise


# ---------------------------------------------------------------------------
# 单原件扫描
# ---------------------------------------------------------------------------


@dataclass
class _FileScan:
    """单个原件的扫描结果（已就地压缩，避免保留整包原始行）。

    - `finite`：该原件内去重后的 (ts_us, value) 有限观测；
    - `anomalies`：该原件内去重后的 (ts_us, code) 缺测/非有限观测。

    逐批次聚合后只保留紧凑的 Arrow 表，跨原件比对时再做一次合并聚合，
    因此内存随「不同取值/时刻数」增长，而不是随完整行集翻倍。
    """

    sha256: str
    finite: list[pa.Table] = field(default_factory=list)
    anomalies: list[pa.Table] = field(default_factory=list)


def _sha256_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(8 * 1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _timestamp_us(column: pa.ChunkedArray) -> pa.Array:
    """把 timestamp_utc 归一化为 UTC 微秒整型（Arrow 内完成，不落 Python）。

    支持 timestamp（带/不带时区）、date、ISO-8601 字符串；其余类型明确失败。
    无时区 timestamp 与 date 按 UTC 解释，与入湖校验规则一致。
    选用微秒（而非纳秒）作为统一口径：微秒 int64 可覆盖 0001..9999 全部
    现代日期，不会因纳秒范围（1677..2262）把合法原件误判为不可读。
    """
    t = column.type
    try:
        if pa.types.is_timestamp(t):
            if t.tz is None:
                normalized = pc.assume_timezone(column, "UTC")
            else:
                normalized = pc.cast(column, pa.timestamp(t.unit, "UTC"))
            # 统一降到微秒：us 的 int64 覆盖完整日期范围，且严格转换能在
            # 超出可表示范围时明确失败，而不是静默回绕成错误时刻。
            return pc.cast(
                pc.cast(normalized, pa.timestamp("us", "UTC")), pa.int64()
            )
        if pa.types.is_date32(t):
            return pc.cast(
                pc.cast(column, pa.timestamp("us", "UTC")), pa.int64()
            )
        if pa.types.is_date64(t):
            ms = pc.cast(column, pa.int64())
            return pc.multiply_checked(ms, pa.scalar(1_000, pa.int64()))
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            # ISO-8601 字符串：必须带 Z 或 ±HH:MM 偏移，cast 后统一为 UTC
            return pc.cast(
                pc.cast(column, pa.timestamp("us", "UTC")), pa.int64()
            )
        raise ReportSourceUnreadable(
            f"timestamp_utc 列类型 {t} 不受支持，需为 timestamp/date/ISO8601 字符串"
        )
    except ReportSourceUnreadable:
        raise
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        raise ReportSourceUnreadable(
            f"timestamp_utc 列无法按 UTC 时刻解析：{exc}"
        ) from exc


_US_PER_DAY = 86_400 * 1_000_000
_EPOCH_DAY = dt.date(1970, 1, 1).toordinal()

# 盐度状态编码：0=有限值，1..4=缺测/非有限（用于 Arrow 侧聚合）
_CODE_FINITE = 0
_CODE_MISSING = 1
_CODE_NAN = 2
_CODE_POS_INF = 3
_CODE_NEG_INF = 4
_CODE_TO_STATE = {
    _CODE_MISSING: MISSING,
    _CODE_NAN: NAN,
    _CODE_POS_INF: POSITIVE_INFINITY,
    _CODE_NEG_INF: NEGATIVE_INFINITY,
}


def _utc_days(us: pa.Array) -> pa.Array:
    """UTC 微秒 -> 自 1970-01-01 起的日期序数（对负时刻做向下取整修正）。"""
    q = pc.divide(us, pa.scalar(_US_PER_DAY, pa.int64()))
    rem = pc.subtract(us, pc.multiply_checked(
        q, pa.scalar(_US_PER_DAY, pa.int64())
    ))
    negative = pc.less(rem, pa.scalar(0, pa.int64()))
    return pc.subtract(q, pc.cast(negative, pa.int64()))


def _iso_utc(us: int) -> str:
    """UTC 微秒 -> 稳定 ISO-8601 文本（整秒不带小数）。"""
    seconds, fraction = divmod(us, 1_000_000)
    base = _EPOCH + dt.timedelta(seconds=seconds)
    head = base.strftime("%Y-%m-%dT%H:%M:%S")
    if fraction:
        return f"{head}.{fraction:06d}".rstrip("0") + "Z"
    return head + "Z"


def _classify_column(sal: pa.ChunkedArray) -> pa.Array:
    """把盐度列映射为状态码（int8），有限值统一把 -0.0 归一为 0.0。"""
    nan = pc.fill_null(pc.is_nan(sal), False)
    pos_inf = pc.fill_null(pc.equal(sal, pa.scalar(float("inf"))), False)
    neg_inf = pc.fill_null(pc.equal(sal, pa.scalar(float("-inf"))), False)
    code = pc.if_else(
        pc.fill_null(pc.is_null(sal), False),
        pa.scalar(_CODE_MISSING, pa.int8()),
        pa.scalar(_CODE_FINITE, pa.int8()),
    )
    code = pc.if_else(nan, pa.scalar(_CODE_NAN, pa.int8()), code)
    code = pc.if_else(pos_inf, pa.scalar(_CODE_POS_INF, pa.int8()), code)
    return pc.if_else(neg_inf, pa.scalar(_CODE_NEG_INF, pa.int8()), code)


def _scan_one_source(source: SourceFile, station_id: str,
                     obs_date: dt.date) -> _FileScan:
    """完整读取一份原件并抽取三列；任何不完整/不一致都明确失败。"""
    path = source.path
    try:
        actual = _sha256_file(path)
    except OSError as exc:
        raise ReportSourceUnreadable(f"原件 {source.sha256} 无法读取：{exc}") from exc
    if actual != source.sha256:
        raise ReportSourceUnreadable(
            f"原件 {source.sha256} 内容摘要不符（实际 {actual}），"
            "拒绝用损坏的原件生成报告"
        )

    try:
        parquet = pq.ParquetFile(path)
    except (pa.ArrowInvalid, pa.ArrowIOError, OSError) as exc:
        raise ReportSourceUnreadable(
            f"原件 {source.sha256} 无法按 Parquet 读取：{exc}"
        ) from exc

    names = parquet.schema_arrow.names
    missing = [c for c in REQUIRED_COLUMNS if c not in names]
    if missing:
        raise ReportSourceUnreadable(
            f"原件 {source.sha256} 缺少必需列：{', '.join(missing)}；实际列为 {names}"
        )
    if parquet.metadata.num_rows == 0:
        raise ReportSourceUnreadable(f"原件 {source.sha256} 为空表，无法生成报告")

    scan = _FileScan(sha256=source.sha256)
    expected_station = pa.scalar(station_id)
    expected_day = obs_date.toordinal()
    try:
        for batch in parquet.iter_batches(
            batch_size=SCAN_BATCH_ROWS, columns=list(REQUIRED_COLUMNS),
            use_threads=False,
        ):
            table = pa.Table.from_batches([batch])

            station_col = table.column("station_id")
            if not (
                pa.types.is_string(station_col.type)
                or pa.types.is_large_string(station_col.type)
            ):
                raise ReportSourceUnreadable(
                    f"原件 {source.sha256} 的 station_id 列必须是字符串"
                )
            station_neq = pc.fill_null(
                pc.not_equal(station_col, expected_station), True
            )
            if pc.any(station_neq).as_py():
                raise ReportSourceUnreadable(
                    f"原件 {source.sha256} 含与查询站号 {station_id!r} 不符的行，"
                    "拒绝生成混杂报告"
                )

            us = _timestamp_us(table.column("timestamp_utc"))
            if pc.any(pc.is_null(us)).as_py():
                raise ReportSourceUnreadable(
                    f"原件 {source.sha256} 存在空的 UTC 时刻行，无法参与时刻比对"
                )
            # 用归一化后的 UTC 时刻取日期序数，逐行核对查询日期
            days = pc.add(_utc_days(us), pa.scalar(_EPOCH_DAY, pa.int64()))
            bad_day = pc.not_equal(days, pa.scalar(expected_day, pa.int64()))
            if pc.any(bad_day).as_py():
                raise ReportSourceUnreadable(
                    f"原件 {source.sha256} 含 UTC 日期与查询日期 "
                    f"{obs_date.isoformat()} 不符的行，拒绝生成混杂报告"
                )

            sal_col = table.column("salinity")
            if not (
                pa.types.is_floating(sal_col.type)
                or pa.types.is_integer(sal_col.type)
            ):
                raise ReportSourceUnreadable(
                    f"原件 {source.sha256} 的 salinity 列必须是数值类型"
                )
            sal = pc.cast(sal_col, pa.float64())
            # 有限值把 -0.0 归一到 0.0，避免被 count_distinct 当成两个取值
            sal = pc.if_else(pc.equal(sal, pa.scalar(0.0)), pa.scalar(0.0), sal)
            code = _classify_column(sal_col)
            # 批次内先做聚合：同一原件内重复的 (时刻, 取值)/(时刻, 状态)
            # 无需在后续合并中重复出现，内存随基数而非总行数增长。
            finite = pa.table({"ts_us": us, "value": sal}).filter(
                pc.equal(code, pa.scalar(_CODE_FINITE, pa.int8()))
            )
            if finite.num_rows:
                scan.finite.append(
                    finite.group_by(["ts_us", "value"]).aggregate([])
                )
            anomalies = pa.table({"ts_us": us, "code": code}).filter(
                pc.not_equal(code, pa.scalar(_CODE_FINITE, pa.int8()))
            )
            if anomalies.num_rows:
                scan.anomalies.append(
                    anomalies.group_by(["ts_us", "code"]).aggregate([])
                )
    except ReportSourceUnreadable:
        raise
    except (pa.ArrowInvalid, pa.ArrowIOError, pa.ArrowTypeError, OSError) as exc:
        raise ReportSourceUnreadable(
            f"原件 {source.sha256} 读取不完整：{exc}"
        ) from exc
    return scan


# ---------------------------------------------------------------------------
# 报告组装
# ---------------------------------------------------------------------------


def _build_report_sync(
    settings: Settings, db: Database, station_id: str, obs_date: dt.date,
    *, offset: int = 0, limit: int = 1000,
) -> dict:
    snapshot = _create_snapshot_sync(settings, db, station_id, obs_date)
    try:
        scans = [_scan_one_source(src, station_id, obs_date)
                 for src in snapshot.sources]
        return _assemble_report(snapshot, scans, offset=offset, limit=limit)
    finally:
        snapshot.cleanup()


def _assemble_report(snapshot: Snapshot, scans: list[_FileScan], *,
                     offset: int, limit: int) -> dict:
    sources = [
        {
            "sha256": src.sha256,
            "size": src.size,
            "receipt_ids": list(src.receipt_ids),
        }
        for src in snapshot.sources
    ]
    receipt_ids_by_sha = {src.sha256: src.receipt_ids for src in snapshot.sources}

    # 汇总所有原件的紧凑投影（每个原件内部已去重），再在 Arrow 层做
    # 跨原件聚合；不把逐行数据搬进 Python 对象。
    finite_chunks = [chunk for scan in scans for chunk in scan.finite]

    # 缺测/非有限：按 (时刻, 原件, 状态) 去重计数；明细只为请求窗口展开
    anomaly_table = _anomaly_summary(scans)
    conflict_table = _conflict_times(finite_chunks)
    conflict_total = conflict_table.num_rows
    anomaly_total = anomaly_table.num_rows
    missing_total = _count_codes(anomaly_table, (_CODE_MISSING,))
    non_finite_total = _count_codes(
        anomaly_table, (_CODE_NAN, _CODE_POS_INF, _CODE_NEG_INF)
    )

    conflicts = _conflict_window(scans, conflict_table, offset, limit,
                                 receipt_ids_by_sha)
    anomalies = _anomaly_window(anomaly_table, offset, limit, receipt_ids_by_sha)
    missing = [a for a in anomalies if a["missing"]]
    non_finite = [a for a in anomalies if a["non_finite"]]

    if not sources:
        status = "no_data"
    elif conflict_total:
        status = "conflict"
    elif anomaly_total:
        status = "incomplete"
    else:
        status = "ok"

    # 缺测/非有限让报告不再「干净」：has_conflicts 明确为真，绝不当作无冲突
    has_conflicts = status in ("conflict", "incomplete")

    generated_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "station_id": snapshot.station_id,
        "obs_date": snapshot.obs_date.isoformat(),
        "status": status,
        "has_conflicts": has_conflicts,
        "has_value_conflicts": bool(conflict_total),
        "has_anomalies": bool(anomaly_total),
        "clean": status == "ok",
        "conflict_count": conflict_total,
        "anomaly_count": anomaly_total,
        "missing_count": missing_total,
        "non_finite_count": non_finite_total,
        "source_count": len(sources),
        "receipt_count": sum(len(s["receipt_ids"]) for s in sources),
        "detail_offset": offset,
        "detail_limit": limit,
        "conflicts_returned": len(conflicts),
        "anomalies_returned": len(anomalies),
        "conflicts_truncated": offset + len(conflicts) < conflict_total,
        "anomalies_truncated": offset + len(anomalies) < anomaly_total,
        "sources": sources,
        "conflicts": conflicts,
        "missing": missing,
        "non_finite": non_finite,
        "anomalies": anomalies,
        "generated_at": generated_at,
    }


def _prune_duplicate_sources(scans: list[_FileScan]) -> list[_FileScan]:
    """快照已按 sha 去重，这里只防御性处理重复扫描。"""
    seen: set[str] = set()
    out: list[_FileScan] = []
    for scan in scans:
        if scan.sha256 not in seen:
            seen.add(scan.sha256)
            out.append(scan)
    return out


def _conflict_times(finite_chunks: list[pa.Table]) -> pa.Table:
    """按 UTC 时刻聚合有限盐度：返回至少有两个不同取值的时刻表。"""
    empty = pa.table({"ts_us": pa.array([], pa.int64())})
    if not finite_chunks:
        return empty
    table = pa.concat_tables(finite_chunks)
    if table.num_rows == 0:
        return empty
    grouped = table.group_by("ts_us").aggregate([("value", "count_distinct")])
    conflict = grouped.filter(
        pc.greater(grouped["value_count_distinct"], pa.scalar(1, pa.int64()))
    )
    return conflict.select(["ts_us"]).sort_by([("ts_us", "ascending")])


def _anomaly_summary(scans: list[_FileScan]) -> pa.Table:
    """按 (时刻, 原件, 状态码) 汇总缺测/非有限观测。

    只保留紧凑的 Arrow 列（不逐行展开接收号），明细窗口再补接收号，
    避免异常很多时把内存放大。
    """
    rows: list[pa.Table] = []
    empty = pa.table(
        {
            "ts_us": pa.array([], pa.int64()),
            "sha256": pa.array([], pa.string()),
            "code": pa.array([], pa.int8()),
        }
    )
    for scan in _prune_duplicate_sources(scans):
        if not scan.anomalies:
            continue
        table = pa.concat_tables(scan.anomalies)
        rows.append(
            pa.table(
                {
                    "ts_us": table["ts_us"],
                    "sha256": pa.array([scan.sha256] * table.num_rows, pa.string()),
                    "code": table["code"],
                }
            )
        )
    if not rows:
        return empty
    return pa.concat_tables(rows).sort_by(
        [("ts_us", "ascending"), ("sha256", "ascending"), ("code", "ascending")]
    )


def _count_codes(anomaly_table: pa.Table, codes: tuple[int, ...]) -> int:
    if anomaly_table.num_rows == 0:
        return 0
    mask = pc.is_in(anomaly_table["code"],
                    value_set=pa.array(list(codes), pa.int8()))
    return int(pc.sum(pc.cast(mask, pa.int64())).as_py() or 0)


def _conflict_window(
    scans: list[_FileScan], conflict_table: pa.Table, offset: int, limit: int,
    receipt_ids_by_sha: dict[str, list[str]],
) -> list[dict]:
    if conflict_table.num_rows == 0:
        return []
    window = conflict_table.slice(offset, limit)
    if window.num_rows == 0:
        return []
    wanted = window["ts_us"]

    # 窗口内每个时刻 + 盐度值 -> 给出该值的原件集合
    by_source: dict[int, dict[float, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for scan in _prune_duplicate_sources(scans):
        if not scan.finite:
            continue
        table = pa.concat_tables(scan.finite)
        window_rows = table.filter(pc.is_in(table["ts_us"], value_set=wanted))
        if window_rows.num_rows == 0:
            continue
        # 同一原件内重复的同值行无需重复登记
        deduped = window_rows.group_by(["ts_us", "value"]).aggregate([])
        for ts_us, value in zip(deduped["ts_us"].to_pylist(),
                                deduped["value"].to_pylist()):
            by_source[int(ts_us)][float(value)].add(scan.sha256)

    details: list[dict] = []
    for ts_us in wanted.to_pylist():
        values = by_source.get(int(ts_us), {})
        if len(values) < 2:  # 理论上不会发生；防御性跳过
            continue
        details.append(_conflict_detail(int(ts_us), values, receipt_ids_by_sha))
    return details


def _anomaly_window(
    anomaly_table: pa.Table, offset: int, limit: int,
    receipt_ids_by_sha: dict[str, list[str]],
) -> list[dict]:
    window = anomaly_table.slice(offset, limit)
    details: list[dict] = []
    for ts_us, sha, code in zip(
        window["ts_us"].to_pylist(),
        window["sha256"].to_pylist(),
        window["code"].to_pylist(),
    ):
        details.append(
            _anomaly_detail(
                int(ts_us), sha, _CODE_TO_STATE[int(code)],
                receipt_ids_by_sha.get(sha, []),
            )
        )
    return details


def _conflict_detail(
    ts_us: int, by_value: dict[float, set[str]],
    receipt_ids_by_sha: dict[str, list[str]],
) -> dict:
    values = []
    for value in sorted(by_value):
        shas = sorted(by_value[value])
        values.append(
            {
                "salinity": value,
                "salinity_state": FINITE,
                "missing": False,
                "non_finite": False,
                "sha256s": shas,
                "receipt_ids": sorted(
                    {rid for sha in shas for rid in receipt_ids_by_sha[sha]}
                ),
            }
        )
    all_shas = sorted({sha for shas in by_value.values() for sha in shas})
    return {
        "timestamp_utc": _iso_utc(ts_us),
        "values": values,
        "sha256s": all_shas,
        "receipt_ids": sorted(
            {rid for sha in all_shas for rid in receipt_ids_by_sha[sha]}
        ),
    }


def _anomaly_detail(
    ts_us: int, sha: str, state: str, receipt_ids: list[str],
) -> dict:
    return {
        "timestamp_utc": _iso_utc(ts_us),
        "salinity": None,
        "salinity_state": state,
        "missing": state == MISSING,
        "non_finite": state in (NAN, POSITIVE_INFINITY, NEGATIVE_INFINITY),
        "sha256": sha,
        "sha256s": [sha],
        "receipt_ids": sorted(receipt_ids),
    }


def build_report(
    settings: Settings, db: Database, station_id: str, obs_date: dt.date,
    *, offset: int = 0, limit: int = 1000,
) -> dict:
    """生成一次站日盐度冲突报告（同步，供专用线程池调用）。"""
    return _build_report_sync(
        settings, db, station_id, obs_date, offset=offset, limit=limit
    )

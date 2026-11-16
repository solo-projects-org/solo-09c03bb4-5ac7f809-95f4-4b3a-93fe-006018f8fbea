"""请求/响应模型与字段校验。"""

from __future__ import annotations

import datetime as dt
import re

from pydantic import BaseModel, Field, field_validator

_RECEIPT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$")
_SHA_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class DeclarationIn(BaseModel):
    """接收声明：站号、UTC 日期、原包长度与 SHA-256。"""

    receipt_id: str = Field(..., description="接收号，同号重传沿用同一记录")
    station_id: str = Field(..., min_length=1, max_length=64, description="浮标站号")
    obs_date: dt.date = Field(..., description="观测 UTC 日期，ISO 格式 YYYY-MM-DD")
    size: int = Field(..., ge=0, description="原包字节长度（Parquet 文件大小）")
    sha256: str = Field(..., min_length=64, max_length=64, description="原包 SHA-256 十六进制摘要")

    @field_validator("receipt_id")
    @classmethod
    def _check_receipt(cls, v: str) -> str:
        if not _RECEIPT_RE.match(v):
            raise ValueError("接收号仅允许字母数字及 ._- 且以字母数字开头，最长 64 位")
        return v

    @field_validator("sha256")
    @classmethod
    def _check_sha(cls, v: str) -> str:
        if not _SHA_RE.match(v):
            raise ValueError("SHA-256 必须是 64 位十六进制字符串")
        return v.lower()


class ReceiptOut(BaseModel):
    receipt_id: str
    station_id: str
    obs_date: str
    declared_size: int
    declared_sha256: str
    state: str
    bytes_received: int
    actual_sha256: str | None
    blob_sha256: str | None
    row_count: int | None
    last_error: str | None
    created_at: str
    updated_at: str


class UploadResult(BaseModel):
    receipt_id: str
    state: str
    bytes_received: int
    declared_size: int
    actual_sha256: str | None
    row_count: int | None
    message: str


class QuotaOut(BaseModel):
    quota_bytes: int
    used_bytes: int
    blob_bytes: int
    staging_bytes: int
    available_bytes: int


class ReconcileOut(BaseModel):
    removed_staging_files: int
    removed_orphan_blobs: int
    reset_receipts: int
    removed_report_snapshots: int = 0


class ReportSourceOut(BaseModel):
    """报告快照中的一份去重原件。"""

    sha256: str
    size: int
    receipt_ids: list[str]


class ConflictValueOut(BaseModel):
    """冲突时刻下的一个不同盐度取值及其来源。"""

    salinity: float
    salinity_state: str = Field(..., description="有限值固定为 finite")
    missing: bool = False
    non_finite: bool = False
    sha256s: list[str]
    receipt_ids: list[str]


class ConflictOut(BaseModel):
    """同一 UTC 时刻出现多个不同有限盐度的冲突明细。"""

    timestamp_utc: str
    values: list[ConflictValueOut]
    sha256s: list[str]
    receipt_ids: list[str]


class SalinityAnomalyOut(BaseModel):
    """缺测（NULL）或非有限（NaN/±Inf）盐度的明确标记。"""

    timestamp_utc: str
    salinity: float | None = None
    salinity_state: str = Field(
        ..., description="missing | nan | positive_infinity | negative_infinity"
    )
    missing: bool
    non_finite: bool
    sha256: str
    sha256s: list[str]
    receipt_ids: list[str]


class SalinityReportOut(BaseModel):
    """站日盐度冲突报告（基于查询时点的一致快照）。"""

    station_id: str
    obs_date: str
    status: str = Field(
        ..., description="ok | conflict | incomplete | no_data"
    )
    has_conflicts: bool = Field(
        ..., description="存在盐度冲突或缺测/非有限盐度时为 true，绝不误报无冲突"
    )
    has_value_conflicts: bool
    has_anomalies: bool
    clean: bool
    conflict_count: int
    anomaly_count: int
    missing_count: int
    non_finite_count: int
    source_count: int
    receipt_count: int
    detail_offset: int = Field(..., description="明细窗口起点（按时间排序）")
    detail_limit: int = Field(..., description="明细窗口最大条数")
    conflicts_returned: int
    anomalies_returned: int
    conflicts_truncated: bool
    anomalies_truncated: bool
    sources: list[ReportSourceOut]
    conflicts: list[ConflictOut]
    missing: list[SalinityAnomalyOut]
    non_finite: list[SalinityAnomalyOut]
    anomalies: list[SalinityAnomalyOut]
    generated_at: str

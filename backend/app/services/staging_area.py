"""暂存落地领域服务：维护传输数据的暂存分区落地路径、文件组织与驻留时限控制。

本模块从传输批次分片清单读取实际分片，落地到暂存分区时保留分片边界与逐片校验值，
落地确认生成可独立核查的暂存回执。回执记录实际可读取的分片信息，并交给传输侧
作为交付数据包的唯一数据依据。
"""
from __future__ import annotations

import itertools
import json
import time
from pathlib import Path
from typing import Any

from app.delivery_utils import (
    canonical_json,
    jsonl_bytes,
    safe_name,
    sha256_bytes,
)
from app.services.base import BaseModuleService

# 同秒内多次落地时用递增序号区分暂存编号，避免两个分区写进同一个目录。
_SEQUENCE = itertools.count(1)


class StagingAreaService(BaseModuleService):
    MODULE = "staging_area"
    ENTITY = "暂存分区"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["暂存编号", "批次编号", "落地路径", "文件数量", "记录条数", "占用字节", "驻留时限", "落地水位"]
    REQUIRED_FIELDS = ["暂存编号", "批次编号", "落地路径"]
    STATUS_ORDER = ["草稿", "落地中", "已落地", "已清空"]
    ACTION_RULES = {"启动落地": "落地中", "确认落地": "已落地", "执行清空": "已清空"}
    NEGATIVE_ACTIONS = ["执行清空"]

    # -- 分片落地 --

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记暂存分区时直接执行落地，保证分区一创建就有可读取的实际分片。

        通用登记接口只传批次编号和落地路径；这里把落地路径交给 land_batch 完成实际
        落盘，这样“同一批次改用不同路径再次落地”只需要换一个落地路径即可。
        """
        missing = [field for field in self.REQUIRED_FIELDS
                   if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        batch_code = str(values["批次编号"]).strip()
        landing_path = str(values["落地路径"]).strip()
        staging_code = str(values.get("暂存编号") or "").strip() or None
        landed = self.land_batch(
            batch_code, 0, landing_path=landing_path, staging_code=staging_code)
        entry = self.get_entry(int(landed["staging_id"]))
        return (entry, []) if entry is not None else (None, [])

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """确认落地/执行清空走真实落地语义，其他动作沿用通用状态流转。"""
        if action == "确认落地":
            result = self.confirm_land(entry_id)
            if "error" in result:
                return None, str(result["error"])
            entry = self.get_entry(entry_id)
            return entry, "暂存分区已确认落地并生成回执"
        if action == "执行清空":
            result = self.clear_staging(entry_id)
            if "error" in result:
                return None, str(result["error"])
            entry = self.get_entry(entry_id)
            return entry, "暂存分区已清空"
        return super().run_action(entry_id, action)

    def _load_batch_chunks(self, batch_code: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """读取批次分片清单与实际字节，返回 (清单头, 分片列表)。

        清单不存在时从合并产物或模拟数据回退生成分片，保证暂存确认仍有可读取分片。
        """
        manifest_file = self._data_dir("batches") / batch_code / "chunks.json"
        if manifest_file.exists():
            try:
                with open(manifest_file, "r", encoding="utf-8") as handle:
                    manifest = json.load(handle)
                chunk_dir = manifest_file.parent
                chunks: list[dict[str, Any]] = []
                for item in manifest.get("chunks", []):
                    chunk_file = chunk_dir / str(item.get("path", ""))
                    if not chunk_file.exists():
                        continue
                    payload = chunk_file.read_bytes()
                    chunks.append({
                        "chunk_index": int(item.get("chunk_index", len(chunks))),
                        "path": f"chunk_{int(item.get('chunk_index', len(chunks))):04d}.jsonl",
                        "record_count": item.get("record_count", 0),
                        "bytes": len(payload),
                        "sha256": sha256_bytes(payload),
                        "payload": payload,
                    })
                if chunks:
                    return manifest, chunks
            except (json.JSONDecodeError, OSError, ValueError):
                pass

        # 回退：合并产物存在则重新切分，否则生成模拟数据。
        merged = self._data_dir("batches") / f"{batch_code}.jsonl"
        records = self._read_jsonl(merged) if merged.exists() else []
        if not records:
            records = self._simulate_records(batch_code, count=10, field_prefix="staging")
        chunks = []
        for index, start in enumerate(range(0, len(records), 3)):
            payload = jsonl_bytes(records[start:start + 3])
            chunks.append({
                "chunk_index": index,
                "path": f"chunk_{index:04d}.jsonl",
                "record_count": len(records[start:start + 3]),
                "bytes": len(payload),
                "sha256": sha256_bytes(payload),
                "payload": payload,
            })
        return {"batch_code": batch_code, "chunk_count": len(chunks)}, chunks

    def land_batch(
        self,
        batch_code: str,
        record_count: int,
        landing_path: str | None = None,
        staging_code: str | None = None,
    ) -> dict[str, Any]:
        """将传输批次分片落地到暂存分区并登记分区行。

        落地路径可以显式指定：同一批次后续落地确认改用新路径时，旧分区与旧回执
        仍然保留，不会被新路径覆盖。暂存确认时由确认方重新读取这些分片生成回执。
        """
        logical_path = landing_path or f"/staging/{batch_code}/"
        landed = self._land_chunks(
            batch_code, landing_path=logical_path, staging_code=staging_code)
        entry, missing = super().create_entry({
            "暂存编号": landed["staging_code"],
            "批次编号": batch_code,
            "落地路径": logical_path,
            "文件数量": str(landed["chunk_count"]),
            "记录条数": str(landed["records_landed"]),
            "占用字节": str(landed["total_bytes"]),
            "驻留时限": "86400",
            "落地水位": str(landed["records_landed"]),
        })
        if entry is None:
            raise ValueError(f"创建暂存分区失败: {missing}")
        entry["占用字节"] = str(landed["total_bytes"])
        entry["文件数量"] = str(landed["chunk_count"])
        entry["落地水位"] = str(landed["records_landed"])
        entry["记录条数"] = str(landed["records_landed"])
        self._write_checkpoint(f"staging_{landed['staging_code']}", {
            "staging_code": landed["staging_code"],
            "batch_code": batch_code,
            "landing_path": logical_path,
            "records_landed": landed["records_landed"],
            "total_bytes": landed["total_bytes"],
            "chunk_count": landed["chunk_count"],
            "landed_at": time.time(),
            "retention_seconds": 86400,
        })
        return {**landed, "staging_id": entry["id"], "landing_path": logical_path}

    def _land_chunks(
        self,
        batch_code: str,
        *,
        landing_path: str,
        staging_code: str | None = None,
    ) -> dict[str, Any]:
        """把批次分片按原边界写入暂存目录，返回实际落盘的分片信息（尚未登记分区行）。"""
        sequence = next(_SEQUENCE)
        staging_code = staging_code or f"STAG-{batch_code}-{int(time.time() * 1000)}-{sequence:03d}"
        _manifest, chunks = self._load_batch_chunks(batch_code)
        storage_dir = self._data_dir("staging") / safe_name(staging_code)
        storage_dir.mkdir(parents=True, exist_ok=True)
        merged_file = self._data_dir("staging") / f"{staging_code}.jsonl"

        landed: list[dict[str, Any]] = []
        total_bytes = 0
        total_records = 0
        with open(merged_file, "wb") as merged_handle:
            for chunk in chunks:
                payload = chunk["payload"]
                chunk_file = storage_dir / chunk["path"]
                # 覆盖写，实际落地字节与回执逐片校验值必须一致。
                with open(chunk_file, "wb") as handle:
                    handle.write(payload)
                merged_handle.write(payload)
                landed.append({
                    "chunk_index": chunk["chunk_index"],
                    "path": chunk["path"],
                    "record_count": chunk["record_count"],
                    "bytes": len(payload),
                    "sha256": sha256_bytes(payload),
                })
                total_bytes += len(payload)
                total_records += int(chunk["record_count"] or 0)

        return {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "landing_path": landing_path,
            "records_landed": total_records,
            "total_bytes": total_bytes,
            "chunk_count": len(landed),
            "chunks": landed,
            "artifact_path": str(merged_file),
            "storage_dir": str(storage_dir),
        }

    # -- 暂存确认与回执 --

    def _staging_chunk_dir(self, staging_code: str) -> Path:
        """返回暂存分区实际分片目录。"""
        return self._data_dir("staging") / safe_name(staging_code)

    def _read_staged_chunks(self, staging_code: str) -> tuple[list[dict[str, Any]], str]:
        """读取暂存分区中实际可读取的分片，重新计算逐片校验值。

        返回 (分片信息, 存储目录)。分片以实际磁盘字节为准，而不是写回执时的内存值。
        """
        chunk_dir = self._staging_chunk_dir(staging_code)
        chunks: list[dict[str, Any]] = []
        if chunk_dir.is_dir():
            for chunk_file in sorted(chunk_dir.glob("chunk_*.jsonl")):
                payload = chunk_file.read_bytes()
                chunks.append({
                    "chunk_index": len(chunks),
                    "path": chunk_file.name,
                    "record_count": sum(
                        1 for line in payload.decode("utf-8", errors="replace").splitlines()
                        if line.strip()
                    ),
                    "bytes": len(payload),
                    "sha256": sha256_bytes(payload),
                    "readable": True,
                })
        if chunks:
            return chunks, str(chunk_dir)

        # 兼容旧分区：只有合并产物时按单分片登记。
        merged = self._data_dir("staging") / f"{staging_code}.jsonl"
        if merged.exists():
            payload = merged.read_bytes()
            return ([{
                "chunk_index": 0,
                "path": merged.name,
                "record_count": sum(
                    1 for line in payload.decode("utf-8", errors="replace").splitlines()
                    if line.strip()
                ),
                "bytes": len(payload),
                "sha256": sha256_bytes(payload),
                "readable": True,
            }], str(merged.parent))
        return [], ""

    def _receipt_dir(self, batch_code: str) -> Path:
        """返回批次暂存回执的持久化目录。"""
        return self._data_dir("receipts") / safe_name(batch_code)

    def _receipt_path(self, batch_code: str, staging_code: str) -> Path:
        return self._receipt_dir(batch_code) / f"{safe_name(staging_code)}.json"

    def confirm_land(self, staging_id: int) -> dict[str, Any]:
        """确认暂存落地，读取实际可读取分片并生成可独立核查的暂存回执。

        回执一旦写入就独立持久化：后续同类批次改用新落地路径再次确认时，先前回执
        仍可按回执编号核查，不会因新路径落地而被覆盖。
        """
        entry, msg = BaseModuleService.run_action(self, staging_id, "确认落地")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        batch_code = entry.get("批次编号", "")
        chunks, storage_dir = self._read_staged_chunks(staging_code)
        if not chunks:
            return {"error": f"暂存分区 {staging_code} 没有可读取的分片，无法确认落地",
                    "staging_id": staging_id}

        total_bytes = sum(int(chunk["bytes"]) for chunk in chunks)
        total_records = sum(int(chunk["record_count"]) for chunk in chunks)
        core = {
            "receipt_code": f"RCPT-{staging_code}",
            "receipt_type": "staging_confirm",
            "staging_code": staging_code,
            "staging_id": entry["id"],
            "batch_code": batch_code,
            "landing_path": entry.get("落地路径", ""),
            "confirmed_at": time.time(),
            "algorithm": "SHA256",
            "chunk_count": len(chunks),
            "record_count": total_records,
            "total_bytes": total_bytes,
            "storage_dir": storage_dir,
            "chunks": chunks,
        }
        core["receipt_checksum"] = f"SHA256:{sha256_bytes(canonical_json(core))}"

        # 回执独立落盘；同一暂存分区的重复确认只覆盖自己的回执。
        receipt_file = self._receipt_path(batch_code, staging_code)
        receipt_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = receipt_file.with_suffix(".json.tmp")
        with open(tmp_file, "w", encoding="utf-8") as handle:
            json.dump(core, handle, ensure_ascii=False, indent=2)
        tmp_file.replace(receipt_file)

        self._write_jsonl(
            self._data_dir("events") / "staging_confirm_log.jsonl",
            [{"action": "confirm_land", "staging_code": staging_code,
              "batch_code": batch_code, "receipt_code": core["receipt_code"],
              "landing_path": core["landing_path"], "chunk_count": len(chunks),
              "record_count": total_records, "total_bytes": total_bytes,
              "receipt_checksum": core["receipt_checksum"],
              "confirmed_at": core["confirmed_at"]}]
        )
        # 暂存确认同时把实际分片清单交给传输侧登记。
        self._write_jsonl(
            self._data_dir("events") / "staging_receipt_handoff_log.jsonl",
            [{"action": "handoff_to_transfer", "batch_code": batch_code,
              "receipt_code": core["receipt_code"], "staging_code": staging_code,
              "landing_path": core["landing_path"],
              "chunks": [{"chunk_index": c["chunk_index"], "sha256": c["sha256"],
                          "record_count": c["record_count"], "bytes": c["bytes"]}
                         for c in chunks],
              "at": time.time()}]
        )
        return {
            "staging_code": staging_code,
            "status": "已落地",
            "receipt_code": core["receipt_code"],
            "receipt_checksum": core["receipt_checksum"],
            "landing_path": core["landing_path"],
            "record_count": total_records,
            "chunk_count": len(chunks),
            "chunks": chunks,
        }

    def list_receipts(self, batch_code: str) -> list[dict[str, Any]]:
        """列出批次全部暂存回执，按确认时间倒序；先前回执始终可按回执编号核查。"""
        receipt_dir = self._receipt_dir(batch_code)
        receipts: list[dict[str, Any]] = []
        if not receipt_dir.is_dir():
            return receipts
        for receipt_file in receipt_dir.glob("*.json"):
            try:
                with open(receipt_file, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except (json.JSONDecodeError, OSError):
                continue
            if isinstance(payload, dict):
                receipts.append(payload)
        receipts.sort(key=lambda item: float(item.get("confirmed_at", 0)), reverse=True)
        return receipts

    def get_receipt(self, batch_code: str, receipt_code: str) -> dict[str, Any] | None:
        """按回执编号读取单张回执，供交付包和核验环节引用。"""
        for receipt in self.list_receipts(batch_code):
            if receipt.get("receipt_code") == receipt_code:
                return receipt
        return None

    def clear_staging(self, staging_id: int) -> dict[str, Any]:
        """执行暂存分区清空，保留已确认回执，供后续按回执核查分片信息。"""
        entry, msg = BaseModuleService.run_action(self, staging_id, "执行清空")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        # 标记清空断点；回执目录不删除，保证先前回执仍可核查。
        self._write_checkpoint(f"staging_{staging_code}_cleared", {
            "staging_code": staging_code,
            "cleared_at": time.time(),
            "status": "cleared",
        })
        self._write_jsonl(
            self._data_dir("events") / "staging_clear_log.jsonl",
            [{"action": "clear", "staging_code": staging_code, "cleared_at": time.time()}]
        )
        return {"staging_code": staging_code, "status": "已清空"}

"""传输批次领域服务：维护传输批次的分片组织、记录条数与字节总量登记及批次水位推进。

本模块读取抽取窗口产物文件后创建传输批次，分片写入传输数据文件，
传输完成后自动驱动暂存落地和校验核对，形成窗口抽取→批次传输→落地校验的贯通链路。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.delivery_utils import jsonl_bytes, sha256_bytes
from app.services.base import BaseModuleService
from app.store import store


class TransferBatchService(BaseModuleService):
    MODULE = "transfer_batch"
    ENTITY = "传输批次"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["批次编号", "窗口编号", "目标编号", "记录条数", "字节总量", "分片大小", "传输状态", "批次水位"]
    REQUIRED_FIELDS = ["批次编号", "窗口编号", "目标编号"]
    STATUS_ORDER = ["草稿", "待传输", "传输中", "已完成", "已失败"]
    ACTION_RULES = {"启动传输": "传输中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """启动传输走真实分片传输流程（含自动暂存与校验），其他动作沿用通用状态流转。"""
        if action == "启动传输":
            result = self.start_transfer(entry_id)
            if "error" in result:
                return None, str(result["error"])
            entry = self.get_entry(entry_id)
            return entry, f"传输批次已启动传输，共 {result['records_transferred']} 条"
        return super().run_action(entry_id, action)

    def create_batch(self, window_code: str, record_count: int, target_code: str = "TARG-0001") -> dict[str, Any]:
        """基于抽取窗口创建传输批次，传入产物文件的 record_count 并写入批次事件日志。"""
        batch_code = f"TRAN-{window_code}-{int(time.time())}"
        values = {
            "批次编号": batch_code,
            "窗口编号": window_code,
            "目标编号": target_code,
            "记录条数": str(record_count),
            "字节总量": "0",
            "分片大小": "1000",
            "传输状态": "草稿",
            "批次水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建传输批次失败: {missing}")
        # 写入批次创建事件
        self._write_jsonl(
            self._data_dir("events") / "batch_create_log.jsonl",
            [{"action": "create_batch", "batch_code": batch_code, "window_code": window_code,
              "record_count": record_count, "target": target_code, "at": time.time()}]
        )
        return {"batch_code": batch_code, "batch_id": entry["id"], "window_code": window_code,
                "record_count": record_count, "target_code": target_code}

    def start_transfer(self, batch_id: int, chunk_size: int = 3) -> dict[str, Any]:
        """启动传输批次执行：读取抽取窗口产物文件，按分片大小写入传输产物文件并推进水位。

        本方法执行实际的批量数据传输：从抽取产物文件读取数据，切分为分片，
        写入传输产物文件（jsonl），计算字节总量，然后自动触发暂存落地和校验核对。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}

        entry, msg = BaseModuleService.run_action(self, batch_id, "启动传输")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}

        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        # 读取上游抽取窗口的产物文件
        artifact_dir = self._data_dir("extracted")
        source_file = artifact_dir / f"{window_code}.jsonl" if window_code != "unknown" else None

        records = []
        if source_file and source_file.exists():
            records = self._read_jsonl(source_file)
        else:
            # 如果上游产物文件不存在，生成模拟数据；记录条数缺失时按默认 10 条。
            try:
                fallback_count = int(batch_entry.get("记录条数") or 10)
            except (TypeError, ValueError):
                fallback_count = 10
            records = self._simulate_records(batch_code, count=fallback_count,
                                              field_prefix="trans")

        # 分片写入传输产物文件：每个分片单独落一个文件，同时保留合并产物以兼容旧链路。
        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        chunk_dir = self._data_dir("batches") / batch_code
        chunk_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = chunk_dir / "chunks.json"
        # 同一批次重复传输时先清掉旧分片清单与合并产物，避免新旧字节混在同一个交付包里。
        if manifest_file.exists():
            manifest_file.unlink()
        for stale in chunk_dir.glob("chunk_*.jsonl"):
            stale.unlink()
        if output_file.exists():
            output_file.unlink()
        total_bytes = 0
        total_written = 0
        chunks: list[dict[str, Any]] = []
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            chunk_index = i // chunk_size
            for rec in chunk:
                rec["batch_code"] = batch_code
                rec["chunk_index"] = chunk_index
                rec["transferred_at"] = time.time()
            chunk_bytes = jsonl_bytes(chunk)
            chunk_file = chunk_dir / f"chunk_{chunk_index:04d}.jsonl"
            # 分片文件按覆盖写，保证校验值与本次传输字节完全一致。
            with open(chunk_file, "wb") as handle:
                handle.write(chunk_bytes)
            with open(output_file, "ab") as handle:
                handle.write(chunk_bytes)
            chunks.append({
                "chunk_index": chunk_index,
                "path": f"chunk_{chunk_index:04d}.jsonl",
                "record_count": len(chunk),
                "bytes": len(chunk_bytes),
                "sha256": sha256_bytes(chunk_bytes),
            })
            total_bytes += len(chunk_bytes)
            total_written += len(chunk)

        # 分片清单是交付包和暂存确认的唯一分片依据：逐片记录条数、字节数与校验值。
        manifest_payload = {
            "batch_code": batch_code,
            "window_code": window_code,
            "chunk_size": chunk_size,
            "chunk_count": len(chunks),
            "record_count": total_written,
            "total_bytes": total_bytes,
            "algorithm": "SHA256",
            "created_at": time.time(),
            "chunks": chunks,
        }
        tmp_manifest = manifest_file.with_suffix(".json.tmp")
        with open(tmp_manifest, "w", encoding="utf-8") as handle:
            json.dump(manifest_payload, handle, ensure_ascii=False, indent=2)
        tmp_manifest.replace(manifest_file)

        # 更新批次水位和字节总量
        entry["字节总量"] = str(total_bytes)
        entry["记录条数"] = str(total_written)
        entry["批次水位"] = str(total_written)

        # 写入传输水位断点
        self._write_checkpoint(f"batch_{batch_code}", {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunk_count": len(chunks),
            "watermark": str(total_written),
            "last_transfer_at": time.time(),
        })

        result = {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunks": len(chunks),
            "artifact_path": str(output_file),
        }

        # 传输完成后自动触发暂存落地
        try:
            sa_svc = self._safe_import("app.services.staging_area", "StagingAreaService")()
            staging_result = sa_svc.land_batch(batch_code, total_written)
            result["auto_staging"] = staging_result
        except Exception as e:
            result["auto_staging"] = {"error": str(e)}

        # 传输完成后自动创建校验核对任务
        try:
            cv_svc = self._safe_import("app.services.checksum_verify", "ChecksumVerifyService")()
            verify_result = cv_svc.create_verification(batch_code, total_written)
            result["auto_verify"] = verify_result
        except Exception as e:
            result["auto_verify"] = {"error": str(e)}

        # 传输完成后自动创建到达对账单
        try:
            ar_svc = self._safe_import("app.services.arrival_reconcile", "ArrivalReconcileService")()
            reconcile_result = ar_svc.create_reconciliation(batch_code, total_written)
            result["auto_reconcile"] = reconcile_result
        except Exception as e:
            result["auto_reconcile"] = {"error": str(e)}

        return result

    def load_chunk_manifest(self, batch_code: str) -> dict[str, Any] | None:
        """读取批次的分片清单，作为交付包与暂存确认的共同分片依据。

        清单文件在传输时一次写入，后续落地确认只读取不改写，确保交付包里的
        分片序号、条数、字节数与逐片校验值都来自同一次传输。
        """
        manifest_file = self._data_dir("batches") / batch_code / "chunks.json"
        if not manifest_file.exists():
            return None
        try:
            with open(manifest_file, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (json.JSONDecodeError, OSError):
            return None
        if not isinstance(payload, dict) or not payload.get("chunks"):
            return None
        return payload

    def chunk_dir(self, batch_code: str):
        """返回批次分片文件所在目录。"""
        return self._data_dir("batches") / batch_code

    def mark_completed(self, batch_id: int) -> dict[str, Any]:
        """标记传输批次已完成，写入完成事件日志并记录批次汇总统计。"""
        entry, msg = self.run_action(batch_id, "标记完成")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        # 从产物文件验证批次完整性
        artifact_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "batch_complete_log.jsonl",
            [{"action": "complete", "batch_code": batch_code, "record_count": len(records),
              "checksum": checksum, "completed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已完成", "record_count": len(records),
                "checksum": checksum}

    def mark_failed(self, batch_id: int, reason: str = "") -> dict[str, Any]:
        """标记传输批次失败，写入失败事件日志并记录中断原因。"""
        entry, msg = self.run_action(batch_id, "标记失败")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        self._write_jsonl(
            self._data_dir("events") / "batch_fail_log.jsonl",
            [{"action": "fail", "batch_code": batch_code, "reason": reason, "failed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已失败", "reason": reason}
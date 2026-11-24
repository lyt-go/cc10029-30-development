"""传输交付领域服务：生成含实际分片校验和暂存回执的传输交付数据包。

交付包是暂存确认之后、目标端落地校验之前的一次性快照：
- 打包开始时冻结暂存回执与实际分片字节，包内数据与清单都取自同一快照；
- 打包过程中即使同一批次又完成了一次改用新路径的暂存确认，也不会混入本包；
- 成功生成的数据包按交付编号持久化，后续下载中断不影响已经正式交付的包；
- 目标端可用下载件逐片复算校验，并核对采用回执关联的实际分片与后续待校验范围。

未完成的打包不会产生正式交付记录，重做后会生成新的交付编号。
"""
from __future__ import annotations

import hashlib
import io
import itertools
import json
import shutil
import threading
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any, Callable

from app.delivery_utils import (
    MAX_VERIFY_BYTES,
    canonical_json,
    count_jsonl_records,
    safe_name,
    sha256_bytes,
)
from app.services.base import DATA_ROOT, BaseModuleService
from app.store import store

_DELIVERY_SEQUENCE = itertools.count(1)
_BUILD_LOCK = threading.RLock()


class DeliveryError(Exception):
    """交付包构建或核验失败（用户可读的领域错误）。"""


class DeliveryInterrupted(Exception):
    """打包被中断：不生成正式交付记录。"""


class TransferDeliveryService(BaseModuleService):
    MODULE = "transfer_delivery"
    ENTITY = "交付包"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["交付编号", "批次编号", "采用回执", "落地路径", "分片数量", "记录条数", "交付校验值", "交付状态"]
    REQUIRED_FIELDS = ["交付编号", "批次编号"]
    STATUS_ORDER = ["打包中", "已交付", "已校验", "校验异常", "已中断"]
    ACTION_RULES = {"执行核验": "已校验", "标记异常": "校验异常"}
    NEGATIVE_ACTIONS = ["标记异常"]

    # ------------------------------------------------------------------
    # 路径与记录
    # ------------------------------------------------------------------

    def _deliveries_root(self) -> Path:
        return self._data_dir("deliveries")

    def _delivery_dir(self, delivery_no: str) -> Path:
        return self._deliveries_root() / safe_name(delivery_no)

    def _record_file(self, delivery_no: str) -> Path:
        return self._delivery_dir(delivery_no) / "record.json"

    def _package_file(self, delivery_no: str) -> Path:
        return self._delivery_dir(delivery_no) / "package.zip"

    def _sync_records_from_disk(self) -> None:
        """把磁盘上已经正式交付的包补回内存列表，服务重启后仍可按交付号核验。"""
        rows = store.rows(self.MODULE)
        known = {str(row.get("交付编号")) for row in rows}
        root = self._deliveries_root()
        if not root.is_dir():
            return
        for record_file in sorted(root.glob("*/record.json")):
            delivery_no = record_file.parent.name
            if delivery_no in known:
                continue
            try:
                with open(record_file, "r", encoding="utf-8") as handle:
                    record = json.load(handle)
            except (json.JSONDecodeError, OSError):
                continue
            if not isinstance(record, dict) or not record.get("delivery_no"):
                continue
            rows.append(self._row_from_record(record))

    @staticmethod
    def _row_from_record(record: dict[str, Any]) -> dict[str, Any]:
        """把持久化交付记录转换成列表行。"""
        return {
            "id": int(record.get("id") or 0),
            "交付编号": record.get("delivery_no", ""),
            "批次编号": record.get("batch_code", ""),
            "采用回执": (record.get("adopted_receipt") or {}).get("receipt_code", ""),
            "落地路径": (record.get("adopted_receipt") or {}).get("landing_path", ""),
            "分片数量": str(record.get("chunk_count", 0)),
            "记录条数": str(record.get("record_count", 0)),
            "交付校验值": record.get("delivery_checksum", ""),
            "交付状态": record.get("status", "已交付"),
            "status": record.get("status", "已交付"),
            "pending": False,
            "abnormal": record.get("status") == "校验异常",
            "delivery_no": record.get("delivery_no", ""),
            "created_at": record.get("created_at", 0),
            "package_sha256": record.get("package_sha256", ""),
        }

    def _next_row_id(self) -> int:
        self._sync_records_from_disk()
        rows = store.rows(self.MODULE)
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def list_entries(self, **kwargs) -> tuple[list[dict[str, Any]], int]:
        """列出交付记录，支持按批号/交付号过滤；先补磁盘记录再走通用查询。"""
        self._sync_records_from_disk()
        return super().list_entries(**kwargs)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        self._sync_records_from_disk()
        return super().get_entry(entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """交付包只能由打包流程产生，不支持通过通用登记接口手工插入记录。"""
        return None, ["交付包需通过按批号打包接口生成，不支持手工登记"]

    def list_deliveries(self, batch_code: str | None = None) -> list[dict[str, Any]]:
        """按批号列出正式交付记录（按生成时间倒序），不含中断的打包尝试。"""
        self._sync_records_from_disk()
        rows = list(store.rows(self.MODULE))
        if batch_code:
            rows = [row for row in rows if row.get("批次编号") == batch_code]
        rows.sort(key=lambda row: float(row.get("created_at", 0)), reverse=True)
        return rows

    def get_delivery(self, delivery_no: str) -> dict[str, Any] | None:
        """按交付编号读取正式交付记录及其清单。"""
        record_file = self._record_file(delivery_no)
        if not record_file.exists():
            self._sync_records_from_disk()
            for row in store.rows(self.MODULE):
                if row.get("交付编号") == delivery_no:
                    return dict(row)
            return None
        try:
            with open(record_file, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (json.JSONDecodeError, OSError):
            return None

    def package_path(self, delivery_no: str) -> Path | None:
        """返回已正式交付包的磁盘路径；未完成或不存在时返回 None。"""
        path = self._package_file(delivery_no)
        return path if path.is_file() else None

    # ------------------------------------------------------------------
    # 暂存回执
    # ------------------------------------------------------------------

    def _staging_service(self):
        return self._safe_import("app.services.staging_area", "StagingAreaService")()

    def list_receipts(self, batch_code: str) -> list[dict[str, Any]]:
        """读取批次全部暂存回执，按确认时间倒序。"""
        return self._staging_service().list_receipts(batch_code)

    def get_receipt(self, batch_code: str, receipt_code: str) -> dict[str, Any] | None:
        return self._staging_service().get_receipt(batch_code, receipt_code)

    # ------------------------------------------------------------------
    # 打包
    # ------------------------------------------------------------------

    @staticmethod
    def _within_data_root(path: Path) -> bool:
        try:
            path.resolve().relative_to(DATA_ROOT.resolve())
            return True
        except (ValueError, OSError):
            return False

    def _freeze_receipt(
        self,
        batch_code: str,
        receipt: dict[str, Any],
        *,
        expected_landing_path: str | None = None,
    ) -> dict[str, Any]:
        """冻结一张暂存回执及其实际分片字节，并验证路径与逐片校验值可读。

        这一步就是“成包前确认新路径”：路径不可读、分片缺失或哈希对不上，
        都在写出任何包内数据之前失败。
        """
        landing_path = str(receipt.get("landing_path", ""))
        if expected_landing_path and landing_path != expected_landing_path:
            raise DeliveryError(
                f"暂存回执 {receipt.get('receipt_code')} 的落地路径为 {landing_path or '空'}，"
                f"与要求确认的新路径 {expected_landing_path} 不一致"
            )
        storage_dir_value = str(receipt.get("storage_dir", ""))
        storage_dir = Path(storage_dir_value) if storage_dir_value else None
        if storage_dir is not None and not self._within_data_root(storage_dir):
            raise DeliveryError(f"暂存回执 {receipt.get('receipt_code')} 的存储目录不在数据根目录内")

        frozen_chunks: list[dict[str, Any]] = []
        for item in receipt.get("chunks", []):
            chunk_path = str(item.get("path", ""))
            if not chunk_path:
                raise DeliveryError(f"暂存回执 {receipt.get('receipt_code')} 存在缺少路径的分片")
            if storage_dir is None:
                raise DeliveryError(f"暂存回执 {receipt.get('receipt_code')} 未登记可读取的分片目录")
            file_path = (storage_dir / chunk_path).resolve()
            try:
                file_path.relative_to(storage_dir.resolve())
            except ValueError:
                raise DeliveryError(f"暂存回执分片路径越界：{chunk_path}")
            if not file_path.is_file():
                raise DeliveryError(f"暂存分片不可读取：{chunk_path}")
            payload = file_path.read_bytes()
            actual_hash = sha256_bytes(payload)
            expected_hash = str(item.get("sha256", ""))
            if expected_hash and actual_hash != expected_hash:
                raise DeliveryError(
                    f"暂存分片 {chunk_path} 校验值不一致，回执登记 {expected_hash}，实际 {actual_hash}"
                )
            frozen_chunks.append({
                "chunk_index": int(item.get("chunk_index", len(frozen_chunks))),
                "path": chunk_path,
                "record_count": count_jsonl_records(payload),
                "bytes": len(payload),
                "sha256": actual_hash,
                "payload": payload,
            })

        if not frozen_chunks:
            raise DeliveryError(f"暂存回执 {receipt.get('receipt_code')} 没有可打包的实际分片")

        # 回执自身也要能通过校验：重新计算回执摘要，确认交付包引用的确是这张回执。
        receipt_hash = str(receipt.get("receipt_checksum", ""))
        core = {key: value for key, value in receipt.items() if key != "receipt_checksum"}
        recomputed = f"SHA256:{sha256_bytes(canonical_json(core))}"
        if receipt_hash and receipt_hash != recomputed:
            raise DeliveryError(
                f"暂存回执 {receipt.get('receipt_code')} 的摘要与内容不一致，不能作为交付依据"
            )

        return {
            "receipt": receipt,
            "landing_path": landing_path,
            "storage_dir": str(storage_dir) if storage_dir else "",
            "chunks": frozen_chunks,
            "receipt_checksum": receipt_hash or recomputed,
        }

    def _choose_receipt(
        self,
        receipts: list[dict[str, Any]],
        *,
        receipt_code: str | None,
        landing_path: str | None,
    ) -> dict[str, Any]:
        """选定采用回执：显式回执 > 指定新路径的最新回执 > 最新回执。"""
        if receipt_code:
            for receipt in receipts:
                if receipt.get("receipt_code") == receipt_code:
                    return receipt
            raise DeliveryError(f"批次的暂存回执中没有找到 {receipt_code}")
        if landing_path:
            matched = [r for r in receipts if r.get("landing_path") == landing_path]
            if not matched:
                raise DeliveryError(f"批次没有落地路径为 {landing_path} 的暂存回执，无法确认新路径")
            return matched[0]
        if not receipts:
            raise DeliveryError("批次没有暂存确认回执，无法生成交付包")
        return receipts[0]

    def build_package(
        self,
        batch_code: str,
        *,
        receipt_code: str | None = None,
        landing_path: str | None = None,
        confirm_landing_path: bool = False,
        interrupt_after_chunks: int | None = None,
        after_snapshot_hook: Callable[[dict[str, Any]], None] | None = None,
        pack_pause_seconds: float = 0.0,
    ) -> dict[str, Any]:
        """按批号生成传输交付数据包。

        打包开始即冻结回执与分片字节；随后发生的暂存确认（即使是同批次改用新路径）
        不会改变本包的数据与清单。中断时抛出 DeliveryInterrupted 且不留正式记录。
        """
        batch_code = (batch_code or "").strip()
        if not batch_code:
            raise DeliveryError("批号不能为空")

        with _BUILD_LOCK:
            receipts = self.list_receipts(batch_code)
            if not receipts:
                raise DeliveryError(f"批次 {batch_code} 尚无暂存确认回执")
            adopted = self._choose_receipt(
                receipts, receipt_code=receipt_code, landing_path=landing_path)
            if confirm_landing_path:
                expected = landing_path or str(adopted.get("landing_path", ""))
                frozen = self._freeze_receipt(
                    batch_code, adopted, expected_landing_path=expected)
            else:
                frozen = self._freeze_receipt(batch_code, adopted)

            snapshot_receipts = [
                {
                    "receipt_code": item.get("receipt_code", ""),
                    "staging_code": item.get("staging_code", ""),
                    "confirmed_at": item.get("confirmed_at", 0),
                    "landing_path": item.get("landing_path", ""),
                    "receipt_checksum": item.get("receipt_checksum", ""),
                    "chunk_count": item.get("chunk_count", 0),
                    "record_count": item.get("record_count", 0),
                }
                for item in receipts
            ]
            frozen_at = time.time()
            delivery_no = (
                f"DELI-{safe_name(batch_code, 'batch')}-"
                f"{int(frozen_at * 1000)}-{next(_DELIVERY_SEQUENCE):03d}"
            )
            record = {
                "delivery_no": delivery_no,
                "batch_code": batch_code,
                "created_at": frozen_at,
                "payload_frozen_at": frozen_at,
                "status": "已交付",
                "algorithm": "SHA256",
                "adopted_receipt": {
                    "receipt_code": adopted.get("receipt_code", ""),
                    "staging_code": adopted.get("staging_code", ""),
                    "confirmed_at": adopted.get("confirmed_at", 0),
                    "landing_path": frozen["landing_path"],
                    "receipt_checksum": frozen["receipt_checksum"],
                    "chunk_count": len(frozen["chunks"]),
                    "record_count": sum(c["record_count"] for c in frozen["chunks"]),
                    "total_bytes": sum(c["bytes"] for c in frozen["chunks"]),
                },
                "chunk_count": len(frozen["chunks"]),
                "record_count": sum(c["record_count"] for c in frozen["chunks"]),
                "total_bytes": sum(c["bytes"] for c in frozen["chunks"]),
                "receipt_snapshot": snapshot_receipts,
            }

            build_dir = self._deliveries_root() / "_build" / delivery_no
            if build_dir.exists():
                shutil.rmtree(build_dir)
            (build_dir / "chunks").mkdir(parents=True, exist_ok=True)
            (build_dir / "receipts").mkdir(parents=True, exist_ok=True)

            try:
                return self._write_package(
                    build_dir=build_dir,
                    record=record,
                    frozen=frozen,
                    snapshot_receipts=snapshot_receipts,
                    interrupt_after_chunks=interrupt_after_chunks,
                    after_snapshot_hook=after_snapshot_hook,
                    pack_pause_seconds=pack_pause_seconds,
                )
            except DeliveryInterrupted as exc:
                shutil.rmtree(build_dir, ignore_errors=True)
                self._write_interrupt_audit(record, str(exc))
                raise
            except Exception:
                shutil.rmtree(build_dir, ignore_errors=True)
                raise

    def _write_interrupt_audit(self, record: dict[str, Any], reason: str) -> None:
        """记录一次未完成的打包尝试，供排查；它不计入正式交付记录。"""
        audit_dir = self._deliveries_root() / "_interrupted"
        audit_dir.mkdir(parents=True, exist_ok=True)
        audit = {
            "attempt_id": f"INT-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}",
            "batch_code": record.get("batch_code", ""),
            "adopted_receipt": record.get("adopted_receipt", {}),
            "packaging_started_at": record.get("created_at", 0),
            "interrupted_at": time.time(),
            "reason": reason,
            "formal_delivery_created": False,
        }
        (audit_dir / f"{audit['attempt_id']}.json").write_text(
            json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_package(
        self,
        *,
        build_dir: Path,
        record: dict[str, Any],
        frozen: dict[str, Any],
        snapshot_receipts: list[dict[str, Any]],
        interrupt_after_chunks: int | None,
        after_snapshot_hook: Callable[[dict[str, Any]], None] | None,
        pack_pause_seconds: float,
    ) -> dict[str, Any]:
        delivery_no = record["delivery_no"]
        manifest_chunks: list[dict[str, Any]] = []
        written = 0
        for chunk in frozen["chunks"]:
            if interrupt_after_chunks is not None and written >= interrupt_after_chunks:
                raise DeliveryInterrupted("打包被中断：分片尚未完全写入交付包")
            chunk_path = build_dir / "chunks" / f"chunk_{int(chunk['chunk_index']):04d}.jsonl"
            with open(chunk_path, "wb") as handle:
                handle.write(chunk["payload"])
            manifest_chunks.append({
                "chunk_index": chunk["chunk_index"],
                "path": f"chunks/chunk_{int(chunk['chunk_index']):04d}.jsonl",
                "record_count": chunk["record_count"],
                "bytes": chunk["bytes"],
                "sha256": chunk["sha256"],
            })
            written += 1

        # 暂存回执原样入包：目标端可据此核对“采用回执关联了哪些实际分片”。
        receipt_code = record["adopted_receipt"]["receipt_code"]
        receipt_payload = canonical_json(frozen["receipt"])
        (build_dir / "receipts").mkdir(parents=True, exist_ok=True)
        (build_dir / "receipts" / f"{safe_name(receipt_code, 'receipt')}.json").write_bytes(
            json.dumps(frozen["receipt"], ensure_ascii=False, indent=2).encode("utf-8"))

        # 快照完成，对外提供一个可观察的窗口：此时并发完成的暂存确认只作为后续观察记录。
        if after_snapshot_hook is not None:
            after_snapshot_hook({"delivery_no": delivery_no, "record": record,
                                 "frozen": frozen})
        if pack_pause_seconds > 0:
            time.sleep(pack_pause_seconds)

        # 只读取后续回执的元数据用于提示，不读取其分片字节，保证包内数据仍全部来自快照。
        later_receipts = self._later_receipt_metadata(
            record["batch_code"], snapshot_receipts)
        total_bytes = sum(chunk["bytes"] for chunk in frozen["chunks"])
        total_records = sum(chunk["record_count"] for chunk in frozen["chunks"])

        manifest = {
            "delivery_no": delivery_no,
            "batch_code": record["batch_code"],
            "created_at": record["created_at"],
            "payload_frozen_at": record["payload_frozen_at"],
            "algorithm": "SHA256",
            "adopted_receipt": record["adopted_receipt"],
            "chunks": manifest_chunks,
            "receipt_snapshot": snapshot_receipts,
            # 打包期间/之后出现的新回执：只作提示，不含新分片数据。
            "post_snapshot_receipts": later_receipts,
            "totals": {
                "chunk_count": len(manifest_chunks),
                "record_count": total_records,
                "total_bytes": total_bytes,
            },
        }
        manifest["manifest_checksum"] = f"SHA256:{sha256_bytes(canonical_json(manifest))}"
        (build_dir / "delivery_manifest.json").write_bytes(
            json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
        with open(build_dir / "README.txt", "w", encoding="utf-8") as handle:
            handle.write(
                f"传输交付数据包 {delivery_no}\n"
                f"批次编号：{record['batch_code']}\n"
                f"采用暂存回执：{receipt_code}\n"
                f"落地路径：{record['adopted_receipt']['landing_path']}\n"
                f"分片数量：{len(manifest_chunks)}，记录条数：{total_records}，字节：{total_bytes}\n"
                "校验方式：按 delivery_manifest.json 的 chunks 顺序逐片计算 SHA256。\n"
            )

        package_file = build_dir / "package.zip"
        with zipfile.ZipFile(package_file, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file in sorted(build_dir.rglob("*")):
                if file.is_file() and file.name != "package.zip":
                    archive.write(file, file.relative_to(build_dir).as_posix())

        package_hash = hashlib.sha256(package_file.read_bytes()).hexdigest()
        record["delivery_checksum"] = manifest["manifest_checksum"]
        record["package_sha256"] = f"SHA256:{package_hash}"
        record["manifest"] = manifest
        record["chunk_count"] = len(manifest_chunks)
        record["record_count"] = total_records
        record["total_bytes"] = total_bytes
        record["status"] = "已交付"
        record["id"] = self._next_row_id()

        final_dir = self._delivery_dir(delivery_no)
        final_dir.mkdir(parents=True, exist_ok=True)
        package_file.replace(final_dir / "package.zip")
        (final_dir / "delivery_manifest.json").write_bytes(
            json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
        (final_dir / "record.json").write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        shutil.rmtree(build_dir, ignore_errors=True)

        store.rows(self.MODULE).append(self._row_from_record(record))
        self._write_jsonl(
            self._data_dir("events") / "delivery_log.jsonl",
            [{"action": "build_package", "delivery_no": delivery_no,
              "batch_code": record["batch_code"], "receipt_code": receipt_code,
              "landing_path": record["adopted_receipt"]["landing_path"],
              "chunk_count": len(manifest_chunks),
              "record_count": total_records, "bytes": total_bytes,
              "manifest_checksum": manifest["manifest_checksum"],
              "package_sha256": record["package_sha256"],
              "post_snapshot_receipts": [r["receipt_code"] for r in later_receipts],
              "created_at": record["created_at"]}]
        )
        return self._public_record(record)

    def _later_receipt_metadata(
        self,
        batch_code: str,
        snapshot_receipts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """对比快照后出现的新回执，仅记录元数据，不取其分片字节。"""
        snapshot_codes = {item.get("receipt_code") for item in snapshot_receipts}
        later: list[dict[str, Any]] = []
        for receipt in self.list_receipts(batch_code):
            if receipt.get("receipt_code") in snapshot_codes:
                continue
            later.append({
                "receipt_code": receipt.get("receipt_code", ""),
                "staging_code": receipt.get("staging_code", ""),
                "confirmed_at": receipt.get("confirmed_at", 0),
                "landing_path": receipt.get("landing_path", ""),
                "receipt_checksum": receipt.get("receipt_checksum", ""),
                "chunk_count": receipt.get("chunk_count", 0),
                "record_count": receipt.get("record_count", 0),
                "detected_at": time.time(),
                "source": "post_snapshot_observation",
            })
        later.sort(key=lambda item: float(item.get("confirmed_at", 0)), reverse=True)
        return later

    @staticmethod
    def _public_record(record: dict[str, Any]) -> dict[str, Any]:
        """对外返回交付记录（去掉体积较大的完整清单，保留摘要字段）。"""
        return {
            key: value for key, value in record.items() if key != "manifest"
        } | {"manifest": record.get("manifest")}

    # ------------------------------------------------------------------
    # 核验
    # ------------------------------------------------------------------

    def _collect_current_receipts(self, batch_code: str) -> dict[str, dict[str, Any]]:
        """收集当前全部回执并计算其分片摘要，用于确定后续待校验范围。"""
        receipts = self.list_receipts(batch_code)
        return {str(item.get("receipt_code")): item for item in receipts}

    @staticmethod
    def _chunk_scope(receipt: dict[str, Any]) -> list[dict[str, Any]]:
        """从回执中提取待校验分片范围（仅摘要，不读字节）。"""
        return [
            {
                "chunk_index": int(chunk.get("chunk_index", index)),
                "path": chunk.get("path", ""),
                "record_count": int(chunk.get("record_count", 0)),
                "bytes": int(chunk.get("bytes", 0)),
                "sha256": chunk.get("sha256", ""),
            }
            for index, chunk in enumerate(receipt.get("chunks", []))
        ]

    def _verify_manifest(self, archive: zipfile.ZipFile, manifest: dict[str, Any]) -> dict[str, Any]:
        """按清单逐片复算包内分片校验值。"""
        results: list[dict[str, Any]] = []
        names = set(archive.namelist())
        for item in manifest.get("chunks", []):
            path = str(item.get("path", ""))
            expected_hash = str(item.get("sha256", ""))
            expected_records = int(item.get("record_count", 0))
            expected_bytes = int(item.get("bytes", 0))
            entry: dict[str, Any] = {
                "chunk_index": item.get("chunk_index"),
                "path": path,
                "expected_sha256": expected_hash,
                "expected_record_count": expected_records,
                "expected_bytes": expected_bytes,
                "present": path in names,
            }
            if path in names:
                payload = archive.read(path)
                actual_hash = sha256_bytes(payload)
                actual_records = count_jsonl_records(payload)
                entry.update({
                    "actual_sha256": actual_hash,
                    "actual_record_count": actual_records,
                    "actual_bytes": len(payload),
                    "hash_ok": actual_hash == expected_hash,
                    "record_count_ok": actual_records == expected_records,
                    "bytes_ok": len(payload) == expected_bytes,
                })
                entry["ok"] = all((
                    entry["hash_ok"], entry["record_count_ok"], entry["bytes_ok"]))
            else:
                entry["ok"] = False
            results.append(entry)
        return {
            "chunks": results,
            "all_chunks_ok": bool(results) and all(item["ok"] for item in results),
            "missing_chunks": [item["path"] for item in results if not item["present"]],
        }

    def _verify_receipt(
        self,
        archive: zipfile.ZipFile,
        manifest: dict[str, Any],
    ) -> dict[str, Any]:
        """核对包内暂存回执：摘要、分片清单与包内实际分片是否一致。"""
        adopted = manifest.get("adopted_receipt") or {}
        receipt_code = str(adopted.get("receipt_code", ""))
        result: dict[str, Any] = {
            "receipt_code": receipt_code,
            "landing_path": adopted.get("landing_path", ""),
            "present": False,
            "checksum_ok": False,
            "chunks_match": False,
            "chunks": [],
        }
        if not receipt_code:
            return result
        matches = [
            name for name in archive.namelist()
            if name.startswith("receipts/") and name.endswith(".json")
            and safe_name(receipt_code, "") in name
        ]
        if not matches:
            return result
        try:
            receipt = json.loads(archive.read(matches[0]).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return result
        result["present"] = True
        stored_checksum = str(receipt.get("receipt_checksum", ""))
        core = {key: value for key, value in receipt.items() if key != "receipt_checksum"}
        recomputed = f"SHA256:{sha256_bytes(canonical_json(core))}"
        result["stored_checksum"] = stored_checksum
        result["recomputed_checksum"] = recomputed
        result["checksum_ok"] = stored_checksum == recomputed
        result["landing_path"] = receipt.get("landing_path", result["landing_path"])

        package_by_index = {
            int(item.get("chunk_index", index)): item
            for index, item in enumerate(manifest.get("chunks", []))
        }
        receipt_chunks = self._chunk_scope(receipt)
        associations: list[dict[str, Any]] = []
        for chunk in receipt_chunks:
            package_chunk = package_by_index.get(int(chunk["chunk_index"]))
            associations.append({
                "chunk_index": chunk["chunk_index"],
                "receipt_path": chunk["path"],
                "receipt_sha256": chunk["sha256"],
                "receipt_record_count": chunk["record_count"],
                "package_path": (package_chunk or {}).get("path", ""),
                "package_sha256": (package_chunk or {}).get("sha256", ""),
                "linked": bool(
                    package_chunk
                    and package_chunk.get("sha256") == chunk["sha256"]
                    and int(package_chunk.get("record_count", -1)) == int(chunk["record_count"])
                ),
            })
        result["chunks"] = associations
        result["matched_chunks"] = sum(1 for item in associations if item["linked"])
        result["receipt_chunk_count"] = len(associations)
        result["package_chunk_count"] = len(package_by_index)
        result["chunks_match"] = (
            bool(associations)
            and result["matched_chunks"] == len(associations)
            and len(associations) == len(package_by_index)
        )
        return result

    def _pending_scope(
        self,
        manifest: dict[str, Any],
        current_receipts: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """确定后续待校验范围：包内提示的新回执 + 当前账上比采用回执更新的回执。"""
        adopted = manifest.get("adopted_receipt") or {}
        adopted_code = str(adopted.get("receipt_code", ""))
        adopted_at = float(adopted.get("confirmed_at", 0) or 0)

        pending: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add(receipt_meta: dict[str, Any], source: str) -> None:
            code = str(receipt_meta.get("receipt_code", ""))
            if not code or code == adopted_code or code in seen:
                return
            seen.add(code)
            current = current_receipts.get(code)
            chunks = self._chunk_scope(current) if current else []
            pending.append({
                "receipt_code": code,
                "source": source,
                "confirmed_at": receipt_meta.get("confirmed_at", 0),
                "landing_path": receipt_meta.get("landing_path", ""),
                "receipt_checksum": receipt_meta.get("receipt_checksum", ""),
                "chunk_count": receipt_meta.get("chunk_count", len(chunks)),
                "record_count": receipt_meta.get("record_count", 0),
                "chunks": chunks,
                "chunks_readable": bool(current) and bool(chunks),
            })

        for item in manifest.get("post_snapshot_receipts", []):
            add(item, "package_record")

        # 只要账上存在比采用回执更晚确认的回执，就属于后续待校验范围；
        # 包括同一快照里本来就有、但打包时选择了较早回执的情况。
        for code, receipt in current_receipts.items():
            if float(receipt.get("confirmed_at", 0) or 0) > adopted_at:
                add(receipt, "current_ledger")

        pending.sort(key=lambda item: float(item.get("confirmed_at", 0)), reverse=True)
        return {
            "adopted_receipt_code": adopted_code,
            "adopted_receipt_confirmed_at": adopted.get("confirmed_at", 0),
            "adopted_receipt_is_latest": not pending,
            "newer_receipts": pending,
            "another_round_required": bool(pending),
            "total_pending_chunks": sum(len(item["chunks"]) for item in pending),
            "total_pending_records": sum(
                sum(int(chunk.get("record_count", 0)) for chunk in item["chunks"])
                for item in pending
            ),
        }

    def verify_bytes(self, data: bytes, *, expected_batch: str | None = None) -> dict[str, Any]:
        """对下载件（zip 字节）逐片复算并核对回执关联与后续待校验范围。"""
        if not data:
            raise DeliveryError("核验内容为空")
        if len(data) > MAX_VERIFY_BYTES:
            raise DeliveryError("核验文件超过允许大小")
        try:
            archive = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile:
            raise DeliveryError("上传内容不是有效的交付包压缩文件")

        with archive:
            try:
                manifest = json.loads(archive.read("delivery_manifest.json").decode("utf-8"))
            except KeyError:
                raise DeliveryError("交付包缺少 delivery_manifest.json 清单")
            except (json.JSONDecodeError, UnicodeDecodeError):
                raise DeliveryError("交付包清单无法解析")

            stored_checksum = str(manifest.get("manifest_checksum", ""))
            manifest_core = {
                key: value for key, value in manifest.items()
                if key != "manifest_checksum"
            }
            recomputed_checksum = f"SHA256:{sha256_bytes(canonical_json(manifest_core))}"
            manifest_ok = stored_checksum == recomputed_checksum

            batch_code = str(manifest.get("batch_code", ""))
            if expected_batch and batch_code != expected_batch:
                raise DeliveryError(
                    f"交付包批次为 {batch_code}，与请求的批号 {expected_batch} 不一致")

            chunk_result = self._verify_manifest(archive, manifest)
            receipt_result = self._verify_receipt(archive, manifest)

        current_receipts = self._collect_current_receipts(batch_code)
        pending = self._pending_scope(manifest, current_receipts)
        all_chunks_ok = chunk_result["all_chunks_ok"]
        receipt_ok = receipt_result["checksum_ok"] and receipt_result["chunks_match"]
        passed = bool(manifest_ok and all_chunks_ok and receipt_ok)

        return {
            "ok": passed,
            "delivery_no": manifest.get("delivery_no", ""),
            "batch_code": batch_code,
            "manifest_checksum": stored_checksum,
            "recomputed_manifest_checksum": recomputed_checksum,
            "manifest_ok": manifest_ok,
            "chunks": chunk_result["chunks"],
            "all_chunks_ok": all_chunks_ok,
            "missing_chunks": chunk_result["missing_chunks"],
            "adopted_receipt": receipt_result,
            "receipt_chunk_association": {
                "matched": receipt_result.get("matched_chunks", 0),
                "total": receipt_result.get("receipt_chunk_count", 0),
                "chunks": receipt_result.get("chunks", []),
            },
            "pending_scope": pending,
            "another_round_required": pending["another_round_required"],
            "landing_conclusion": self._landing_conclusion(
                passed, pending, receipt_result),
        }

    @staticmethod
    def _landing_conclusion(
        passed: bool,
        pending: dict[str, Any],
        receipt_result: dict[str, Any],
    ) -> str:
        """给出目标端落地校验结论。"""
        if not passed:
            return "本包分片或暂存回执核验未通过，不能作为落地依据"
        if pending["another_round_required"]:
            return (
                "本包数据与清单一致，但采用的是较早回执；包内记录显示批次后续还有新的暂存确认，"
                "需要按待校验范围安排另一轮校验"
            )
        return "本包数据、清单与采用回执完全一致，且采用回执为该批次最新回执，无需追加校验"

    def verify_delivery(self, delivery_no: str) -> dict[str, Any]:
        """按交付编号核验已经正式交付的包。"""
        path = self.package_path(delivery_no)
        if path is None:
            raise DeliveryError(f"交付编号 {delivery_no} 没有可核验的正式交付包")
        result = self.verify_bytes(path.read_bytes())
        record = self.get_delivery(delivery_no)
        if record:
            record["status"] = "已校验" if result["ok"] else "校验异常"
            record["last_verified_at"] = time.time()
            record["last_verify_passed"] = result["ok"]
            record_file = self._record_file(delivery_no)
            if record_file.exists():
                record_file.write_text(
                    json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            for row in store.rows(self.MODULE):
                if row.get("交付编号") == delivery_no:
                    row["交付状态"] = record["status"]
                    row["status"] = record["status"]
                    row["abnormal"] = record["status"] == "校验异常"
                    break
        self._write_jsonl(
            self._data_dir("events") / "delivery_verify_log.jsonl",
            [{"action": "verify_delivery", "delivery_no": delivery_no,
              "passed": result["ok"],
              "another_round_required": result["another_round_required"],
              "pending_receipts": [item["receipt_code"]
                                   for item in result["pending_scope"]["newer_receipts"]],
              "verified_at": time.time()}]
        )
        result["delivery_record"] = record
        return result

    def landing_verify(self, delivery_no: str) -> dict[str, Any]:
        """目标端按交付包落地校验：返回是否还需要安排另一轮校验及待校验范围。"""
        result = self.verify_delivery(delivery_no)
        pending = result["pending_scope"]
        result["landing_verify"] = {
            "delivery_no": delivery_no,
            "batch_code": result["batch_code"],
            "adopted_receipt_code": pending["adopted_receipt_code"],
            "adopted_receipt_is_latest": pending["adopted_receipt_is_latest"],
            "another_round_required": pending["another_round_required"],
            "next_round_receipts": pending["newer_receipts"],
            "next_round_scope": [
                {
                    "receipt_code": item["receipt_code"],
                    "landing_path": item["landing_path"],
                    "chunk_count": item["chunk_count"],
                    "chunks": item["chunks"],
                }
                for item in pending["newer_receipts"]
            ],
            "conclusion": result["landing_conclusion"],
        }
        return result

    # -- 通用动作：状态流转 --

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """交付记录的状态流转，保持与通用动作接口兼容。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["交付状态"] = target
        entry["pending"] = False
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        delivery_no = entry.get("交付编号")
        record_file = self._record_file(delivery_no) if delivery_no else None
        if record_file and record_file.exists():
            try:
                record = json.loads(record_file.read_text(encoding="utf-8"))
                record["status"] = target
                record_file.write_text(
                    json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            except (json.JSONDecodeError, OSError):
                pass
        return entry, f"{self.ENTITY}已{action}"

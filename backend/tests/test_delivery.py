"""集成测试：传输交付数据包的打包、中断重做、回执核查与落地校验。

覆盖需求中的关键约束：
- 同一批次先后两次暂存确认（第二次改用新路径），按批号只输入批号即可打包；
- 打包与并发的暂存确认隔离，包内数据与清单来自同一次交付；
- 打包中断不产生正式交付记录，重做后可看到两次正式交付；
- 已正式交付的包按交付号仍可下载核验；
- 下载件逐片复算、回执分片关联与后续待校验范围可核查。
"""
from __future__ import annotations

import io
import json
import shutil
import zipfile

import pytest

from app.services.base import DATA_ROOT
from app.services.staging_area import StagingAreaService
from app.services.transfer_batch import TransferBatchService
from app.services.transfer_delivery import (
    DeliveryError,
    DeliveryInterrupted,
    TransferDeliveryService,
)
from app.store import store


@pytest.fixture(autouse=True)
def isolated_data():
    """每个用例重置内存表和交付相关磁盘产物，避免跨用例的同名批次互相污染。"""
    from app.seed import SEED_ROWS

    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    shutil.rmtree(DATA_ROOT, ignore_errors=True)
    yield
    shutil.rmtree(DATA_ROOT, ignore_errors=True)


@pytest.fixture()
def batch_with_two_receipts():
    """准备一个批次：第一次暂存确认用默认路径，第二次改用新路径。"""
    tb = TransferBatchService()
    sa = StagingAreaService()
    batch = tb.create_batch("EXTR-DELIVERY-TEST", 10, "TARG-0001")
    transfer = tb.start_transfer(batch["batch_id"], chunk_size=3)
    batch_code = batch["batch_code"]

    first_id = transfer["auto_staging"]["staging_id"]
    first = sa.confirm_land(first_id)
    second_landing = sa.land_batch(batch_code, 10, landing_path="/staging/delivery-new/")
    second = sa.confirm_land(second_landing["staging_id"])
    return {
        "batch_code": batch_code,
        "first": first,
        "second": second,
        "landing_path": "/staging/delivery-new/",
    }


def _manifest_from_package(path) -> dict:
    with zipfile.ZipFile(path) as archive:
        return json.loads(archive.read("delivery_manifest.json").decode("utf-8"))


def _chunk_bytes_from_package(path, chunk_path: str) -> bytes:
    with zipfile.ZipFile(path) as archive:
        return archive.read(chunk_path)


class TestDeliveryPackage:
    def test_batch_number_only_builds_latest_receipt(self, batch_with_two_receipts):
        """只输入批号即可打包，默认采用最新暂存回执（新路径）。"""
        svc = TransferDeliveryService()
        result = svc.build_package(batch_with_two_receipts["batch_code"])

        assert result["adopted_receipt"]["receipt_code"] == \
            batch_with_two_receipts["second"]["receipt_code"]
        assert result["adopted_receipt"]["landing_path"] == batch_with_two_receipts["landing_path"]
        assert result["chunk_count"] > 0

        path = svc.package_path(result["delivery_no"])
        assert path is not None and path.is_file()
        manifest = _manifest_from_package(path)
        assert manifest["batch_code"] == batch_with_two_receipts["batch_code"]
        # 包内清单与采用回执必须来自同一份暂存回执。
        receipt_files = [
            name for name in zipfile.ZipFile(path).namelist()
            if name.startswith("receipts/")
        ]
        assert len(receipt_files) == 1
        assert result["adopted_receipt"]["receipt_code"] in receipt_files[0]

    def test_manifest_matches_chunk_bytes_from_same_delivery(self, batch_with_two_receipts):
        """压缩包里的数据与清单来自同一次交付，可照清单逐片复算校验。"""
        svc = TransferDeliveryService()
        result = svc.build_package(batch_with_two_receipts["batch_code"])
        path = svc.package_path(result["delivery_no"])
        manifest = _manifest_from_package(path)

        for chunk in manifest["chunks"]:
            payload = _chunk_bytes_from_package(path, chunk["path"])
            import hashlib
            assert hashlib.sha256(payload).hexdigest() == chunk["sha256"]
            assert len(payload) == chunk["bytes"]
            assert len([line for line in payload.splitlines() if line.strip()]) == \
                chunk["record_count"]

    def test_verify_download_matches_receipt_and_scope(self, batch_with_two_receipts):
        """用下载件复算校验，并核查采用回执关联的分片与后续待校验范围。"""
        svc = TransferDeliveryService()
        result = svc.build_package(
            batch_with_two_receipts["batch_code"],
            receipt_code=batch_with_two_receipts["first"]["receipt_code"],
        )
        path = svc.package_path(result["delivery_no"])
        verified = svc.verify_bytes(path.read_bytes())

        assert verified["ok"] is True
        assert verified["manifest_ok"] is True
        assert verified["all_chunks_ok"] is True
        assert verified["adopted_receipt"]["chunks_match"] is True
        association = verified["receipt_chunk_association"]
        assert association["matched"] == association["total"] > 0
        for item in association["chunks"]:
            assert item["linked"] is True
            assert item["receipt_sha256"] == item["package_sha256"]

        # 采用较早回执：后续还有一次新路径暂存确认，仍需另一轮校验。
        assert verified["another_round_required"] is True
        pending = verified["pending_scope"]["newer_receipts"]
        assert [item["receipt_code"] for item in pending] == \
            [batch_with_two_receipts["second"]["receipt_code"]]
        assert pending[0]["landing_path"] == batch_with_two_receipts["landing_path"]
        assert pending[0]["chunks_readable"] is True
        assert verified["landing_conclusion"].startswith("本包数据与清单一致")

    def test_confirm_new_landing_path_before_packing(self, batch_with_two_receipts):
        """成包前确认新路径：采用旧路径回执却要求新路径时拒绝打包。"""
        svc = TransferDeliveryService()
        with pytest.raises(DeliveryError):
            svc.build_package(
                batch_with_two_receipts["batch_code"],
                receipt_code=batch_with_two_receipts["first"]["receipt_code"],
                landing_path=batch_with_two_receipts["landing_path"],
                confirm_landing_path=True,
            )
        # 路径一致时可以成包。
        result = svc.build_package(
            batch_with_two_receipts["batch_code"],
            receipt_code=batch_with_two_receipts["second"]["receipt_code"],
            landing_path=batch_with_two_receipts["landing_path"],
            confirm_landing_path=True,
        )
        assert result["adopted_receipt"]["landing_path"] == \
            batch_with_two_receipts["landing_path"]

    def test_concurrent_confirmation_does_not_mix_into_package(self, batch_with_two_receipts):
        """打包未完成时同时完成的暂存确认不会混入包内数据与清单。"""
        svc = TransferDeliveryService()
        sa = StagingAreaService()
        batch_code = batch_with_two_receipts["batch_code"]
        observed = {}

        def concurrent_confirm(_ctx):
            landing = sa.land_batch(batch_code, 10, landing_path="/staging/during-pack/")
            receipt = sa.confirm_land(landing["staging_id"])
            observed["receipt"] = receipt

        result = svc.build_package(
            batch_code, after_snapshot_hook=concurrent_confirm, pack_pause_seconds=0.01)

        path = svc.package_path(result["delivery_no"])
        manifest = _manifest_from_package(path)
        receipt_codes = {item["receipt_code"] for item in manifest["receipt_snapshot"]}
        assert observed["receipt"]["receipt_code"] not in receipt_codes
        # 新回执只作为快照后观察记录出现在提示里，不携带其分片字节。
        assert manifest["adopted_receipt"]["receipt_code"] not in {
            item["receipt_code"] for item in manifest["post_snapshot_receipts"]}
        assert [item["receipt_code"] for item in manifest["post_snapshot_receipts"]] == \
            [observed["receipt"]["receipt_code"]]
        verified = svc.verify_delivery(result["delivery_no"])
        assert verified["ok"] is True
        assert verified["another_round_required"] is True

    def test_interrupted_build_creates_no_formal_record_then_redo(self, batch_with_two_receipts):
        """中断未完成的导出不留正式交付；重做后可见两次正式交付记录。"""
        svc = TransferDeliveryService()
        batch_code = batch_with_two_receipts["batch_code"]

        with pytest.raises(DeliveryInterrupted):
            svc.build_package(batch_code, interrupt_after_chunks=1)
        assert svc.list_deliveries(batch_code) == []

        first = svc.build_package(batch_code)
        second = svc.build_package(
            batch_code, receipt_code=batch_with_two_receipts["first"]["receipt_code"])
        deliveries = svc.list_deliveries(batch_code)
        assert len(deliveries) == 2
        assert {row["交付编号"] for row in deliveries} == \
            {first["delivery_no"], second["delivery_no"]}

    def test_redownload_after_interruption_keeps_previous_delivery(self, batch_with_two_receipts):
        """中断后重新下载时，此前已正式交付的包仍可按交付号核验。"""
        svc = TransferDeliveryService()
        batch_code = batch_with_two_receipts["batch_code"]
        first = svc.build_package(batch_code)
        first_bytes = svc.package_path(first["delivery_no"]).read_bytes()

        with pytest.raises(DeliveryInterrupted):
            svc.build_package(batch_code, interrupt_after_chunks=2)

        # 旧包仍在磁盘上，按交付号可下载且核验通过。
        again = svc.package_path(first["delivery_no"])
        assert again is not None and again.read_bytes() == first_bytes
        verified = svc.verify_delivery(first["delivery_no"])
        assert verified["ok"] is True
        assert verified["delivery_no"] == first["delivery_no"]

    def test_landing_verify_reports_next_round_scope(self, batch_with_two_receipts):
        """目标端落地校验：采用较早回执时给出另一轮校验的待校验范围。"""
        svc = TransferDeliveryService()
        result = svc.build_package(
            batch_with_two_receipts["batch_code"],
            receipt_code=batch_with_two_receipts["first"]["receipt_code"],
        )
        landing = svc.landing_verify(result["delivery_no"])
        detail = landing["landing_verify"]
        assert detail["another_round_required"] is True
        assert detail["adopted_receipt_is_latest"] is False
        assert detail["next_round_scope"][0]["landing_path"] == \
            batch_with_two_receipts["landing_path"]
        assert len(detail["next_round_scope"][0]["chunks"]) == \
            detail["next_round_receipts"][0]["chunk_count"]

    def test_tampered_download_fails_chunk_recompute(self, batch_with_two_receipts):
        """下载件被改动后，逐片复算必须发现与清单不符。"""
        svc = TransferDeliveryService()
        result = svc.build_package(batch_with_two_receipts["batch_code"])
        path = svc.package_path(result["delivery_no"])
        with zipfile.ZipFile(path) as archive:
            entries = {name: archive.read(name) for name in archive.namelist()}
        chunk_name = sorted(name for name in entries if name.startswith("chunks/"))[0]
        entries[chunk_name] = entries[chunk_name] + b'{"tampered": true}\n'
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            for name, payload in entries.items():
                archive.writestr(name, payload)

        verified = svc.verify_bytes(buffer.getvalue())
        assert verified["ok"] is False
        assert verified["all_chunks_ok"] is False

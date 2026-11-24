"""传输交付数据包接口：暂存回执查询、按批号打包下载、按交付号下载与核验。

这些接口都挂在 /api/delivery 下，不走通用 /api/{module} 动态路由，避免与模块名冲突。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from app.services.transfer_delivery import (
    DeliveryError,
    DeliveryInterrupted,
    TransferDeliveryService,
)

router = APIRouter(prefix="/api/delivery", tags=["传输交付数据包"])

MAX_UPLOAD_BYTES = 64 * 1024 * 1024


def service() -> TransferDeliveryService:
    return TransferDeliveryService()


@router.get("/receipts")
def list_receipts(batch: str = Query(..., description="批次编号")) -> dict[str, Any]:
    """列出批次全部暂存回执；先前回执仍可按回执编号核查。"""
    items = service().list_receipts(batch.strip())
    return {"batch_code": batch.strip(), "items": items, "total": len(items)}


@router.get("/deliveries")
def list_deliveries(batch: str = Query(..., description="批次编号")) -> dict[str, Any]:
    """按批号列出正式交付记录（不含中断的打包尝试）。"""
    items = service().list_deliveries(batch.strip())
    return {"batch_code": batch.strip(), "items": items, "total": len(items)}


@router.post("/package")
async def build_package(request: Request) -> dict[str, Any]:
    """按批号请求打包：只输入批号即可，打包前确认采用的新落地路径。"""
    payload = await _json_body(request)
    batch = str(payload.get("batch") or payload.get("batch_code") or "").strip()
    if not batch:
        raise HTTPException(status_code=400, detail="请提供批号 batch")
    svc = service()
    try:
        result = svc.build_package(
            batch,
            receipt_code=_optional_str(payload.get("receipt_code")),
            landing_path=_optional_str(payload.get("landing_path")),
            confirm_landing_path=bool(payload.get("confirm_landing_path")),
            interrupt_after_chunks=_optional_int(payload.get("interrupt_after_chunks")),
        )
    except DeliveryInterrupted as exc:
        # 打包未完成：明确告知没有生成正式交付记录，可直接重做。
        raise HTTPException(
            status_code=409,
            detail={"message": str(exc), "formal_delivery_created": False},
        )
    except DeliveryError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {
        "ok": True,
        "delivery_no": result["delivery_no"],
        "batch_code": result["batch_code"],
        "adopted_receipt": result["adopted_receipt"],
        "chunk_count": result["chunk_count"],
        "record_count": result["record_count"],
        "total_bytes": result["total_bytes"],
        "delivery_checksum": result["delivery_checksum"],
        "package_sha256": result["package_sha256"],
        "download_url": f"/api/delivery/{result['delivery_no']}/download",
    }


@router.get("/{delivery_no}/download")
def download_package(delivery_no: str) -> Response:
    """按交付号下载已经正式交付的包；中断重下后旧包仍可下载。"""
    svc = service()
    path = svc.package_path(delivery_no)
    if path is None:
        raise HTTPException(status_code=404, detail=f"交付编号 {delivery_no} 没有可下载的正式交付包")
    return Response(
        content=path.read_bytes(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{delivery_no}.zip"',
            "X-Delivery-No": delivery_no,
        },
    )


@router.post("/{delivery_no}/verify")
def verify_delivery(delivery_no: str) -> dict[str, Any]:
    """按交付号核验正式交付包，并给出后续待校验范围。"""
    svc = service()
    try:
        result = svc.verify_delivery(delivery_no)
    except DeliveryError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _verify_payload(result)


@router.post("/{delivery_no}/landing-verify")
def landing_verify(delivery_no: str) -> dict[str, Any]:
    """目标端按交付包落地校验：确认是否需要安排另一轮校验。"""
    svc = service()
    try:
        result = svc.landing_verify(delivery_no)
    except DeliveryError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _verify_payload(result)


@router.post("/verify-upload")
async def verify_upload(request: Request, batch: str | None = Query(default=None)) -> dict[str, Any]:
    """用下载件（zip 原始字节）复算校验，核对采用回执关联分片与后续待校验范围。"""
    body = await request.body()
    svc = service()
    try:
        result = svc.verify_bytes(body, expected_batch=batch)
    except DeliveryError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _verify_payload(result)


@router.get("/{delivery_no}")
def get_delivery(delivery_no: str) -> dict[str, Any]:
    """按交付号读取正式交付记录（含清单摘要与回执引用）。"""
    record = service().get_delivery(delivery_no)
    if record is None:
        raise HTTPException(status_code=404, detail=f"交付编号 {delivery_no} 不存在")
    return record


async def _json_body(request: Request) -> dict[str, Any]:
    """读取 JSON 请求体；空体按空对象处理。"""
    raw = await request.body()
    if not raw:
        return {}
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")
    return payload


def _optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="interrupt_after_chunks 必须是整数")


def _verify_payload(result: dict[str, Any]) -> dict[str, Any]:
    """把核验结果整理成接口返回结构。"""
    payload = {
        "ok": result["ok"],
        "delivery_no": result.get("delivery_no", ""),
        "batch_code": result.get("batch_code", ""),
        "manifest_ok": result.get("manifest_ok"),
        "manifest_checksum": result.get("manifest_checksum", ""),
        "recomputed_manifest_checksum": result.get("recomputed_manifest_checksum", ""),
        "all_chunks_ok": result.get("all_chunks_ok"),
        "chunks": result.get("chunks", []),
        "missing_chunks": result.get("missing_chunks", []),
        "adopted_receipt": result.get("adopted_receipt", {}),
        "receipt_chunk_association": result.get("receipt_chunk_association", {}),
        "pending_scope": result.get("pending_scope", {}),
        "another_round_required": result.get("another_round_required"),
        "landing_conclusion": result.get("landing_conclusion", ""),
    }
    if "landing_verify" in result:
        payload["landing_verify"] = result["landing_verify"]
    if "delivery_record" in result:
        record = result["delivery_record"] or {}
        payload["delivery_record"] = {
            "交付编号": record.get("delivery_no", ""),
            "批次编号": record.get("batch_code", ""),
            "采用回执": (record.get("adopted_receipt") or {}).get("receipt_code", ""),
            "交付状态": record.get("status", ""),
        }
    return payload

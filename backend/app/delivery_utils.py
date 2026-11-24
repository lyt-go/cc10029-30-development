"""交付包通用工具：文件名清理、逐片校验值、清单规范化与安全打包。

交付包要求“压缩包里的数据与清单来自同一次交付”，因此所有清单条目都必须基于
打包时读到的字节计算，不能再次从可能已被后续落地确认改写的路径取数。
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

# 单个交付包允许的最大校验上传体积（字节）。下载件回传复算时防止内存被打满。
MAX_VERIFY_BYTES = 64 * 1024 * 1024


def safe_name(value: str, fallback: str = "item") -> str:
    """把批号/交付号转换成可安全用作目录名的片段。"""
    cleaned = re.sub(r"[^0-9A-Za-z._\-一-鿿]+", "_", str(value or "")).strip("._")
    return cleaned or fallback


def sha256_bytes(data: bytes) -> str:
    """返回字节内容的完整 SHA256 十六进制摘要。"""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    """按块读取文件并返回完整 SHA256 摘要。"""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json(payload: Any) -> bytes:
    """输出稳定的 JSON 字节，用于清单自校验与跨端复算。"""
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def jsonl_bytes(records: list[dict[str, Any]]) -> bytes:
    """按传输产物相同的序列化规则输出 jsonl 字节。"""
    return "".join(
        json.dumps(record, ensure_ascii=False, default=str) + "\n" for record in records
    ).encode("utf-8")


def count_jsonl_records(data: bytes) -> int:
    """统计 jsonl 字节中的非空行数。"""
    return sum(1 for line in data.decode("utf-8", errors="replace").splitlines() if line.strip())


def chunk_entry(index: int, data: bytes) -> dict[str, Any]:
    """构造一条分片清单条目：序号、包内路径、条数、字节数与逐片校验值。"""
    return {
        "chunk_index": index,
        "path": f"chunks/chunk_{index:04d}.jsonl",
        "record_count": count_jsonl_records(data),
        "bytes": len(data),
        "sha256": sha256_bytes(data),
    }

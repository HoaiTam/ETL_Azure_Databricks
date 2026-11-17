"""Chuẩn hóa tên tiếng Anh trước khi ghép Instacart với Open Food Facts."""

from __future__ import annotations

import re

_NOISE = re.compile(r"\b(?:organic|bag\s+of|pack|\d+(?:\.\d+)?\s*(?:fl\s*oz|oz|lb|ct|fl|ml|g|kg))\b")
_PUNCTUATION = re.compile(r"[^a-z0-9 ]")
_WHITESPACE = re.compile(r"\s+")


def norm(s: str | None) -> str:
    """Hạ chữ, bỏ từ nhiễu/đơn vị đo và dấu câu; None thành chuỗi rỗng.

    Bỏ số đo trước dấu câu để 12.5 oz không bị tách thành 12 và 5 oz.
    Lượt sau nhận cả cụm bag-of sau khi dấu gạch được đổi thành khoảng trắng.
    Không dịch tên, suy đoán thương hiệu hoặc quyết định hai sản phẩm có khớp nhau.
    """
    s = _NOISE.sub(" ", (s or "").lower())
    s = _PUNCTUATION.sub(" ", s)
    return _WHITESPACE.sub(" ", _NOISE.sub(" ", s)).strip()

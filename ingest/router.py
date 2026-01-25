from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

try:
    import pdfplumber
except Exception:  # pragma: no cover - optional dependency
    pdfplumber = None


@dataclass
class RouterResult:
    juri_prog_pages: list[int]
    metas_pages: list[int]
    page_texts: dict[int, str]
    used_fallback: bool


_JURI_PROG_KEYWORDS = [
    "jurisdic",
    "programa",
    "presupuesto",
]
_METAS_KEYWORDS = [
    "metas",
    "evolucion",
    "evoluci",
    "principales",
]


def _score_page(text: str, keywords: Iterable[str]) -> int:
    text_lower = text.lower()
    return sum(1 for key in keywords if key in text_lower)


def _extract_page_texts(pdf_path: str) -> dict[int, str]:
    if pdfplumber is None:
        return {}
    page_texts: dict[int, str] = {}
    with pdfplumber.open(pdf_path) as pdf:
        for index, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            page_texts[index] = text
    return page_texts


def route_sections(pdf_path: str) -> RouterResult:
    page_texts = _extract_page_texts(pdf_path)
    juri_prog_pages: list[int] = []
    metas_pages: list[int] = []
    for page_num, text in page_texts.items():
        if _score_page(text, _JURI_PROG_KEYWORDS) >= 2:
            juri_prog_pages.append(page_num)
        if _score_page(text, _METAS_KEYWORDS) >= 2:
            metas_pages.append(page_num)

    used_fallback = False
    if not juri_prog_pages and not metas_pages:
        used_fallback = True

    return RouterResult(
        juri_prog_pages=juri_prog_pages,
        metas_pages=metas_pages,
        page_texts=page_texts,
        used_fallback=used_fallback,
    )

"""Extract and conservatively classify issuer evidence from SEC submissions."""

from __future__ import annotations

import html
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any


CLASSIFIER_VERSION = "sec-primary-catalyst-v1"
DIRECT_MATERIAL_ITEMS = frozenset({"1.01", "2.01", "2.02"})

_DILUTION_OR_FINANCING = (
    "at-the-market offering",
    "at the market offering",
    "registered direct offering",
    "public offering of common stock",
    "private placement of common stock",
    "securities purchase agreement",
    "equity line of credit",
    "common stock purchase agreement",
    "sale of shares of common stock",
    "issuance of shares of common stock",
    "convertible notes offering",
    "convertible senior notes offering",
)
_NEGATIVE_EVENT = (
    "lowers full-year guidance",
    "lowers its full-year guidance",
    "reduces full-year guidance",
    "withdraws full-year guidance",
    "going concern",
    "delisting notice",
    "workforce reduction",
    "reduction in force",
)
_POSITIVE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "GUIDANCE_RAISE",
        re.compile(
            r"\b(?:raises?|increases?) (?:its )?(?:full[- ]year )?guidance\b", re.I
        ),
    ),
    (
        "REGULATORY_APPROVAL",
        re.compile(
            r"\b(?:fda (?:approves?|approval)|receives? (?:fda |regulatory )?approval|granted (?:accelerated )?approval)\b",
            re.I,
        ),
    ),
    (
        "POSITIVE_CLINICAL_RESULTS",
        re.compile(
            r"\b(?:positive (?:top[- ]?line |phase [123] )?(?:clinical |trial )?results|(?:met|achieved) (?:its |the )?primary endpoint)\b",
            re.I,
        ),
    ),
    (
        "CONTRACT_AWARD",
        re.compile(r"\b(?:awarded|wins?) (?:a |an )?[^.]{0,120}\bcontract\b", re.I),
    ),
    (
        "RECORD_RESULTS",
        re.compile(
            r"\b(?:reports?|announces?) (?:record|strongest)[^.]{0,100}\b(?:revenue|sales|earnings|results)\b",
            re.I,
        ),
    ),
)


@dataclass(frozen=True)
class SubmissionDocument:
    document_type: str
    sequence: str
    filename: str
    description: str
    raw_text: str


class _VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._ignored_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "ix:hidden"}:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "ix:hidden"} and self._ignored_depth:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._ignored_depth:
            self.parts.append(data)


def visible_text(raw: str) -> str:
    """Return normalized visible text from an HTML or plain-text document."""
    parser = _VisibleTextParser()
    try:
        parser.feed(raw)
        extracted = " ".join(parser.parts)
    except Exception:
        extracted = raw
    return re.sub(r"\s+", " ", html.unescape(extracted)).strip()


def _sgml_field(block: str, name: str) -> str:
    match = re.search(rf"(?im)^<{re.escape(name)}>\s*([^\r\n<]*)", block)
    return match.group(1).strip() if match else ""


def parse_submission_documents(raw: str) -> list[SubmissionDocument]:
    """Parse EDGAR's complete-submission SGML document envelopes."""
    documents: list[SubmissionDocument] = []
    for match in re.finditer(r"(?is)<DOCUMENT>(.*?)</DOCUMENT>", raw):
        block = match.group(1)
        text_match = re.search(r"(?is)<TEXT>(.*)</TEXT>", block)
        documents.append(
            SubmissionDocument(
                document_type=_sgml_field(block, "TYPE"),
                sequence=_sgml_field(block, "SEQUENCE"),
                filename=_sgml_field(block, "FILENAME"),
                description=_sgml_field(block, "DESCRIPTION"),
                raw_text=text_match.group(1) if text_match else block,
            )
        )
    return documents


def evidence_documents(
    primary_document: str,
    documents: Sequence[SubmissionDocument],
) -> list[SubmissionDocument]:
    """Select the filing document and issuer communication exhibits."""
    selected = [
        document
        for document in documents
        if document.filename == primary_document
        or document.document_type.upper().startswith("EX-99")
        or re.search(
            r"\b(?:press|news) release\b|\bshareholder letter\b|\bresults presentation\b",
            document.description,
            re.I,
        )
    ]
    unique: dict[tuple[str, str, str], SubmissionDocument] = {}
    for document in selected:
        unique[(document.document_type, document.filename, document.sequence)] = (
            document
        )
    return list(unique.values())


def _matched_phrases(text: str, phrases: Sequence[str]) -> list[str]:
    lowered = text.lower()
    return sorted({phrase for phrase in phrases if phrase in lowered})


def classify_primary_catalyst(
    filing: Mapping[str, Any],
    documents: Sequence[SubmissionDocument],
    *,
    accepted_after_prior_close: bool,
) -> dict[str, Any]:
    """Classify direct materiality, direction, and conflicts without guessing."""
    selected = evidence_documents(str(filing.get("primary_document") or ""), documents)
    texts = [visible_text(document.raw_text) for document in selected]
    combined = " ".join(texts)
    # Directional phrases must occur near the beginning of an issuer document,
    # where a release headline and lead normally live. Conflict screening scans
    # all retained evidence because financing language may be buried later.
    lead = " ".join(text[:5000] for text in texts)
    dilution = _matched_phrases(combined, _DILUTION_OR_FINANCING)
    negative = _matched_phrases(combined, _NEGATIVE_EVENT)
    positive = [
        {"category": category, "phrase": match.group(0)}
        for category, pattern in _POSITIVE_PATTERNS
        if (match := pattern.search(lead)) is not None
    ]
    items = {str(item) for item in filing.get("items", [])}
    direct_items = sorted(items.intersection(DIRECT_MATERIAL_ITEMS))
    conflict = bool(dilution or negative or filing.get("dilution_conflict") is True)
    direct_material = bool(direct_items)
    positive_direction = bool(positive) and not conflict
    verified_material = (
        accepted_after_prior_close
        and bool(selected)
        and (direct_material or positive_direction)
        and not conflict
    )
    if not accepted_after_prior_close:
        disposition = "STALE_BEFORE_PRIOR_CLOSE"
    elif conflict:
        disposition = "NEGATIVE_OR_FINANCING_CONFLICT"
    elif positive_direction:
        disposition = "VERIFIED_POSITIVE_PRIMARY"
    elif direct_material:
        disposition = "VERIFIED_MATERIAL_DIRECTION_UNRESOLVED"
    else:
        disposition = "UNRESOLVED_PRIMARY_SOURCE"
    return {
        "classifier_version": CLASSIFIER_VERSION,
        "disposition": disposition,
        "accepted_after_prior_close": accepted_after_prior_close,
        "evidence_document_count": len(selected),
        "evidence_documents": [
            {
                "type": document.document_type,
                "sequence": document.sequence,
                "filename": document.filename,
                "description": document.description,
            }
            for document in selected
        ],
        "direct_material_items": direct_items,
        "positive_matches": positive,
        "dilution_matches": dilution,
        "negative_matches": negative,
        "dilution_or_negative_conflict": conflict,
        "verified_material_catalyst": verified_material,
        "verified_positive_direction": positive_direction,
        "direction": (
            "POSITIVE"
            if positive_direction
            else "NEGATIVE_OR_CONFLICT"
            if conflict
            else "UNRESOLVED"
        ),
    }

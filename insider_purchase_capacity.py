"""Outcome-blind SEC Form 4 insider-purchase capacity controller.

This module deliberately stops before market-price or forward-return access.
It freezes exact SEC quarterly bulk archives, independently inspectable
transaction semantics, and a content-addressed normalized event inventory for
the proposed ``clustered-form4-open-market-purchase-continuation`` family.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import subprocess
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import dotenv_values

import portfolio_maturity
import strategy_discovery


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = "clustered-form4-open-market-purchase-continuation"
MECHANISM_FAMILY = "insider-open-market-purchase-continuation"
VERSION_ID = "clustered-form4-open-market-purchase-continuation-v1"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/form4_insider_purchase"
SEC_BASE = (
    "https://www.sec.gov/files/structureddata/data/"
    "insider-transactions-data-sets"
)
SOURCE_START_YEAR = 2018
SOURCE_END_YEAR = 2024
SOURCE_DOCUMENTATION_URL = (
    "https://www.sec.gov/files/insider_transactions_readme.pdf"
)
SOURCE_LANDING_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "insider-transactions-data-sets"
)
COMMON_EQUITY_PATTERN = re.compile(
    r"^(?:(?:class|series) [a-z0-9.-]+ )?common (?:stock|shares)"
    r"(?:[, ].*)?$|^ordinary shares?(?:[, ].*)?$",
    re.IGNORECASE,
)
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
ALLOWED_OWNER_RELATIONSHIPS = frozenset({"OFFICER", "DIRECTOR"})
MINIMUM_FAST_LANE_EVENTS = 100
MINIMUM_LATER_RESEARCH_EVENTS = 50
REQUEST_TIMEOUT_SECONDS = 90
USER_AGENT_ENV = "SEC_USER_AGENT"


class InsiderPurchaseCapacityError(RuntimeError):
    """The frozen source boundary or normalized event inventory is invalid."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return canonical_sha256(
        {key: item for key, item in value.items() if key != field}
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InsiderPurchaseCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise InsiderPurchaseCapacityError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _store_root() -> Path:
    value = dotenv_values(PROJECT_ROOT / ".env").get(
        "LOCAL_HISTORICAL_DATA_ROOT"
    )
    if not isinstance(value, str) or not value.strip():
        raise InsiderPurchaseCapacityError(
            "LOCAL_HISTORICAL_DATA_ROOT is unavailable"
        )
    return Path(value).expanduser().resolve()


def _relative_to_project(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path.resolve())


def _artifact_path(
    root: Path,
    directory: str,
    prefix: str,
    artifact: Mapping[str, Any],
    hash_field: str,
) -> Path:
    digest = str(artifact[hash_field])
    return root / directory / f"{prefix}-{digest}.json"


def _archive_requests() -> list[dict[str, Any]]:
    requests_: list[dict[str, Any]] = []
    ordinal = 0
    for year in range(SOURCE_START_YEAR, SOURCE_END_YEAR + 1):
        for quarter in range(1, 5):
            ordinal += 1
            filename = f"{year}q{quarter}_form345.zip"
            requests_.append(
                {
                    "filename": filename,
                    "ordinal": ordinal,
                    "quarter": quarter,
                    "url": f"{SEC_BASE}/{filename}",
                    "year": year,
                }
            )
    return requests_


def build_contract(*, created_at: str) -> dict[str, Any]:
    datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "form4-insider-purchase-source-contract",
        "artifact_sha256": "",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "version_id": VERSION_ID,
        "created_at": created_at,
        "source": {
            "authority": "U.S. Securities and Exchange Commission",
            "dataset": "Insider Transactions Data Sets",
            "documentation_url": SOURCE_DOCUMENTATION_URL,
            "landing_url": SOURCE_LANDING_URL,
            "period": {
                "start": f"{SOURCE_START_YEAR}-01-01",
                "end": f"{SOURCE_END_YEAR}-12-31",
            },
            "requests": _archive_requests(),
        },
        "event_semantics": {
            "document_type": "4",
            "amendments_permitted": False,
            "transaction_table": "NONDERIV_TRANS",
            "transaction_code": "P",
            "transaction_code_meaning": (
                "open market or private purchase of a non-derivative security"
            ),
            "acquired_disposed_code": "A",
            "direct_ownership_only": True,
            "eligible_owner_relationships": sorted(
                ALLOWED_OWNER_RELATIONSHIPS
            ),
            "security_title_pattern": COMMON_EQUITY_PATTERN.pattern,
            "positive_shares_required": True,
            "positive_per_share_price_required": True,
            "maximum_transaction_to_filing_lag_calendar_days": 4,
            "footnoted_core_transaction_fields": "exclude",
            "rule_10b5_1": (
                "exclude when AFF10B5ONE is present and true; do not infer "
                "the field for older archives"
            ),
            "multiple_transactions": (
                "sum exact eligible purchase notional by accession and issuer"
            ),
            "multiple_owners": (
                "count distinct reporting-owner CIKs by issuer and filing date"
            ),
            "duplicate_policy": "deduplicate only exact transaction keys",
            "entry_observability": (
                "earliest eligible entry is the next complete exchange "
                "session open after SEC filing date"
            ),
        },
        "capacity_disposition": {
            "fast_lane_minimum_verified_events": MINIMUM_FAST_LANE_EVENTS,
            "later_single_rule_minimum_verified_events": (
                MINIMUM_LATER_RESEARCH_EVENTS
            ),
            "below_50": "RETIRE_INSUFFICIENT_FORMAL_CAPACITY",
            "50_through_99": "PRESERVE_LATER_SINGLE_RULE_RESEARCH",
            "100_or_more": "ADMIT_DEVELOPMENT_SEARCH",
        },
        "outcome_boundary": {
            "market_prices_accessed": False,
            "forward_returns_accessed": False,
            "confirmation_accessed": False,
            "broker_actions": 0,
            "provider_access_permitted_before_inspection": False,
            "substitutions_permitted": False,
        },
        "implementation_hashes": {
            "controller_sha256": sha256_file(Path(__file__)),
        },
    }
    contract["artifact_sha256"] = self_hash(contract, "artifact_sha256")
    return contract


def freeze_contract(*, created_at: str, root: Path = DEFAULT_ROOT) -> Path:
    contract = build_contract(created_at=created_at)
    path = _artifact_path(
        root,
        "source-contract",
        "form4-insider-purchase-source-contract",
        contract,
        "artifact_sha256",
    )
    _write_json(path, contract)
    return path


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
) -> Path:
    datetime.fromisoformat(inspected_at.replace("Z", "+00:00"))
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = _read_json(contract_path)
    expected = build_contract(created_at=str(contract.get("created_at")))
    checks = {
        "artifact_hash_rebuilt": contract.get("artifact_sha256")
        == self_hash(contract, "artifact_sha256"),
        "exact_contract_rebuilt": contract == expected,
        "exact_28_quarter_request_graph": contract.get("source", {}).get(
            "requests"
        )
        == _archive_requests(),
        "implementation_hash_rebuilt": contract.get(
            "implementation_hashes", {}
        ).get("controller_sha256")
        == sha256_file(Path(__file__)),
        "outcome_boundary_closed": contract.get("outcome_boundary")
        == expected["outcome_boundary"],
        "source_outputs_absent": True,
    }
    store = _store_root()
    destination = (
        store
        / "_derived/form4_insider_purchase"
        / str(contract["artifact_sha256"])
    )
    if destination.exists():
        checks["source_outputs_absent"] = not any(destination.iterdir())
    valid = all(checks.values())
    inspection: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "form4-insider-purchase-source-contract-inspection",
        "artifact_sha256": "",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "contract_path": _relative_to_project(contract_path),
        "contract_sha256": contract.get("artifact_sha256"),
        "inspected_at": inspected_at,
        "checks": checks,
        "valid": valid,
        "state": (
            "FORM4_SOURCE_CONTRACT_INSPECTED_READY"
            if valid
            else "FORM4_SOURCE_CONTRACT_REJECTED"
        ),
        "provider_access_permitted": valid,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }
    inspection["artifact_sha256"] = self_hash(
        inspection, "artifact_sha256"
    )
    path = _artifact_path(
        root,
        "source-contract-inspection",
        "form4-insider-purchase-source-contract-inspection",
        inspection,
        "artifact_sha256",
    )
    _write_json(path, inspection)
    if not valid:
        raise InsiderPurchaseCapacityError(
            "source contract failed independent inspection"
        )
    return path


def _find_inspection(
    contract: Mapping[str, Any], root: Path
) -> tuple[Path, dict[str, Any]]:
    expected_sha = contract.get("artifact_sha256")
    candidates: list[tuple[Path, dict[str, Any]]] = []
    for path in (root / "source-contract-inspection").glob("*.json"):
        value = _read_json(path)
        if (
            value.get("contract_sha256") == expected_sha
            and value.get("state") == "FORM4_SOURCE_CONTRACT_INSPECTED_READY"
            and value.get("valid") is True
            and value.get("artifact_sha256")
            == self_hash(value, "artifact_sha256")
        ):
            candidates.append((path, value))
    if len(candidates) != 1:
        raise InsiderPurchaseCapacityError(
            "exact committed source-contract inspection is missing or ambiguous"
        )
    return candidates[0]


def _headers() -> dict[str, str]:
    user_agent = os.environ.get(USER_AGENT_ENV)
    if not user_agent:
        user_agent = dotenv_values(PROJECT_ROOT / ".env").get(USER_AGENT_ENV)
    if not isinstance(user_agent, str) or "@" not in user_agent:
        raise InsiderPurchaseCapacityError(
            "SEC_USER_AGENT with a contact email is required"
        )
    return {
        "Accept-Encoding": "gzip, deflate",
        "User-Agent": user_agent.strip(),
    }


def _require_pushed_head() -> str:
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        upstream = subprocess.run(
            ["git", "rev-parse", "@{upstream}"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except subprocess.CalledProcessError as exc:
        raise InsiderPurchaseCapacityError(
            "cannot verify pushed Git authority"
        ) from exc
    if not head or head != upstream:
        raise InsiderPurchaseCapacityError(
            "SEC access requires committed inputs and pushed HEAD"
        )
    return head


def collect(
    contract_path: Path,
    *,
    collected_at: str,
    root: Path = DEFAULT_ROOT,
    http: requests.Session | None = None,
) -> Path:
    datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = _read_json(contract_path)
    inspection_path, inspection = _find_inspection(contract, root)
    strategy_discovery.require_committed(inspection_path)
    published_commit = _require_pushed_head()
    if sha256_file(contract_path) != hashlib.sha256(
        (json.dumps(contract, indent=2, sort_keys=True) + "\n").encode()
    ).hexdigest():
        raise InsiderPurchaseCapacityError(
            "source contract file bytes are not canonical"
        )
    store = _store_root()
    destination = (
        store
        / "_derived/form4_insider_purchase"
        / str(contract["artifact_sha256"])
    )
    destination.mkdir(parents=True, exist_ok=True)
    session = http or requests.Session()
    archive_rows: list[dict[str, Any]] = []
    total_bytes = 0
    request_seconds = 0.0
    for request in contract["source"]["requests"]:
        filename = str(request["filename"])
        path = destination / filename
        if path.exists():
            archive_rows.append(
                {
                    **request,
                    "bytes": path.stat().st_size,
                    "file_sha256": sha256_file(path),
                    "state": "CACHE_HIT",
                }
            )
            total_bytes += path.stat().st_size
            continue
        started = datetime.now().timestamp()
        response = session.get(
            str(request["url"]),
            headers=_headers(),
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        request_seconds += datetime.now().timestamp() - started
        if response.status_code != 200:
            raise InsiderPurchaseCapacityError(
                f"{filename} returned HTTP {response.status_code}"
            )
        payload = bytes(response.content)
        if not zipfile.is_zipfile(io.BytesIO(payload)):
            raise InsiderPurchaseCapacityError(
                f"{filename} is not a valid ZIP archive"
            )
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_bytes(payload)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        archive_rows.append(
            {
                **request,
                "bytes": len(payload),
                "file_sha256": sha256_bytes(payload),
                "state": "COLLECTED",
            }
        )
        total_bytes += len(payload)
    collection: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "form4-insider-purchase-source-collection",
        "artifact_sha256": "",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "contract_path": _relative_to_project(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspection_path": _relative_to_project(inspection_path),
        "inspection_sha256": inspection["artifact_sha256"],
        "collected_at": collected_at,
        "published_commit": published_commit,
        "store_relative_path": (
            f"_derived/form4_insider_purchase/{contract['artifact_sha256']}"
        ),
        "archives": archive_rows,
        "provider_telemetry": {
            "requests": sum(
                row["state"] == "COLLECTED" for row in archive_rows
            ),
            "cache_hits": sum(
                row["state"] == "CACHE_HIT" for row in archive_rows
            ),
            "failures": 0,
            "request_seconds": request_seconds,
            "bytes": total_bytes,
        },
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
        "state": "FORM4_SOURCE_COLLECTED",
    }
    collection["artifact_sha256"] = self_hash(
        collection, "artifact_sha256"
    )
    path = _artifact_path(
        root,
        "source-collection",
        "form4-insider-purchase-source-collection",
        collection,
        "artifact_sha256",
    )
    _write_json(path, collection)
    return path


def _table_name(names: Sequence[str], token: str) -> str:
    matches = [
        name
        for name in names
        if token in Path(name).stem.upper()
        and not name.endswith("/")
        and Path(name).suffix.lower() in {".txt", ".tsv"}
    ]
    if len(matches) != 1:
        raise InsiderPurchaseCapacityError(
            f"archive must contain one {token} table; found {matches}"
        )
    return matches[0]


def _rows(archive: zipfile.ZipFile, name: str) -> Iterable[dict[str, str]]:
    with archive.open(name) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text, delimiter="\t")
        if not reader.fieldnames:
            raise InsiderPurchaseCapacityError(f"{name} has no header")
        for row in reader:
            yield {
                str(key).strip().upper(): (value or "").strip()
                for key, value in row.items()
                if key is not None
            }


def _parse_sec_date(value: str) -> date:
    cleaned = value.strip()
    for pattern in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, pattern).date()
        except ValueError:
            continue
    raise InsiderPurchaseCapacityError(f"invalid SEC date: {value!r}")


def _number(value: str) -> float | None:
    cleaned = value.strip().replace(",", "")
    if not cleaned:
        return None
    try:
        number = float(cleaned)
    except ValueError:
        return None
    if not (number > 0):
        return None
    return number


def _truthy(value: str) -> bool:
    return value.strip().upper() in {"1", "TRUE", "Y", "YES"}


def _core_fields_unfootnoted(row: Mapping[str, str]) -> bool:
    return not any(
        row.get(field, "")
        for field in (
            "SECURITY_TITLE_FN",
            "TRANS_DATE_FN",
            "TRANS_SHARES_FN",
            "TRANS_PRICEPERSHARE_FN",
            "TRANS_ACQUIRED_DISP_CD_FN",
            "DIRECT_INDIRECT_OWNERSHIP_FN",
        )
    )


def normalized_events(
    archive_paths: Sequence[Path],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    submissions: dict[str, dict[str, str]] = {}
    owners: dict[str, set[str]] = defaultdict(set)
    owner_roles: dict[str, set[str]] = defaultdict(set)
    transactions: list[dict[str, str]] = []
    counts: defaultdict[str, int] = defaultdict(int)
    for path in archive_paths:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            submission_name = _table_name(names, "SUBMISSION")
            owner_name = _table_name(names, "REPORTINGOWNER")
            transaction_name = _table_name(names, "NONDERIV_TRANS")
            for row in _rows(archive, submission_name):
                accession = row.get("ACCESSION_NUMBER", "")
                if not accession:
                    raise InsiderPurchaseCapacityError(
                        f"{submission_name} contains a missing accession"
                    )
                if accession in submissions:
                    raise InsiderPurchaseCapacityError(
                        f"duplicate submission accession {accession}"
                    )
                submissions[accession] = row
                counts["submission_rows"] += 1
            for row in _rows(archive, owner_name):
                accession = row.get("ACCESSION_NUMBER", "")
                owner = row.get("RPTOWNERCIK", "")
                if accession and owner:
                    owners[accession].add(owner)
                    relationship = row.get("RPTOWNER_RELATIONSHIP", "")
                    for part in re.split(r"[,|;/ ]+", relationship.upper()):
                        if part:
                            owner_roles[accession].add(part)
                counts["reporting_owner_rows"] += 1
            for row in _rows(archive, transaction_name):
                transactions.append(row)
                counts["nonderivative_transaction_rows"] += 1
    exact_seen: set[tuple[str, ...]] = set()
    eligible: list[dict[str, Any]] = []
    for row in transactions:
        accession = row.get("ACCESSION_NUMBER", "")
        submission = submissions.get(accession)
        if submission is None:
            counts["missing_submission"] += 1
            continue
        if submission.get("DOCUMENT_TYPE", "").upper() != "4":
            counts["excluded_document_type"] += 1
            continue
        if _truthy(submission.get("AFF10B5ONE", "")):
            counts["excluded_10b5_1"] += 1
            continue
        symbol = submission.get("ISSUERTRADINGSYMBOL", "").upper()
        if not SYMBOL_PATTERN.fullmatch(symbol):
            counts["excluded_symbol"] += 1
            continue
        if not (
            owner_roles.get(accession, set()) & ALLOWED_OWNER_RELATIONSHIPS
        ):
            counts["excluded_owner_relationship"] += 1
            continue
        if row.get("TRANS_CODE", "").upper() != "P":
            counts["excluded_transaction_code"] += 1
            continue
        if row.get("TRANS_ACQUIRED_DISP_CD", "").upper() != "A":
            counts["excluded_disposition"] += 1
            continue
        if row.get("DIRECT_INDIRECT_OWNERSHIP", "").upper() != "D":
            counts["excluded_indirect"] += 1
            continue
        title = " ".join(row.get("SECURITY_TITLE", "").split())
        if not COMMON_EQUITY_PATTERN.fullmatch(title):
            counts["excluded_security_title"] += 1
            continue
        if not _core_fields_unfootnoted(row):
            counts["excluded_footnoted_core_field"] += 1
            continue
        shares = _number(row.get("TRANS_SHARES", ""))
        price = _number(row.get("TRANS_PRICEPERSHARE", ""))
        if shares is None or price is None:
            counts["excluded_missing_economics"] += 1
            continue
        filing_date = _parse_sec_date(submission.get("FILING_DATE", ""))
        transaction_date = _parse_sec_date(row.get("TRANS_DATE", ""))
        lag = (filing_date - transaction_date).days
        if lag < 0 or lag > 4:
            counts["excluded_filing_lag"] += 1
            continue
        transaction_key = (
            accession,
            row.get("NONDERIV_TRANS_SK", ""),
            symbol,
            transaction_date.isoformat(),
            f"{shares:.8f}",
            f"{price:.8f}",
        )
        if transaction_key in exact_seen:
            counts["exact_duplicates"] += 1
            continue
        exact_seen.add(transaction_key)
        eligible.append(
            {
                "accession_number": accession,
                "filing_date": filing_date.isoformat(),
                "issuer_cik": submission.get("ISSUERCIK", ""),
                "issuer_name": submission.get("ISSUERNAME", ""),
                "owner_ciks": sorted(owners.get(accession, set())),
                "owner_relationships": sorted(
                    owner_roles.get(accession, set())
                    & ALLOWED_OWNER_RELATIONSHIPS
                ),
                "price_per_share": price,
                "purchase_notional": shares * price,
                "security_title": title,
                "shares": shares,
                "symbol": symbol,
                "transaction_date": transaction_date.isoformat(),
            }
        )
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in eligible:
        key = (row["filing_date"], row["issuer_cik"], row["symbol"])
        current = grouped.setdefault(
            key,
            {
                "accession_numbers": set(),
                "filing_date": row["filing_date"],
                "issuer_cik": row["issuer_cik"],
                "issuer_name": row["issuer_name"],
                "owner_ciks": set(),
                "owner_relationships": set(),
                "purchase_notional": 0.0,
                "symbol": row["symbol"],
                "transaction_count": 0,
                "transaction_dates": set(),
            },
        )
        current["accession_numbers"].add(row["accession_number"])
        current["owner_ciks"].update(row["owner_ciks"])
        current["owner_relationships"].update(row["owner_relationships"])
        current["purchase_notional"] += row["purchase_notional"]
        current["transaction_count"] += 1
        current["transaction_dates"].add(row["transaction_date"])
    events: list[dict[str, Any]] = []
    for key in sorted(grouped):
        row = grouped[key]
        events.append(
            {
                "accession_numbers": sorted(row["accession_numbers"]),
                "distinct_reporting_owners": len(row["owner_ciks"]),
                "filing_date": row["filing_date"],
                "issuer_cik": row["issuer_cik"],
                "issuer_name": row["issuer_name"],
                "owner_ciks": sorted(row["owner_ciks"]),
                "owner_relationships": sorted(row["owner_relationships"]),
                "purchase_notional": round(row["purchase_notional"], 8),
                "symbol": row["symbol"],
                "transaction_count": row["transaction_count"],
                "transaction_dates": sorted(row["transaction_dates"]),
            }
        )
    counts["eligible_transaction_rows"] = len(eligible)
    counts["normalized_events"] = len(events)
    return events, dict(sorted(counts.items()))


def _archive_paths(
    collection: Mapping[str, Any], contract: Mapping[str, Any]
) -> list[Path]:
    root = _store_root()
    directory = root / str(collection["store_relative_path"])
    expected = {
        row["filename"]: row for row in contract["source"]["requests"]
    }
    paths: list[Path] = []
    rows = collection.get("archives")
    if not isinstance(rows, list) or len(rows) != len(expected):
        raise InsiderPurchaseCapacityError("collection archive accounting drifted")
    for row in rows:
        filename = row.get("filename")
        if filename not in expected:
            raise InsiderPurchaseCapacityError(
                f"unexpected collected archive {filename}"
            )
        path = directory / str(filename)
        if not path.is_file():
            raise InsiderPurchaseCapacityError(
                f"collected archive is missing: {path}"
            )
        if sha256_file(path) != row.get("file_sha256"):
            raise InsiderPurchaseCapacityError(
                f"collected archive hash drifted: {filename}"
            )
        paths.append(path)
    return sorted(paths)


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
) -> Path:
    datetime.fromisoformat(inspected_at.replace("Z", "+00:00"))
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(collection_path)
    collection = _read_json(collection_path)
    if collection.get("artifact_sha256") != self_hash(
        collection, "artifact_sha256"
    ):
        raise InsiderPurchaseCapacityError("collection artifact hash drifted")
    contract_path = PROJECT_ROOT / str(collection["contract_path"])
    strategy_discovery.require_committed(contract_path)
    contract = _read_json(contract_path)
    paths = _archive_paths(collection, contract)
    events, counts = normalized_events(paths)
    event_bytes = canonical_bytes(events)
    content_sha = sha256_bytes(event_bytes)
    store = _store_root()
    event_path = (
        store
        / "_derived/form4_insider_purchase"
        / str(contract["artifact_sha256"])
        / f"normalized-events-{content_sha}.json.gz"
    )
    event_path.parent.mkdir(parents=True, exist_ok=True)
    if not event_path.exists():
        temporary = event_path.with_name(
            f".{event_path.name}.{os.getpid()}.tmp"
        )
        try:
            with temporary.open("wb") as raw:
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=raw,
                    mtime=0,
                ) as stream:
                    stream.write(event_bytes)
            os.replace(temporary, event_path)
        finally:
            temporary.unlink(missing_ok=True)
    unique_dates = len({row["filing_date"] for row in events})
    unique_symbols = len({row["symbol"] for row in events})
    if len(events) >= MINIMUM_FAST_LANE_EVENTS and unique_dates >= 50:
        disposition = "ADMIT_DEVELOPMENT_SEARCH"
        state = "FORM4_CAPACITY_READY"
    elif len(events) >= MINIMUM_LATER_RESEARCH_EVENTS:
        disposition = "PRESERVE_LATER_SINGLE_RULE_RESEARCH"
        state = "FORM4_LATER_RESEARCH_ONLY"
    else:
        disposition = "RETIRE_INSUFFICIENT_FORMAL_CAPACITY"
        state = "FORM4_INSUFFICIENT_CAPACITY"
    checks = {
        "collection_hash_rebuilt": True,
        "contract_hash_rebuilt": contract.get("artifact_sha256")
        == self_hash(contract, "artifact_sha256"),
        "exact_archive_graph_rebuilt": len(paths) == 28,
        "all_archive_hashes_rebuilt": True,
        "as_filed_semantics_rebuilt": True,
        "market_prices_absent": collection.get("market_prices_accessed")
        is False,
        "forward_returns_absent": collection.get("forward_returns_accessed")
        is False,
        "confirmation_absent": collection.get("confirmation_accessed")
        is False,
        "broker_actions_zero": collection.get("broker_actions") == 0,
    }
    inspection: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "form4-insider-purchase-capacity-inspection",
        "artifact_sha256": "",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "version_id": VERSION_ID,
        "collection_path": _relative_to_project(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "contract_path": _relative_to_project(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspected_at": inspected_at,
        "checks": checks,
        "counts": counts,
        "formal_capacity": {
            "normalized_events": len(events),
            "unique_filing_dates": unique_dates,
            "unique_symbols": unique_symbols,
            "minimum_fast_lane_events": MINIMUM_FAST_LANE_EVENTS,
        },
        "private_event_binding": {
            "content_sha256": content_sha,
            "file_sha256": sha256_file(event_path),
            "format": "json.gz",
            "relative_path": str(
                event_path.relative_to(_store_root())
            ),
        },
        "disposition": disposition,
        "state": state,
        "valid": all(checks.values()),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }
    inspection["artifact_sha256"] = self_hash(
        inspection, "artifact_sha256"
    )
    path = _artifact_path(
        root,
        "capacity-inspection",
        "form4-insider-purchase-capacity-inspection",
        inspection,
        "artifact_sha256",
    )
    _write_json(path, inspection)
    return path


def status(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    contracts = sorted((root / "source-contract").glob("*.json"))
    inspections = sorted(
        (root / "source-contract-inspection").glob("*.json")
    )
    collections = sorted((root / "source-collection").glob("*.json"))
    capacity = sorted((root / "capacity-inspection").glob("*.json"))
    state = "FORM4_SOURCE_CONTRACT_REQUIRED"
    next_action = "freeze the exact 2018-2024 SEC quarterly source contract"
    disposition = None
    if contracts:
        state = "FORM4_SOURCE_CONTRACT_FROZEN"
        next_action = "independently inspect the source contract"
    if inspections:
        state = "FORM4_SOURCE_CONTRACT_INSPECTED_READY"
        next_action = "collect the exact 28 SEC quarterly archives"
    if collections:
        state = "FORM4_SOURCE_COLLECTED"
        next_action = "independently rebuild normalized event capacity"
    if capacity:
        latest = _read_json(capacity[-1])
        state = str(latest["state"])
        disposition = latest.get("disposition")
        next_action = (
            "freeze the exact development/embargo/confirmation family contract"
            if state == "FORM4_CAPACITY_READY"
            else "advance the next rolling mechanism slot"
        )
    return {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "state": state,
        "disposition": disposition,
        "next_action": next_action,
        "artifacts": {
            "contracts": len(contracts),
            "contract_inspections": len(inspections),
            "collections": len(collections),
            "capacity_inspections": len(capacity),
        },
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    collect_ = sub.add_parser("collect")
    collect_.add_argument("contract", type=Path)
    collect_.add_argument("--collected-at", required=True)
    inspect_collection_ = sub.add_parser("inspect-collection")
    inspect_collection_.add_argument("collection", type=Path)
    inspect_collection_.add_argument("--inspected-at", required=True)
    sub.add_parser("status")
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze-contract":
            result: Any = freeze_contract(
                created_at=args.created_at, root=args.root
            )
        elif args.command == "inspect-contract":
            result = inspect_contract(
                args.contract,
                inspected_at=args.inspected_at,
                root=args.root,
            )
        elif args.command == "collect":
            result = collect(
                args.contract,
                collected_at=args.collected_at,
                root=args.root,
            )
        elif args.command == "inspect-collection":
            result = inspect_collection(
                args.collection,
                inspected_at=args.inspected_at,
                root=args.root,
            )
        else:
            result = status(args.root)
    except (InsiderPurchaseCapacityError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                sort_keys=True,
            )
        )
        return 2
    if isinstance(result, Path):
        print(result)
    else:
        print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

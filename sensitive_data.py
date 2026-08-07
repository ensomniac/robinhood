"""Authenticated encryption for exact identifiers in sanitized public context."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import stat
import statistics
import sys
import tempfile
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Sequence

from cryptography.fernet import Fernet, InvalidToken
from dotenv import dotenv_values


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
TOKEN_PREFIX = "enc:fernet"
TOKEN_PATTERN = re.compile(r"enc:fernet:(v[1-9][0-9]*):([A-Za-z0-9_-]+={0,2})")
FULL_TOKEN_PATTERN = re.compile(r"^enc:fernet:(v[1-9][0-9]*):([A-Za-z0-9_-]+={0,2})$")
KEY_NAME_PATTERN = re.compile(r"^TRADE_CONTEXT_KEY_(V[1-9][0-9]*)$")
VERSION_PATTERN = re.compile(r"^v[1-9][0-9]*$")
SENSITIVE_LABEL_PATTERN = re.compile(
    r"(?i)(?:account number|broker(?: [a-z-]+){0,3} order id|"
    r"client(?:-generated)? ref(?:erence)? id|ref(?:erence)? uuid|"
    r"confirmation id|cancellation id|replacement id)\s*:"
)
IDENTIFIER_CONTEXT_PATTERN = re.compile(
    r"(?i)\b(?:order|ref(?:erence)?|confirmation|cancellation|replacement)\b"
)
RAW_UUID_PATTERN = re.compile(
    r"(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\b"
)
PLACEHOLDER_VALUES = frozenset(
    {"", "-", "none", "n/a", "tbd", "not available", "(identifier omitted)"}
)
VALID_FIELDS = (
    "broker_order_id",
    "client_ref_id",
    "confirmation_id",
    "cancellation_id",
    "replacement_id",
    "account_number",
    "other_identifier",
)


class SensitiveDataError(RuntimeError):
    """Base error for identifier encryption and configuration failures."""


class KeyConfigurationError(SensitiveDataError):
    """Raised when the local key file is absent, exposed, or invalid."""


class TokenError(SensitiveDataError):
    """Raised when encrypted data is malformed, altered, or uses the wrong field."""


@dataclass(frozen=True)
class DecryptedValue:
    field: str
    value: str
    key_version: str


@dataclass(frozen=True)
class AuditResult:
    checked_files: int
    encrypted_values: int
    violations: tuple[str, ...]


def _assert_private_file(path: Path) -> None:
    try:
        file_stat = path.stat()
    except FileNotFoundError as exc:
        raise KeyConfigurationError(
            f"Key file not found: {path}; run `python3 sensitive_data.py init-key`"
        ) from exc
    if not stat.S_ISREG(file_stat.st_mode):
        raise KeyConfigurationError(f"Key path is not a regular file: {path}")
    exposed_bits = stat.S_IMODE(file_stat.st_mode) & 0o077
    if exposed_bits:
        raise KeyConfigurationError(
            f"Key file permissions must be 0600 or stricter, not "
            f"{stat.S_IMODE(file_stat.st_mode):04o}: {path}"
        )


def _read_environment(path: Path) -> dict[str, str]:
    _assert_private_file(path)
    parsed = dotenv_values(path, interpolate=False)
    values = {key: value for key, value in parsed.items() if value is not None}
    for key, value in os.environ.items():
        if key == "TRADE_CONTEXT_ACTIVE_KEY" or KEY_NAME_PATTERN.fullmatch(key):
            values[key] = value
    return values


class SensitiveDataCipher:
    """Encrypt and decrypt short identifiers with cached Fernet instances."""

    def __init__(
        self, active_version: str, fernets: dict[str, Fernet], env_path: Path
    ) -> None:
        self.active_version = active_version
        self._fernets = fernets
        self.env_path = env_path

    @classmethod
    def from_env(cls, env_path: Path | str = DEFAULT_ENV_PATH) -> SensitiveDataCipher:
        path = Path(env_path).resolve()
        values = _read_environment(path)
        active_version = values.get("TRADE_CONTEXT_ACTIVE_KEY", "").strip().lower()
        if not VERSION_PATTERN.fullmatch(active_version):
            raise KeyConfigurationError(
                "TRADE_CONTEXT_ACTIVE_KEY must be a version such as v1"
            )

        fernets: dict[str, Fernet] = {}
        for name, value in values.items():
            match = KEY_NAME_PATTERN.fullmatch(name)
            if not match:
                continue
            version = match.group(1).lower()
            try:
                fernets[version] = Fernet(value.strip().encode("ascii"))
            except (TypeError, ValueError, UnicodeEncodeError) as exc:
                raise KeyConfigurationError(
                    f"{name} is not a valid Fernet key"
                ) from exc

        if active_version not in fernets:
            expected_name = f"TRADE_CONTEXT_KEY_{active_version.upper()}"
            raise KeyConfigurationError(
                f"Active key {expected_name} is missing from {path}"
            )
        return cls(active_version, fernets, path)

    @property
    def key_versions(self) -> tuple[str, ...]:
        return tuple(sorted(self._fernets, key=lambda item: int(item[1:])))

    def encrypt(self, value: str, field: str) -> str:
        if field not in VALID_FIELDS:
            raise TokenError(f"Unsupported sensitive field: {field}")
        value = value.strip()
        if not value:
            raise TokenError("Sensitive value cannot be empty")
        if len(value) > 4096:
            raise TokenError("Sensitive value exceeds the 4096-character limit")

        payload = json.dumps(
            {"schema": 1, "field": field, "value": value},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        token = self._fernets[self.active_version].encrypt(payload).decode("ascii")
        return f"{TOKEN_PREFIX}:{self.active_version}:{token}"

    def decrypt(self, token: str, expected_field: str | None = None) -> DecryptedValue:
        token = token.strip().strip("`")
        match = FULL_TOKEN_PATTERN.fullmatch(token)
        if not match:
            raise TokenError("Value is not a valid enc:fernet token")
        key_version, encoded_value = match.groups()
        fernet = self._fernets.get(key_version)
        if fernet is None:
            raise TokenError(
                f"No local key is available for encrypted token version {key_version}"
            )
        try:
            raw_payload = fernet.decrypt(encoded_value.encode("ascii"))
        except InvalidToken as exc:
            raise TokenError(
                "Encrypted token failed authentication; it is altered or uses another key"
            ) from exc
        try:
            payload = json.loads(raw_payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TokenError("Encrypted token payload is invalid") from exc

        if not isinstance(payload, dict) or payload.get("schema") != 1:
            raise TokenError("Encrypted token has an unsupported payload schema")
        field = payload.get("field")
        value = payload.get("value")
        if field not in VALID_FIELDS or not isinstance(value, str) or not value:
            raise TokenError("Encrypted token payload is incomplete")
        if expected_field is not None and field != expected_field:
            raise TokenError(
                f"Encrypted token contains {field!r}, not expected field {expected_field!r}"
            )
        return DecryptedValue(field=field, value=value, key_version=key_version)


@lru_cache(maxsize=8)
def get_cipher(env_path: str = str(DEFAULT_ENV_PATH)) -> SensitiveDataCipher:
    """Return a process-cached cipher for low-overhead repeated operations."""
    return SensitiveDataCipher.from_env(Path(env_path))


def initialize_key_file(env_path: Path | str = DEFAULT_ENV_PATH) -> str:
    """Create a new mode-0600 key file without printing or returning the key."""
    path = Path(env_path).resolve()
    if path.exists():
        raise KeyConfigurationError(f"Refusing to overwrite existing key file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    key = Fernet.generate_key().decode("ascii")
    contents = (
        "# Private key ring for encrypted identifiers in public trade context.\n"
        "# Never commit this file or paste its values into logs, email, or chat.\n"
        "TRADE_CONTEXT_ACTIVE_KEY=v1\n"
        f"TRADE_CONTEXT_KEY_V1={key}\n"
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as env_file:
            env_file.write(contents)
            env_file.flush()
            os.fsync(env_file.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return "v1"


def rotate_key(env_path: Path | str = DEFAULT_ENV_PATH) -> str:
    """Add a new active key while retaining old keys for historical tokens."""
    path = Path(env_path).resolve()
    cipher = SensitiveDataCipher.from_env(path)
    next_number = max(int(version[1:]) for version in cipher.key_versions) + 1
    next_version = f"v{next_number}"
    new_key = Fernet.generate_key().decode("ascii")

    current_lines = path.read_text(encoding="utf-8").splitlines()
    updated_lines: list[str] = []
    active_replaced = False
    for line in current_lines:
        if line.startswith("TRADE_CONTEXT_ACTIVE_KEY="):
            updated_lines.append(f"TRADE_CONTEXT_ACTIVE_KEY={next_version}")
            active_replaced = True
        else:
            updated_lines.append(line)
    if not active_replaced:
        updated_lines.append(f"TRADE_CONTEXT_ACTIVE_KEY={next_version}")
    updated_lines.append(f"TRADE_CONTEXT_KEY_{next_version.upper()}={new_key}")
    updated_contents = "\n".join(updated_lines) + "\n"

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".env.rotate.", dir=path.parent, text=True
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as temporary_file:
            temporary_file.write(updated_contents)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    get_cipher.cache_clear()
    return next_version


def _expand_markdown_paths(paths: Sequence[Path]) -> tuple[Path, ...]:
    expanded: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved.is_dir():
            expanded.update(item for item in resolved.rglob("*.md") if item.is_file())
        elif resolved.is_file():
            expanded.add(resolved)
    return tuple(sorted(expanded))


def audit_context_files(
    paths: Sequence[Path], cipher: SensitiveDataCipher
) -> AuditResult:
    """Reject plaintext values on identifier-labelled Markdown lines."""
    violations: list[str] = []
    encrypted_values = 0
    files = _expand_markdown_paths(paths)

    for path in files:
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            tokens = [match.group(0) for match in TOKEN_PATTERN.finditer(line)]
            for token in tokens:
                try:
                    cipher.decrypt(token)
                    encrypted_values += 1
                except TokenError as exc:
                    violations.append(f"{path}:{line_number}: {exc}")

            label_match = SENSITIVE_LABEL_PATTERN.search(line)
            if label_match:
                value = line[label_match.end() :].strip().strip("`").strip()
                if (
                    value.lower() not in PLACEHOLDER_VALUES
                    and not FULL_TOKEN_PATTERN.fullmatch(value)
                ):
                    violations.append(
                        f"{path}:{line_number}: sensitive identifier is not encrypted"
                    )
                continue

            line_without_tokens = TOKEN_PATTERN.sub("", line)
            if IDENTIFIER_CONTEXT_PATTERN.search(
                line_without_tokens
            ) and RAW_UUID_PATTERN.search(line_without_tokens):
                violations.append(
                    f"{path}:{line_number}: UUID-shaped identifier is not encrypted"
                )

    return AuditResult(
        checked_files=len(files),
        encrypted_values=encrypted_values,
        violations=tuple(violations),
    )


def benchmark_cipher(
    cipher: SensitiveDataCipher, iterations: int
) -> dict[str, float | int]:
    """Measure warmed, in-process cryptographic cost for a representative UUID."""
    if not 10 <= iterations <= 1_000_000:
        raise SensitiveDataError("Benchmark iterations must be between 10 and 1000000")
    sample = "123e4567-e89b-12d3-a456-426614174000"
    cipher.decrypt(cipher.encrypt(sample, "broker_order_id"), "broker_order_id")

    encrypt_times: list[int] = []
    decrypt_times: list[int] = []
    round_trip_times: list[int] = []
    for _ in range(iterations):
        started = time.perf_counter_ns()
        token = cipher.encrypt(sample, "broker_order_id")
        encrypted = time.perf_counter_ns()
        cipher.decrypt(token, "broker_order_id")
        finished = time.perf_counter_ns()
        encrypt_times.append(encrypted - started)
        decrypt_times.append(finished - encrypted)
        round_trip_times.append(finished - started)

    def microseconds(values: list[int], percentile: float) -> float:
        ordered = sorted(values)
        index = min(len(ordered) - 1, int(len(ordered) * percentile))
        return round(ordered[index] / 1000, 3)

    return {
        "iterations": iterations,
        "encrypt_median_us": round(statistics.median(encrypt_times) / 1000, 3),
        "encrypt_p95_us": microseconds(encrypt_times, 0.95),
        "decrypt_median_us": round(statistics.median(decrypt_times) / 1000, 3),
        "decrypt_p95_us": microseconds(decrypt_times, 0.95),
        "round_trip_p95_us": microseconds(round_trip_times, 0.95),
    }


def _read_private_input(prompt: str) -> str:
    if sys.stdin.isatty():
        return getpass.getpass(prompt).strip()
    return sys.stdin.read().strip()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Encrypt exact broker identifiers for safe inline Git storage."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_PATH,
        help="Ignored key file (default: .env beside this script)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-key", help="Create a new mode-0600 .env key file")
    subparsers.add_parser("rotate-key", help="Add and activate a new key version")
    subparsers.add_parser("check", help="Validate key configuration and round trip")

    encrypt_parser = subparsers.add_parser(
        "encrypt", help="Read one private value from stdin and print ciphertext"
    )
    encrypt_parser.add_argument("--field", required=True, choices=VALID_FIELDS)

    decrypt_parser = subparsers.add_parser(
        "decrypt", help="Decrypt a public token from an argument or stdin"
    )
    decrypt_parser.add_argument("token", nargs="?")
    decrypt_parser.add_argument("--expect-field", choices=VALID_FIELDS)

    audit_parser = subparsers.add_parser(
        "audit", help="Check Markdown identifier fields for plaintext or bad tokens"
    )
    audit_parser.add_argument("paths", nargs="*", type=Path)

    benchmark_parser = subparsers.add_parser(
        "benchmark", help="Measure warmed encryption and decryption latency"
    )
    benchmark_parser.add_argument("--iterations", type=int, default=5000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    env_path = args.env_file.resolve()
    try:
        if args.command == "init-key":
            version = initialize_key_file(env_path)
            result: object = {
                "initialized": True,
                "active_key": version,
                "env_file": str(env_path),
                "permissions": "0600",
            }
        elif args.command == "rotate-key":
            version = rotate_key(env_path)
            result = {
                "rotated": True,
                "active_key": version,
                "env_file": str(env_path),
            }
        else:
            cipher = get_cipher(str(env_path))
            if args.command == "check":
                probe = "identifier-encryption-check"
                token = cipher.encrypt(probe, "other_identifier")
                decrypted = cipher.decrypt(token, "other_identifier")
                if decrypted.value != probe:
                    raise SensitiveDataError("Encryption round-trip check failed")
                result = {
                    "configuration_valid": True,
                    "active_key": cipher.active_version,
                    "available_key_versions": list(cipher.key_versions),
                    "env_file": str(env_path),
                    "permissions": "0600",
                }
            elif args.command == "encrypt":
                value = _read_private_input("Sensitive value: ")
                print(cipher.encrypt(value, args.field))
                return 0
            elif args.command == "decrypt":
                token = args.token or _read_private_input("Encrypted token: ")
                decrypted = cipher.decrypt(token, args.expect_field)
                print(decrypted.value)
                return 0
            elif args.command == "audit":
                paths = args.paths or [PROJECT_ROOT / "history"]
                audit = audit_context_files(paths, cipher)
                result = {
                    "checked_files": audit.checked_files,
                    "encrypted_values": audit.encrypted_values,
                    "violations": list(audit.violations),
                    "valid": not audit.violations,
                }
                print(json.dumps(result, indent=2, sort_keys=True))
                return 1 if audit.violations else 0
            else:
                result = benchmark_cipher(cipher, args.iterations)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except SensitiveDataError as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Settings-driven email notifications for the Robinhood Codex workflow."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import requests


DEFAULT_SETTINGS_PATH = Path(__file__).with_name("settings.toml")
DELIVERY_DESTINATION = "ryan@ensomniac.com"
VALID_VERBOSITIES = ("off", "trades", "verbose")
VALID_EVENTS = (
    "setup",
    "trade_placed",
    "trade_modified",
    "trade_completed",
    "critical",
    "session_summary",
)
TRADE_EVENTS = frozenset({"trade_placed", "trade_modified", "trade_completed"})


class EmailConfigurationError(ValueError):
    """Raised when the human-editable email settings are invalid."""


class EmailDeliveryError(RuntimeError):
    """Raised when the mail server does not confirm delivery."""


@dataclass(frozen=True)
class EmailSettings:
    verbosity: str
    endpoint: str
    timeout_seconds: float
    subject_prefix: str


def load_email_settings(path: Path | str = DEFAULT_SETTINGS_PATH) -> EmailSettings:
    """Load and validate the email section from the project settings file."""
    settings_path = Path(path)
    try:
        with settings_path.open("rb") as settings_file:
            document = tomllib.load(settings_file)
    except FileNotFoundError as exc:
        raise EmailConfigurationError(
            f"Settings file not found: {settings_path}"
        ) from exc
    except tomllib.TOMLDecodeError as exc:
        raise EmailConfigurationError(
            f"Settings file is not valid TOML: {exc}"
        ) from exc

    try:
        email = document["notifications"]["email"]
    except (KeyError, TypeError) as exc:
        raise EmailConfigurationError(
            "Settings must contain a [notifications.email] table"
        ) from exc

    if not isinstance(email, dict):
        raise EmailConfigurationError("[notifications.email] must be a table")

    verbosity = str(email.get("verbosity", "")).strip().lower()
    if verbosity not in VALID_VERBOSITIES:
        choices = ", ".join(VALID_VERBOSITIES)
        raise EmailConfigurationError(
            f"notifications.email.verbosity must be one of: {choices}"
        )

    endpoint = str(email.get("endpoint", "")).strip()
    if not endpoint.startswith("https://"):
        raise EmailConfigurationError(
            "notifications.email.endpoint must be an HTTPS URL"
        )

    raw_timeout = email.get("timeout_seconds")
    if isinstance(raw_timeout, bool):
        raise EmailConfigurationError(
            "notifications.email.timeout_seconds must be a number"
        )
    try:
        timeout_seconds = float(raw_timeout)
    except (TypeError, ValueError) as exc:
        raise EmailConfigurationError(
            "notifications.email.timeout_seconds must be a number"
        ) from exc
    if not 0 < timeout_seconds <= 60:
        raise EmailConfigurationError(
            "notifications.email.timeout_seconds must be greater than 0 and at most 60"
        )

    subject_prefix = str(email.get("subject_prefix", "")).strip()
    return EmailSettings(
        verbosity=verbosity,
        endpoint=endpoint,
        timeout_seconds=timeout_seconds,
        subject_prefix=subject_prefix,
    )


def should_send(event: str, verbosity: str) -> bool:
    """Return whether an operational event should send at this verbosity."""
    if event not in VALID_EVENTS:
        raise EmailConfigurationError(
            f"Unknown event {event!r}; expected one of: {', '.join(VALID_EVENTS)}"
        )
    if verbosity not in VALID_VERBOSITIES:
        raise EmailConfigurationError(
            f"Unknown verbosity {verbosity!r}; expected one of: "
            f"{', '.join(VALID_VERBOSITIES)}"
        )
    if verbosity == "off":
        return False
    if verbosity == "trades":
        return event in TRADE_EVENTS
    return True


class EmailSender:
    """Send direct or policy-filtered messages through the SmartSioux API."""

    def __init__(
        self,
        settings: EmailSettings | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings or load_email_settings()
        self._session = session or requests.Session()

    def send(
        self,
        subject: str,
        body: str,
        additional_data: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send one email and return the mail server's decoded JSON response."""
        subject = subject.strip()
        if not subject:
            raise EmailConfigurationError("Email subject cannot be empty")

        rendered_subject = subject
        prefix = self.settings.subject_prefix
        if prefix and not subject.startswith(prefix):
            rendered_subject = f"{prefix} {subject}"

        rendered_body = body.rstrip()
        if additional_data:
            try:
                details = json.dumps(
                    dict(additional_data), indent=2, sort_keys=True, default=str
                )
            except (TypeError, ValueError) as exc:
                raise EmailConfigurationError(
                    "Email additional_data must be JSON-serializable"
                ) from exc
            rendered_body = f"{rendered_body}\n\nDetails:\n{details}"

        payload = {
            "f": "send_mail",
            "subject": rendered_subject,
            "body": rendered_body,
        }
        try:
            response = self._session.post(
                self.settings.endpoint,
                data=payload,
                timeout=self.settings.timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise EmailDeliveryError(f"Email request failed: {exc}") from exc

        try:
            result = response.json()
        except (requests.exceptions.JSONDecodeError, ValueError) as exc:
            raise EmailDeliveryError(
                "Email server returned a non-JSON response"
            ) from exc

        if not isinstance(result, dict):
            raise EmailDeliveryError("Email server returned JSON that is not an object")
        if not result.get("sent"):
            reason = result.get("error") or result.get("message") or "unknown reason"
            raise EmailDeliveryError(f"Email server did not confirm delivery: {reason}")
        return result

    def notify(
        self,
        event: str,
        subject: str,
        body: str,
        additional_data: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Apply verbosity policy, then send or return a structured skip."""
        if not should_send(event, self.settings.verbosity):
            return {
                "event": event,
                "sent": False,
                "skipped": True,
                "verbosity": self.settings.verbosity,
                "reason": "event disabled by email verbosity",
            }

        server_response = self.send(subject, body, additional_data)
        return {
            "event": event,
            "sent": True,
            "skipped": False,
            "verbosity": self.settings.verbosity,
            "destination": DELIVERY_DESTINATION,
            "server_response": server_response,
        }

    def Send(
        self,
        subject: str,
        body: str,
        additional_data: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Backward-compatible alias for the original example's method name."""
        return self.send(subject, body, additional_data)


def _parse_additional_data(raw_value: str | None) -> dict[str, Any] | None:
    if raw_value is None:
        return None
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise EmailConfigurationError(f"--data-json is invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise EmailConfigurationError("--data-json must decode to a JSON object")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send settings-driven Robinhood Codex email notifications."
    )
    parser.add_argument(
        "--settings",
        type=Path,
        default=DEFAULT_SETTINGS_PATH,
        help="TOML settings path (default: settings.toml beside this script)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("check", help="Validate settings without sending email")

    notify_parser = subparsers.add_parser(
        "notify", help="Send an operational event when verbosity permits"
    )
    notify_parser.add_argument("event", choices=VALID_EVENTS)
    notify_parser.add_argument("--subject", required=True)
    notify_parser.add_argument("--body", required=True)
    notify_parser.add_argument(
        "--data-json", help="Optional JSON object appended as structured details"
    )

    subparsers.add_parser(
        "test",
        help="Explicitly send a delivery test, bypassing operational verbosity",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        settings = load_email_settings(args.settings)
        if args.command == "check":
            result: dict[str, Any] = {
                "settings_valid": True,
                "verbosity": settings.verbosity,
                "destination": DELIVERY_DESTINATION,
                "endpoint": settings.endpoint,
            }
        else:
            sender = EmailSender(settings=settings)
            if args.command == "notify":
                result = sender.notify(
                    event=args.event,
                    subject=args.subject,
                    body=args.body,
                    additional_data=_parse_additional_data(args.data_json),
                )
            else:
                timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
                server_response = sender.send(
                    subject="Email integration test",
                    body=(
                        "This is a delivery test from the Robinhood Codex repository.\n"
                        "No trade was placed, modified, or completed.\n"
                        f"Generated at: {timestamp}"
                    ),
                    additional_data={
                        "settings_verbosity": settings.verbosity,
                        "test_only": True,
                    },
                )
                result = {
                    "event": "test",
                    "sent": True,
                    "skipped": False,
                    "verbosity_bypassed": True,
                    "destination": DELIVERY_DESTINATION,
                    "server_response": server_response,
                }
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    except (EmailConfigurationError, EmailDeliveryError) as exc:
        print(
            json.dumps(
                {"sent": False, "error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

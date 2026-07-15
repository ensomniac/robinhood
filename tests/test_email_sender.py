import tempfile
import unittest
from pathlib import Path

import requests

from email_sender import (
    EmailConfigurationError,
    EmailDeliveryError,
    EmailSender,
    EmailSettings,
    load_email_settings,
    should_send,
)


class StubResponse:
    def __init__(self, result=None, error=None):
        self._result = result
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise self._error

    def json(self):
        return self._result


class StubSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, data, timeout):
        self.calls.append({"url": url, "data": data, "timeout": timeout})
        return self.response


def make_settings(verbosity="trades"):
    return EmailSettings(
        verbosity=verbosity,
        endpoint="https://example.test/Api",
        timeout_seconds=12,
        subject_prefix="[Robinhood Codex]",
    )


class PolicyTests(unittest.TestCase):
    def test_off_skips_every_operational_event(self):
        events = (
            "setup",
            "trade_placed",
            "trade_modified",
            "trade_completed",
            "critical",
            "session_summary",
        )
        self.assertTrue(all(not should_send(event, "off") for event in events))

    def test_trades_sends_only_trade_lifecycle_events(self):
        self.assertFalse(should_send("setup", "trades"))
        self.assertTrue(should_send("trade_placed", "trades"))
        self.assertTrue(should_send("trade_modified", "trades"))
        self.assertTrue(should_send("trade_completed", "trades"))
        self.assertFalse(should_send("critical", "trades"))
        self.assertFalse(should_send("session_summary", "trades"))

    def test_verbose_sends_every_operational_event(self):
        self.assertTrue(should_send("setup", "verbose"))
        self.assertTrue(should_send("critical", "verbose"))
        self.assertTrue(should_send("session_summary", "verbose"))


class SenderTests(unittest.TestCase):
    def test_notify_skip_does_not_call_server(self):
        session = StubSession(StubResponse({"sent": True}))
        sender = EmailSender(make_settings("trades"), session=session)

        result = sender.notify("setup", "Candidate", "Watching XYZ")

        self.assertTrue(result["skipped"])
        self.assertFalse(result["sent"])
        self.assertEqual(session.calls, [])

    def test_send_builds_expected_form_payload(self):
        session = StubSession(StubResponse({"sent": True, "status": "accepted"}))
        sender = EmailSender(make_settings(), session=session)

        result = sender.notify(
            "trade_placed",
            "XYZ entry accepted",
            "Broker state: filled",
            {"quantity": 10, "stop": 99.25},
        )

        self.assertTrue(result["sent"])
        self.assertEqual(result["server_response"]["status"], "accepted")
        self.assertEqual(len(session.calls), 1)
        call = session.calls[0]
        self.assertEqual(call["url"], "https://example.test/Api")
        self.assertEqual(call["timeout"], 12)
        self.assertEqual(call["data"]["f"], "send_mail")
        self.assertEqual(
            call["data"]["subject"],
            "[Robinhood Codex] XYZ entry accepted",
        )
        self.assertIn("Broker state: filled\n\nDetails:\n", call["data"]["body"])
        self.assertIn('"quantity": 10', call["data"]["body"])

    def test_server_rejection_raises_delivery_error(self):
        session = StubSession(StubResponse({"sent": False, "error": "disabled"}))
        sender = EmailSender(make_settings(), session=session)

        with self.assertRaisesRegex(EmailDeliveryError, "disabled"):
            sender.send("Subject", "Body")

    def test_http_error_raises_delivery_error(self):
        response = StubResponse(error=requests.HTTPError("503 Server Error"))
        sender = EmailSender(make_settings(), session=StubSession(response))

        with self.assertRaisesRegex(EmailDeliveryError, "503 Server Error"):
            sender.send("Subject", "Body")


class SettingsTests(unittest.TestCase):
    def test_loads_human_editable_toml(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "settings.toml"
            path.write_text(
                """
[notifications.email]
verbosity = "verbose"
endpoint = "https://example.test/Api"
timeout_seconds = 9
subject_prefix = "[Test]"
""".strip(),
                encoding="utf-8",
            )

            settings = load_email_settings(path)

        self.assertEqual(settings.verbosity, "verbose")
        self.assertEqual(settings.timeout_seconds, 9)
        self.assertEqual(settings.subject_prefix, "[Test]")

    def test_rejects_invalid_verbosity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "settings.toml"
            path.write_text(
                """
[notifications.email]
verbosity = "everything"
endpoint = "https://example.test/Api"
timeout_seconds = 9
subject_prefix = "[Test]"
""".strip(),
                encoding="utf-8",
            )

            with self.assertRaises(EmailConfigurationError):
                load_email_settings(path)


if __name__ == "__main__":
    unittest.main()

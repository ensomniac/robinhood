import os
import tempfile
import time
import unittest
from pathlib import Path

from sensitive_data import (
    KeyConfigurationError,
    SensitiveDataCipher,
    TokenError,
    audit_context_files,
    initialize_key_file,
    rotate_key,
)


class TemporaryKeyFile:
    def __enter__(self):
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary_directory.name)
        self.env_path = self.root / ".env"
        initialize_key_file(self.env_path)
        self.cipher = SensitiveDataCipher.from_env(self.env_path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._temporary_directory.cleanup()


class KeyFileTests(unittest.TestCase):
    def test_initializes_private_key_file_without_returning_key(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            env_path = Path(temp_directory) / ".env"

            result = initialize_key_file(env_path)

            self.assertEqual(result, "v1")
            self.assertEqual(env_path.stat().st_mode & 0o777, 0o600)
            contents = env_path.read_text(encoding="utf-8")
            self.assertIn("TRADE_CONTEXT_ACTIVE_KEY=v1", contents)
            self.assertIn("TRADE_CONTEXT_KEY_V1=", contents)

    def test_refuses_to_overwrite_existing_key_file(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            env_path = Path(temp_directory) / ".env"
            initialize_key_file(env_path)

            with self.assertRaises(KeyConfigurationError):
                initialize_key_file(env_path)

    def test_rejects_group_readable_key_file(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            env_path = Path(temp_directory) / ".env"
            initialize_key_file(env_path)
            os.chmod(env_path, 0o640)

            with self.assertRaisesRegex(KeyConfigurationError, "0600"):
                SensitiveDataCipher.from_env(env_path)

    def test_rotation_retains_old_decryption_key(self):
        with TemporaryKeyFile() as fixture:
            old_token = fixture.cipher.encrypt("entry-123", "broker_order_id")

            new_version = rotate_key(fixture.env_path)
            rotated_cipher = SensitiveDataCipher.from_env(fixture.env_path)

            self.assertEqual(new_version, "v2")
            self.assertEqual(rotated_cipher.active_version, "v2")
            self.assertEqual(
                rotated_cipher.decrypt(old_token, "broker_order_id").value,
                "entry-123",
            )
            self.assertTrue(
                rotated_cipher.encrypt("entry-456", "broker_order_id").startswith(
                    "enc:fernet:v2:"
                )
            )


class CipherTests(unittest.TestCase):
    def test_round_trip_preserves_exact_identifier_and_field(self):
        with TemporaryKeyFile() as fixture:
            value = "123e4567-e89b-12d3-a456-426614174000"

            token = fixture.cipher.encrypt(value, "client_ref_id")
            result = fixture.cipher.decrypt(token, "client_ref_id")

            self.assertEqual(result.value, value)
            self.assertEqual(result.field, "client_ref_id")
            self.assertEqual(result.key_version, "v1")

    def test_repeated_encryption_is_randomized(self):
        with TemporaryKeyFile() as fixture:
            first = fixture.cipher.encrypt("order-123", "broker_order_id")
            second = fixture.cipher.encrypt("order-123", "broker_order_id")

            self.assertNotEqual(first, second)
            self.assertEqual(
                fixture.cipher.decrypt(first).value,
                fixture.cipher.decrypt(second).value,
            )

    def test_detects_token_tampering(self):
        with TemporaryKeyFile() as fixture:
            token = fixture.cipher.encrypt("order-123", "broker_order_id")
            replacement = "A" if token[-2] != "A" else "B"
            tampered = token[:-2] + replacement + token[-1]

            with self.assertRaisesRegex(TokenError, "failed authentication"):
                fixture.cipher.decrypt(tampered)

    def test_rejects_wrong_expected_field(self):
        with TemporaryKeyFile() as fixture:
            token = fixture.cipher.encrypt("order-123", "broker_order_id")

            with self.assertRaisesRegex(TokenError, "not expected field"):
                fixture.cipher.decrypt(token, "client_ref_id")

    def test_short_identifier_crypto_is_well_below_order_latency(self):
        with TemporaryKeyFile() as fixture:
            started = time.perf_counter()
            for index in range(250):
                value = f"order-{index}"
                token = fixture.cipher.encrypt(value, "broker_order_id")
                self.assertEqual(fixture.cipher.decrypt(token).value, value)
            elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 2.0)


class AuditTests(unittest.TestCase):
    def test_accepts_encrypted_values_and_blank_template_fields(self):
        with TemporaryKeyFile() as fixture:
            token = fixture.cipher.encrypt("order-123", "broker_order_id")
            context = fixture.root / "context.md"
            context.write_text(
                "\n".join(
                    [
                        "- Broker entry order ID: `" + token + "`",
                        "- Client ref ID:",
                        "- Confirmation ID: TBD",
                    ]
                ),
                encoding="utf-8",
            )

            audit = audit_context_files([context], fixture.cipher)

            self.assertEqual(audit.checked_files, 1)
            self.assertEqual(audit.encrypted_values, 1)
            self.assertEqual(audit.violations, ())

    def test_rejects_plaintext_identifier(self):
        with TemporaryKeyFile() as fixture:
            context = fixture.root / "context.md"
            context.write_text(
                "- Broker stop order ID: real-order-id-123\n",
                encoding="utf-8",
            )

            audit = audit_context_files([context], fixture.cipher)

            self.assertEqual(len(audit.violations), 1)
            self.assertIn("not encrypted", audit.violations[0])

    def test_rejects_uuid_in_order_table_row(self):
        with TemporaryKeyFile() as fixture:
            context = fixture.root / "context.md"
            context.write_text(
                "| 09:40 | order accepted | 123e4567-e89b-42d3-a456-426614174000 |\n",
                encoding="utf-8",
            )

            audit = audit_context_files([context], fixture.cipher)

            self.assertEqual(len(audit.violations), 1)
            self.assertIn("UUID-shaped", audit.violations[0])


if __name__ == "__main__":
    unittest.main()

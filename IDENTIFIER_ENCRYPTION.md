# Identifier Privacy and Encryption

Account inspection on the clean branch is read-only and normally requires no
persisted exact identifier. Discover the authorized Agentic account at runtime;
never guess or store an account number merely for convenience.

If a future authorized maintenance workflow genuinely requires an exact broker
or transaction identifier in a tracked context, encrypt it with
`sensitive_data.py`. The existing `TRADE_CONTEXT_*` environment names are kept
for backward compatibility with the private key ring; they do not imply an
active trading workflow.

## Never persist

Do not store these values in Git, even encrypted:

- passwords or PINs;
- access, refresh, or API authentication tokens;
- MFA material;
- browser cookies or session state;
- broker credentials; or
- private connector payloads.

## Key setup

`.env` must be ignored and mode `0600` or stricter.

```sh
python3 sensitive_data.py init-key
python3 sensitive_data.py check
```

Rotate without deleting old keys needed to decrypt retained tokens:

```sh
python3 sensitive_data.py rotate-key
```

Never commit `.env` or copy its key material into an archive manifest.

## Encrypt and audit

Read the value from the hidden prompt rather than putting it in shell history:

```sh
python3 sensitive_data.py encrypt --field other_identifier
```

Store only the resulting marked `enc:fernet:vN:...` token. Audit tracked account
context before committing:

```sh
python3 sensitive_data.py audit history
```

The audit rejects plaintext values following sensitive labels, malformed or
undecryptable tokens, and UUID-shaped values in identifier context. The current
`history/ACCOUNT_HISTORY.md` intentionally contains no encrypted value because
no exact identifier is needed.

Encryption protects a narrowly scoped identifier at rest; it does not authorize
account mutation, order review, order placement, cancellation, or trading.

# Trade Identifier Encryption

Reviewed: 2026-07-15 (ET)

This repository is public, but exact broker order and client reference IDs are
necessary for reliable order correlation. `sensitive_data.py` stores those exact
values as authenticated Fernet ciphertext directly in the existing Markdown
trade context. There is no separate private trade-state store.

The ignored `.env` contains the only private material: the encryption key ring.
The real `.env` was initialized locally with key version `v1` and mode `0600`.
`.env.example` documents variable names without containing a key.

## What To Encrypt

Encrypt these exact values whenever the broker/tool returns them:

- Broker entry, stop, replacement, and exit order IDs
- Client-generated order `ref_id` UUIDs
- Broker confirmation IDs
- Cancellation and replacement IDs
- Another exact transaction identifier needed for reconciliation

Do not persist passwords, access or refresh tokens, MFA material, session
cookies, broker credentials, or private tool payloads. Do not persist an account
number merely for convenience; discover the authorized Agentic account live.

## Install And Check

```sh
python3 -m pip install -r requirements.txt
python3 sensitive_data.py check
python3 sensitive_data.py audit
```

`check` validates `.env`, requires permissions of `0600` or stricter, constructs
all configured keys, and performs an authenticated round trip. `audit` scans
`TRADES.md` and Markdown under `trades/`, validates every encrypted token it
finds, and rejects plaintext values on sensitive identifier fields.

Run both before a live workflow. Run `audit` again before every commit.

## Encrypt

Run the command without putting the private value in the command itself:

```sh
python3 sensitive_data.py encrypt --field broker_order_id
```

At the hidden `Sensitive value:` prompt, paste the exact identifier. The command
prints only a token similar to:

```text
enc:fernet:v1:gAAAAA...
```

Paste that complete token into the appropriate encrypted identifier record in
the session context. Supported fields are:

- `broker_order_id`
- `client_ref_id`
- `confirmation_id`
- `cancellation_id`
- `replacement_id`
- `account_number` only when exact persistence is operationally required
- `other_identifier`

Fernet tokens intentionally expose their creation timestamp. Session and order
times are already public in this experiment; the identifier value and field
payload remain encrypted and authenticated.

For Codex automation, start the encrypt command in a PTY and provide the private
value through stdin. Do not use `--value`, `echo`, command substitution, or
another form that places plaintext in shell history or the process list.

## Decrypt And Reconcile

Ciphertext is public, so it can be provided as a command argument:

```sh
python3 sensitive_data.py decrypt --expect-field broker_order_id \
  'enc:fernet:v1:gAAAAA...'
```

The command prints the exact original value. Use it transiently to correlate with
fresh broker order history. Never copy decrypted output into Markdown, email,
commit messages, source code, settings, or logs.

Broker state remains authoritative. At session start, query current accounts,
positions, and orders even when context decrypts successfully. After a timeout or
transport error, query broker orders before any retry; never rely only on a saved
identifier.

## Trading Critical Path

Encryption is intentionally outside the latency-sensitive broker sequence:

1. Submit or reconcile the broker action.
2. If an entry filled, establish protective-stop coverage immediately.
3. Confirm order state and resolve any partial fill or unknown outcome.
4. Encrypt and write exact identifiers into context.
5. Update email and Git only after safety-critical work.

When resuming a session, prefer exact IDs returned by the fresh broker query. Only
decrypt historical context when it materially improves correlation. If the key
tool fails during exposure, continue protection or flattening first and repair
the journal afterward.

## Measured Performance

The implementation caches parsed keys and Fernet instances within a process.
`cryptography` performs the underlying operations in optimized native code.

Measured locally on 2026-07-15 with Python 3.14.2 and `cryptography` 49.0.0 over
10,000 warmed UUID-sized round trips:

| Operation | Median | p95 |
| --- | ---: | ---: |
| Encrypt | 16.500 microseconds | 19.333 microseconds |
| Decrypt | 14.834 microseconds | 16.459 microseconds |
| Complete round trip | - | 36.667 microseconds |

A cold `python3 sensitive_data.py check` process took 0.08 seconds. This startup
cost is still outside broker execution and stop-protection work. Re-run the
benchmark only while flat and outside an active setup:

```sh
python3 sensitive_data.py benchmark --iterations 10000
```

## Key Handling And Rotation

The local `.env` format is:

```dotenv
TRADE_CONTEXT_ACTIVE_KEY=v1
TRADE_CONTEXT_KEY_V1=<private-url-safe-key>
```

Never print, email, screenshot, or commit the real key. Keep `.env` mode `0600`
and retain a secure external backup. The repository cannot recover ciphertext if
the key ring is lost.

Rotate only while flat and outside a live workflow:

```sh
python3 sensitive_data.py rotate-key
python3 sensitive_data.py check
```

Rotation adds `v2`, makes it active for new tokens, and retains `v1` locally so
old tokens remain readable. Tokens are versioned, so there is no need to rewrite
historical context. Key rotation cannot revoke ciphertext already present in Git
history if an old key is compromised.

## Publishing Checklist

Before committing any trade-context change:

```sh
python3 sensitive_data.py audit
git check-ignore -q .env
git status --short
```

Confirm that `.env` does not appear in staged files. Only `.env.example`, source,
tests, documentation, and authenticated ciphertext may be published.

## References

- Fernet authenticated encryption and key rotation:
  https://cryptography.io/en/latest/fernet/
- `python-dotenv` configuration loading:
  https://github.com/theskumar/python-dotenv

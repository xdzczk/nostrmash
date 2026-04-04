# Relay Payload Fixture Format

Replay fixtures are newline-delimited JSON (`.ndjson`) files.

Each line is one ingest input in deterministic order:

```json
{"relay_url":"wss://relay.example","payload":{"id":"...","pubkey":"...","created_at":0,"kind":1,"tags":[],"content":"","sig":"..."}}
```

Rules:

- Files are loaded in lexical path order when replaying a directory.
- Lines are replayed exactly in file order.
- `payload` must be the raw Nostr event JSON object (captured wire payload).
- Blank lines are ignored.

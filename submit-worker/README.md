# tyo-submit Worker

Receives Tag You're Out Top->Out results JSON and hands the original bytes to the poller.

```text
submit.html --POST /submit--> Worker + KV <--GET /pending, /log/<id>-- tyo_submissions_poll.py --ci
            <--GET /status--             <--POST /status/<id>------ (GitHub Action, every 15 min,
                                                                    Saturday 18:00 to Sunday 11:45 UTC)
```

## Storage

KV binding `SUBMISSIONS` points to namespace `TYO_SUBMISSIONS`.

| Key | Value |
|---|---|
| `log:<id>` | Original bytes, including BOM. Metadata: `event`, `sha256`, `size`. |
| `sub:<id>` | Authoritative submission record (JSON). |
| `sha:<sha256>` | Earlier submission id for that exact file. |
| `index` | Records newest first, capped at `MAX_INDEX` (200). |

Record fields: `id, event, date, submitter, created, updated, status, size, sha256, n_rounds, preview, summary, note`.
No IP address, user agent or Turnstile data is stored. The connecting IP is only passed to Turnstile verification.
Ids look like `20261022T1409-12142fb0`: UTC minute plus eight random hex digits.

## Endpoints

Responses are JSON except `GET /log/<id>`. Errors have shape `{"ok":false,"error":"<code>","message":"...","details":{...}}`, with optional `details` and CORS headers for allowed origins.

| Method and path | Access | Behavior |
|---|---|---|
| `GET /` | Public | Health check for `tyo-submit`. |
| `POST /submit` | Allowed Origin and Turnstile | Multipart `file`, `event`, `date`, optional `submitter`, optional `preview` (JSON), `cf-turnstile-response`. |
| `GET /status` | Public | Latest 50 records: `id, event, date, submitter, created, updated, status, n_rounds, summary, note`. |
| `GET /pending` | Poller | Indexed records with status `received` or `processed`. |
| `GET /log/<id>` | Poller | Original bytes and `X-Sha256` header. |
| `POST /status/<id>` | Poller | JSON with `status`, optional `note`, optional `summary`. |
| `POST /reindex` | Poller | Rebuild index from authoritative `sub:` records. |

Poller endpoints require `Authorization: Bearer <POLLER_TOKEN>`, checked by constant-time comparison of SHA-256 hashes.
Statuses: `received`, `processed`, `failed`, `published`, `duplicate`, `superseded`.
Preview retains COTD handling: optional JSON up to 2048 characters, retaining `winner` (up to 60 characters), integer `players` and integer `rounds`. Invalid or oversized preview JSON is ignored.

## /submit rejection list

- Missing or disallowed Origin, unreadable multipart, missing file, empty file or file over 1 MB.
- Invalid JSON (`not_json`). A leading UTF-8 BOM is accepted.
- Non-integer `settings.lives`, a `settings.threshold` key (Kerki/topout rather than Tag You're Out), missing or empty `rounds`, or a round missing a non-empty `level.Name` or non-empty `playerResults` (`not_tyo`). Steam IDs are never inspected.
- Event outside whole numbers 58 through 999: "Cup number must be a whole number between 58 and 999."
- A date other than a real `YYYY-MM-DD`, more than 60 days before today or more than one day after today (UTC).
- Trimmed submitter name longer than 40 characters.
- Any multipart string starting with `--` after trimming, or containing line breaks or NUL characters.
- Failed Turnstile verification, including hostname mismatch.
- `DAILY_SUBMIT_CAP` (20) indexed submissions already received within 24 hours.

Every submission is kept on record. An identical file (same SHA-256) still gets its own `sub:` record with status `duplicate`, `same_file_as: <first id>` and the note "same file as <id>", prefixed "REVIEW: " when it was sent under a different cup number than the first time. The bytes are not stored again (2 KV writes). The daily cap is checked first, so resends count towards it.

## Index limitation

The index uses read-modify-write, so concurrent writes can drop an index entry. Authoritative `sub:` records remain stored. Status updates reinsert missing entries; `/reindex` rebuilds the index from all `sub:` keys, capped at `MAX_INDEX`. The poller calls `/reindex` in its first pass of each hour (about 18 times a weekend), or on `--reindex`. Public status, pending results and the daily cap use the bounded index. KV eventual consistency also allows simultaneous identical submissions to race before the dedupe key is visible.

## ONE-TIME SETUP

The Cloudflare account, Wrangler login and Turnstile widget already exist. Reuse them. These are operator setup instructions; no network setup commands were run when creating this folder.

1. Install the development dependency in this folder with `npm install` when ready for deployment.
2. Create separate TyO namespaces:
   ```powershell
   npx wrangler kv namespace create TYO_SUBMISSIONS
   npx wrangler kv namespace create TYO_SUBMISSIONS --preview
   ```
   Paste the ids into the placeholders in `wrangler.toml`. Keep binding `SUBMISSIONS`.
3. Reuse the existing `cotd-submit` Turnstile widget's site key in the TyO submit page. Copy that widget's secret from the dashboard to the clipboard, then run:
   ```powershell
   python -c "import subprocess; subprocess.run('npx wrangler secret put TURNSTILE_SECRET', shell=True, input=subprocess.run(['powershell','-NoProfile','Get-Clipboard'], capture_output=True, text=True).stdout.strip(), text=True)"
   ```
   Do NOT pipe from PowerShell (`Get-Clipboard | npx wrangler ...` or `$x | npx wrangler ...`): PowerShell 5 adds a line break to the piped text and the Worker stores it as part of the secret (seen 2026-10-22: every poller call got 401). Hostnames stay `aizpunr.github.io`, `localhost`, `127.0.0.1`. Never put the secret in tracked files.
4. Generate a NEW TyO poller token (never reuse COTD's or PCDJ's), store it the same way (from Python, not a PowerShell pipe), and keep a copy for the GitHub secret. The 2026-10-22 token is in `C:\Users\rafa\.claude\local\tyo_poller_token.txt`.
5. Deploy with `npx wrangler deploy`. Put the printed URL (expected `https://tyo-submit.cotd-submit.workers.dev`) in `submit.html` as `PROD_WORKER_URL`. Production Origin is `https://aizpunr.github.io`; local page Origins use port 9007.
6. In GitHub repository Settings, Secrets and variables, Actions, add `TYO_WORKER_URL` (deployed URL) and `TYO_POLLER_TOKEN` (the new Worker's token).
7. Under Settings, Actions, General, Workflow permissions, select **Read and write permissions** and enable **Allow GitHub Actions to create and approve pull requests**. The workflow itself is `.github/workflows/process-submission.yml` (every 15 minutes, Saturday 18:00 through Sunday 11:45 UTC, plus manual runs).

For local development, create a gitignored `.dev.vars`:

```
TURNSTILE_SECRET=1x0000000000000000000000000000000AA
POLLER_TOKEN=devtoken
TURNSTILE_HOSTNAMES=
```

The first value is Cloudflare's public always-pass test secret. `TURNSTILE_HOSTNAMES=` switches the hostname check off, since test answers carry a placeholder hostname.

## Day to day

| Task | Command |
|---|---|
| Local Worker with simulated KV | `npx wrangler dev --port 8797 --ip 127.0.0.1` (8787 is used by the COTD, PCDJ and Bonq Workers; two `wrangler dev` on one port both claim it on Windows) |
| Local page against it | `python -m http.server 9007` in the repo, open `http://localhost:9007/submit.html?worker=http://127.0.0.1:8797` |
| Poller rehearsal (scratch clone) | `TYO_POLLER_TOKEN=devtoken python tyo_submissions_poll.py --worker http://127.0.0.1:8797 --no-push` |
| Live logs | `npx wrangler tail` |
| Deploy | `npx wrangler deploy` |
| Tests, no install needed | `npm test` or `node --test test/` |

Rotate the poller token by generating a fresh value, storing it with `npx wrangler secret put POLLER_TOKEN`, and updating GitHub's `TYO_POLLER_TOKEN`.

## Free tier

Uses Workers and Workers KV, without R2. Limits are per Cloudflare account, shared with `cotd-submit` and `pcdj-submit`. A submission costs four KV writes, a status update two; reindex uses paginated KV list operations, record reads and one index write. Uploads are capped at 1 MB and typically about 100 KB. Check the account's current quotas before increasing traffic or caps.

# voyager-watcher

A small scheduled watcher for public space and weather data, run entirely on GitHub Actions.

Every ~5 minutes a workflow runs one cycle of `watcher.py`:

- Snapshots NASA's [DSN Now](https://eyes.nasa.gov/dsn/) feed (`dsn.xml`) and records any Voyager 1 / Voyager 2 contacts plus dishes flagged offline or anomalous.
- Every 30 minutes, records the Honolulu (PHNL) METAR from [aviationweather.gov](https://aviationweather.gov/) and computes crosswind/headwind components for runways 08R/26L and 04R/22L.
- Regenerates `state/summary.md` (last 24 hours) and commits all state back to this repo.
- Optionally POSTs a small JSON "wake" payload to a webhook when something notable happens.

GitHub can delay or skip scheduled runs under load, so the real cadence is "about every 5–15 minutes".

## Read the latest state

- `state/summary.md`: start here. Times are America/Edmonton with UTC in parentheses.
- `state/dsn.jsonl`: one JSON object per DSN cycle.
- `state/phnl_metar.jsonl`: one JSON object per METAR fetch.
- `state/wakes.log`: every wake-hook decision (`sent`, `would_wake`, `queued`, `skipped`, `failed`, `dropped`).
- `state/wake_state.json`: the baseline snapshot, recent wake history, any queued wake, and `feed_down_since`.

Raw URL for the summary:
`https://raw.githubusercontent.com/calebxia-pilot/voyager-watcher/main/state/summary.md`

The JSONL files and `wakes.log` are trimmed to the last 14 days on every cycle.

## Events

`voyager_acquired`, `voyager_lost`, `power_dip`, `unexpected_antenna`, `dsn_anomaly`, `phnl_crosswind` (a crosswind component of 15 kt or more on 26L/08R, sustained or gust).

- Events fire only on a *change* from a previous successful snapshot.
- A failed fetch is never a signal loss.
- **Placeholder feed = feed down.** Sometimes DSN Now serves a blank placeholder: every dish is parked at az 0 / el 90 with the same activity text, target `DSN`, and no signals. That cycle is recorded as `ok: false, feed_down: true`, the baseline is left untouched, and the summary shows "DSN feed down". When real data returns, the stale baseline isn't used to declare `voyager_lost` (nor power dips or unexpected antennas), because we can't know what happened during the outage.

Payload:

```json
{"event": "voyager_lost", "detail": "…", "time_utc": "2026-11-18T10:16:07Z"}
```

Auth headers come from `webhook_auth_headers()`: `Authorization: Bearer <key>` and `X-Webhook-Key: <key>`.

### Rate limits and quiet hours

- At most 4 wakes in any rolling 24 hours.
- At least 2 hours between wakes.
- The same event type is not repeated within 6 hours.
- No wakes from 22:00 to 07:00 America/Edmonton. The most important event in that window is queued and flushed on the first cycle after 07:00. Priority, highest first: `voyager_lost`, `dsn_anomaly`, `power_dip`, `voyager_acquired`, `unexpected_antenna`, `phnl_crosswind`.

## Configuration

Repository secrets (Settings → Secrets and variables → Actions):

- `CALEB_WATCHER_WEBHOOK_URL`
- `CALEB_WATCHER_WEBHOOK_KEY`

If either is missing, events are logged as `would_wake` and nothing is POSTed. For a local run, put them in a git-ignored `.env` file (see `.env.example`).

## Workflows

- `.github/workflows/watch.yml` runs on a `*/5 * * * *` cron and on manual dispatch (with an optional "force METAR" switch). It runs one cycle, then commits `state/` as `github-actions[bot]`. A concurrency group prevents overlapping runs, and a push conflict is retried up to five times with `git pull --rebase`.
- `.github/workflows/keepalive.yml` runs weekly. GitHub disables scheduled workflows in public repos after 60 days without repository activity. The state commits already count as activity. As a second guard, both workflows re-enable each other through the Actions API once a week (`watch.yml` does it too, at most weekly, tracked in `state/keepalive.txt`). If a workflow ever does get disabled, re-enable it from the Actions tab or with `gh workflow enable watch.yml -R calebxia-pilot/voyager-watcher`.

## Local run

```bash
python3 watcher.py --once                 # one cycle; METAR only if due
python3 watcher.py --once --force-metar   # one cycle with METAR
WATCHER_STATE_DIR=/tmp/state python3 watcher.py --once
```

This uses only the Python standard library (3.9+).

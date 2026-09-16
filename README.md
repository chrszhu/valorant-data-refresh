# Valorant Data Refresh

Automated data collection for [ValoStats](https://github.com/chrszhu/valorant).

Fetches Immortal+ competitive match data from the [Henrik API](https://docs.henrikdev.xyz/) and stores it in a Postgres database. Runs **weekly** via GitHub Actions cron (Monday 06:00 UTC).

To keep Neon free-tier compute usage low, the fetch loop makes **no DB calls** — matches are buffered in memory and written in one bulk insert per region, so the DB compute auto-suspends during the long API fetch.

## What it collects

- Leaderboard snapshots for all 6 regions (NA, EU, AP, KR, BR, LATAM)
- The last ~20 competitive matches from the top ~1000 Immortal+ players per region (tunable via `MAX_PLAYERS` / `MATCHES_PER_PLAYER`)
- Full team compositions (all 10 players) for each match
- Agent performance stats (damage, abilities, economy)

## Required secrets

| Secret | Description |
|---|---|
| `DATABASE_URL` | Neon Postgres connection string |
| `HENRIKDEV_API_KEY` | Henrik API key |

## Manual trigger

Go to Actions > Valorant Data Refresh > Run workflow

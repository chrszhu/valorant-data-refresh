# Valorant Data Refresh

Automated data collection for [ValoStats](https://github.com/chrszhu/valorant).

Fetches Immortal+ competitive match data from the [Henrik API](https://docs.henrikdev.xyz/) and stores it in a Postgres database. Runs daily via GitHub Actions cron (public repo = unlimited free minutes).

## What it collects

- Leaderboard snapshots for all 6 regions (NA, EU, AP, KR, BR, LATAM)
- Recent competitive matches from the top 100 Immortal+ players per region
- Full team compositions (all 10 players) for each match
- Agent performance stats (damage, abilities, economy)

## Required secrets

| Secret | Description |
|---|---|
| `DATABASE_URL` | Neon Postgres connection string |
| `HENRIKDEV_API_KEY` | Henrik API key |

## Manual trigger

Go to Actions > Valorant Data Refresh > Run workflow

# valorant-data-refresh

Weekly, database-free data collector for [ValoStats](https://github.com/chrszhu/valorant).

Runs on GitHub Actions (public repo = unlimited minutes). It fetches Immortal+
match data from the [Henrik API](https://docs.henrikdev.xyz/), keeps a deduped
raw match **accumulator**, and computes the static per-region JSON bundles the
site reads directly. **No database.**

## How it works

1. **Accumulator** — the raw match history is stored as a gzipped JSON file
   (`accumulator.json.gz`) attached to the `data-accumulator` GitHub Release. It
   is downloaded at the start of each run and re-uploaded (after every region)
   so a timeout only loses the in-progress region.
2. **Fetch** — for each region it pulls the leaderboard and the recent
   competitive matches of the top ~1000 Immortal+ players (20 matches each),
   appending new matches to the accumulator (deduped by match id).
3. **Compute** — it builds `data/static/{region}.json` for all regions and the
   `all` aggregate. Each map's meta is windowed to its last 90 days of activity,
   split by rank band (Immortal+ / Radiant). Maps with too little fresh data
   (retired maps) fall back to the all-time seed snapshot so they never vanish.
4. **Commit** — the computed bundles are committed here. The site repo pulls
   them weekly (`pull-static-data.yml`) and commits them into its own build.

## Secrets

- `HENRIKDEV_API_KEY` — Henrik API key.
- `GITHUB_TOKEN` — provided automatically; used to read/write the accumulator
  release and commit the bundles.

## Manual run

Actions → **Valorant Data Refresh** → *Run workflow*. Optionally set
`max_players` (default 1000, `0` = no cap).

## Local test (compute only, no fetch/release)

```bash
COMPUTE_ONLY=1 node refresh.mjs   # rebuilds bundles from a local accumulator.json.gz + seed
```

## Env vars

| var | default | meaning |
| --- | --- | --- |
| `REGION` | all six | single region to fetch |
| `MAX_PLAYERS` | 1000 | players per region (0 = no cap) |
| `MATCHES_PER_PLAYER` | 20 | recent matches per player |
| `COMPUTE_ONLY` | — | `1` = skip fetch + release, just rebuild bundles |

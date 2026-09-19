#!/usr/bin/env node
/**
 * Valorant data refresh — database-free.
 *
 * Fetches Immortal+ match data from the Henrik API, keeps a raw match
 * accumulator (gzipped JSON stored as a GitHub Release asset), and computes the
 * static per-region bundles the site reads directly (data/static/{region}.json).
 *
 * No Postgres. The site never touches a database — it just reads these files.
 *
 * Flow:
 *   1. Download the accumulator from the "data-accumulator" release (or empty).
 *   2. Fetch leaderboards + recent competitive matches; append new rows (deduped).
 *   3. Compute bundles (per-map 90-day window, rank split, abilities, comps),
 *      falling back to the seed snapshot for maps with too little fresh data
 *      (e.g. retired maps) so they never disappear.
 *   4. Write data/static/*.json, commit, and re-upload the accumulator.
 *
 * Env:
 *   HENRIKDEV_API_KEY  Henrik API key
 *   GH_TOKEN           token for `gh` (release download/upload) — GITHUB_TOKEN in CI
 *   REGION             optional single region (default: all six)
 *   MAX_PLAYERS        players per region (default 1000, 0 = no cap)
 *   MATCHES_PER_PLAYER recent matches per player (default 20)
 *   COMPUTE_ONLY=1     skip fetching + release; just rebuild bundles from a local
 *                      accumulator.json.gz (if present) + seed. For local testing.
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { gzipSync, gunzipSync } from "zlib";
import { execSync } from "child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = __dirname;
const OUT_DIR = resolve(ROOT, "data", "static");
const ACC_FILE = resolve(ROOT, "accumulator.json.gz");
const SEED_FILE = resolve(ROOT, "seed-agent-map-stats.json");
const RELEASE_TAG = "data-accumulator";

const ALL_REGIONS = ["na", "eu", "ap", "kr", "br", "latam"];
const OUT_REGIONS = ["all", ...ALL_REGIONS];
const REGIONS = process.env.REGION ? [process.env.REGION] : ALL_REGIONS;
const HENRIK_BASE = "https://api.henrikdev.xyz";
const DELAY_MS = 2500;
const MAX_PLAYERS_PER_REGION = parseInt(process.env.MAX_PLAYERS || "1000", 10);
const MATCHES_PER_PLAYER = parseInt(process.env.MATCHES_PER_PLAYER || "20", 10);
// Save + upload the accumulator mid-region every N players so a job killed
// partway through only loses the last few players, not the whole region. The
// next run resumes from here (dedupe skips already-stored matches).
const CHECKPOINT_EVERY = parseInt(process.env.CHECKPOINT_EVERY || "50", 10);
// Early-stop: if this many players in a row yield ZERO new matches, we've caught
// up to already-collected data for this region — stop scanning to save API calls.
// The first run (empty store) never triggers it; daily runs stop quickly once the
// day's new games are in. 0 = disabled (always scan the full player list).
const EARLY_STOP_STREAK = parseInt(process.env.EARLY_STOP_STREAK || "40", 10);
const COMPUTE_ONLY = process.env.COMPUTE_ONLY === "1";
// FETCH_ONLY: pull matches into the accumulator but don't compute/write bundles.
// Used by the per-region matrix jobs; a final COMPUTE_ONLY job builds the bundles.
const FETCH_ONLY = process.env.FETCH_ONLY === "1";
// Drop matches older than this from the accumulator. The site only shows each
// map's last 90 days, and we only ever collect current-rotation maps, so older
// data is never displayed — pruning keeps the accumulator at a bounded size
// forever, which makes running frequently sustainable. 0 = never prune.
const PRUNE_DAYS = parseInt(process.env.PRUNE_DAYS || "120", 10);

// Meta windowing + rank bands — mirrors the site's old DB logic.
const RECENT_WINDOW_DAYS = 90;
const RANK_BANDS = { all: 0, radiant: 27 }; // min tier per band (24=Imm1 … 27=Radiant)
// Prefer fresh accumulator data for a map only once it has a meaningful sample;
// below this a map falls back to the all-time seed aggregate (retired maps, or
// current maps in the first week or two of collection).
const MIN_MAP_GAMES = 200; // agent-appearances (5 per team) => ~20 matches
const MIN_ABILITY_GAMES = 20; // player rows for a character

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── Henrik API ────────────────────────────────────────────────────────────────

function getHeaders() {
  const key = process.env.HENRIKDEV_API_KEY ?? "";
  const headers = { "Content-Type": "application/json" };
  if (key && key !== "your_api_key_here") headers["Authorization"] = key;
  return headers;
}

// A 429 is NOT a failure — it just means "come back later". We wait out the
// server's reset window and retry indefinitely (bounded only by the per-region
// job timeout), so throttling slows us down but never drops data. Only real
// network errors count toward the give-up limit.
async function safeFetch(url, maxNetErrors = 5) {
  let netErrors = 0;
  let throttleWaits = 0;
  for (;;) {
    await sleep(DELAY_MS);
    try {
      const res = await fetch(url, { headers: getHeaders(), cache: "no-store" });
      if (res.status === 429) {
        throttleWaits++;
        // Prefer the server's own hints; fall back to a growing wait so a long
        // throttle backs off instead of hammering (10s → 20s → … capped 120s).
        const retryAfter = parseInt(res.headers.get("retry-after") || "0", 10);
        const resetSec = parseInt(res.headers.get("x-ratelimit-reset") || "0", 10);
        const waitSec = Math.min(
          Math.max(retryAfter, resetSec, Math.min(10 * throttleWaits, 120)) + 5,
          125,
        );
        log(`  Rate limited (wait #${throttleWaits}). Sleeping ${waitSec}s...`);
        await sleep(waitSec * 1000);
        continue;
      }
      return res;
    } catch (err) {
      netErrors++;
      log(`  Network error (${netErrors}/${maxNetErrors}): ${err.message}`);
      if (netErrors >= maxNetErrors) return null;
      await sleep(5000 * netErrors); // linear backoff on transient network errors
    }
  }
}

function extractPatch(gameVersion) {
  const m = gameVersion?.match(/release-0*(\d+\.\d+)/i);
  return m ? m[1] : "unknown";
}

const AGENT_ROLES = {
  Brimstone: "Controller", Viper: "Controller", Omen: "Controller", Astra: "Controller",
  Harbor: "Controller", Clove: "Controller", Tejo: "Controller",
  Jett: "Duelist", Phoenix: "Duelist", Reyna: "Duelist", Raze: "Duelist",
  Yoru: "Duelist", Neon: "Duelist", Iso: "Duelist", Waylay: "Duelist",
  Sova: "Initiator", Breach: "Initiator", Skye: "Initiator", "KAY/O": "Initiator",
  Fade: "Initiator", Gekko: "Initiator",
  Sage: "Sentinel", Cypher: "Sentinel", Killjoy: "Sentinel", Chamber: "Sentinel",
  Deadlock: "Sentinel", Vyse: "Sentinel",
};

function computeArchetype(agents) {
  const counts = { Controller: 0, Duelist: 0, Initiator: 0, Sentinel: 0 };
  for (const a of agents) counts[AGENT_ROLES[a] ?? "Duelist"]++;
  return ["Controller", "Duelist", "Initiator", "Sentinel"]
    .filter((r) => counts[r] > 0)
    .map((r) => `${counts[r]}${r[0]}`)
    .join("+");
}

// ─── Accumulator (raw match store) ───────────────────────────────────────────

function emptyAccumulator() {
  return { players: [], comps: [], leaderboards: {}, progress: {} };
}

// Drop matches older than PRUNE_DAYS so the accumulator stays bounded even when
// runs are frequent. Seed data (retired maps) is separate and never touched.
function pruneAccumulator(acc) {
  if (PRUNE_DAYS <= 0) return;
  const cutoff = Math.floor(Date.now() / 1000) - PRUNE_DAYS * 86400;
  const bp = acc.players.length, bc = acc.comps.length;
  acc.players = acc.players.filter((p) => (p.game_start || 0) >= cutoff);
  acc.comps = acc.comps.filter((c) => (c.game_start || 0) >= cutoff);
  if (acc.players.length !== bp || acc.comps.length !== bc) {
    log(`Pruned >${PRUNE_DAYS}d: players ${bp}->${acc.players.length}, comps ${bc}->${acc.comps.length}`);
  }
}

function downloadAccumulator() {
  // Try the release first (works in CI for every mode). Fall back to a local
  // file (local testing), else start empty.
  try {
    execSync(`gh release download ${RELEASE_TAG} -p accumulator.json.gz -O "${ACC_FILE}" --clobber`, {
      stdio: "pipe", cwd: ROOT,
    });
    log("Downloaded accumulator from release");
    return loadAccumulatorFile();
  } catch {
    if (existsSync(ACC_FILE)) {
      log("Using local accumulator.json.gz");
      return loadAccumulatorFile();
    }
    log("No accumulator (release or local) — starting empty");
    return emptyAccumulator();
  }
}

function loadAccumulatorFile() {
  try {
    const acc = JSON.parse(gunzipSync(readFileSync(ACC_FILE)).toString("utf-8"));
    acc.players ??= []; acc.comps ??= []; acc.leaderboards ??= {}; acc.progress ??= {};
    return acc;
  } catch (err) {
    log(`Failed to read accumulator (${err.message}) — starting empty`);
    return emptyAccumulator();
  }
}

function saveAccumulator(acc) {
  writeFileSync(ACC_FILE, gzipSync(Buffer.from(JSON.stringify(acc))));
  const sizeMb = (readFileSync(ACC_FILE).length / 1e6).toFixed(1);
  log(`Accumulator saved: ${acc.players.length} players, ${acc.comps.length} comps (${sizeMb} MB gz)`);
}

function uploadAccumulator() {
  if (COMPUTE_ONLY) return;
  try {
    execSync(`gh release view ${RELEASE_TAG}`, { stdio: "pipe", cwd: ROOT });
  } catch {
    execSync(`gh release create ${RELEASE_TAG} -t "Raw match accumulator" -n "Deduped raw match history. Regenerated weekly."`, {
      stdio: "inherit", cwd: ROOT,
    });
  }
  execSync(`gh release upload ${RELEASE_TAG} "${ACC_FILE}" --clobber`, { stdio: "inherit", cwd: ROOT });
  log("Uploaded accumulator to release");
}

// ─── Fetch one region into the accumulator ───────────────────────────────────

async function fetchRegion(region, acc) {
  log(`\n--- ${region.toUpperCase()} ---`);

  const lbRes = await safeFetch(`${HENRIK_BASE}/valorant/v3/leaderboard/${region}/pc`);
  if (!lbRes || !lbRes.ok) {
    log("  Failed to fetch leaderboard");
    return 0;
  }
  const lbData = await lbRes.json();
  const allPlayers = lbData.data?.players ?? [];

  acc.leaderboards[region] = {
    updatedAt: Math.floor(Date.now() / 1000),
    players: allPlayers.map((p, idx) => ({
      rank: idx + 1, name: p.name ?? "", tag: p.tag ?? "", tier: p.tier,
      rr: p.rankedRating ?? 0, wins: p.numberOfWins ?? 0, is_anonymized: p.is_anonymized,
    })),
  };
  log(`  Cached ${acc.leaderboards[region].players.length} leaderboard entries`);

  const existingIds = new Set(acc.players.filter((p) => p.region === region).map((p) => p.match_id));
  log(`  ${existingIds.size} matches already stored for ${region}`);

  let players = allPlayers.filter((p) => !p.is_anonymized && p.name && p.tag && p.tier >= 24);
  if (MAX_PLAYERS_PER_REGION > 0) players = players.slice(0, MAX_PLAYERS_PER_REGION);
  log(`  Checking ${players.length} Immortal+ players`);

  let regionMatches = 0;
  let playersChecked = 0;
  let zeroStreak = 0; // consecutive players with no new matches (early-stop signal)
  const seen = new Set();

  // Resume cursor: if a previous run was killed mid-sweep (e.g. hit the 6h cap at
  // a high player count), pick up where it left off instead of re-scanning from
  // the top. Reset to 0 once a full sweep completes so the next run starts fresh.
  acc.progress ??= {};
  const startIdx = Math.min(acc.progress[region] || 0, players.length);
  if (startIdx > 0) log(`  Resuming mid-sweep from player #${startIdx + 1}/${players.length}`);

  for (let i = startIdx; i < players.length; i++) {
    const player = players[i];
    const matchesBefore = regionMatches;
    try {
      const matchRes = await safeFetch(
        `${HENRIK_BASE}/valorant/v3/matches/${region}/${encodeURIComponent(player.name)}/${encodeURIComponent(player.tag)}?filter=competitive&size=${MATCHES_PER_PLAYER}`,
      );
      if (!matchRes || !matchRes.ok) { playersChecked++; continue; }
      const matchData = await matchRes.json();
      const matches = matchData.data ?? [];

      for (const match of matches) {
        try {
          if (!match?.metadata || match.metadata.mode_id !== "competitive") continue;
          const matchId = match.metadata.matchid;
          if (existingIds.has(matchId) || seen.has(matchId)) continue;

          const mapName = match.metadata.map;
          const patch = extractPatch(match.metadata.game_version ?? "");
          const gameStart = match.metadata.game_start ?? 0;
          const actualPlayer = match.players.all_players.find(
            (p) => p.name?.toLowerCase() === player.name.toLowerCase() &&
                   p.tag?.toLowerCase() === player.tag.toLowerCase(),
          );
          if (!actualPlayer) continue;
          const team = actualPlayer.team?.toLowerCase();
          const teamResults = match.teams ?? {};
          if (!teamResults[team]) continue;

          seen.add(matchId);

          let won = false, teamRoundsWon = 0, teamRoundsLost = 0;
          let rounds = match.metadata.rounds_played || 1;
          if (typeof teamResults.red === "number" && typeof teamResults.blue === "number") {
            const myRounds = teamResults[team] ?? 0;
            const oppRounds = teamResults[team === "red" ? "blue" : "red"] ?? 0;
            won = myRounds > oppRounds; teamRoundsWon = myRounds; teamRoundsLost = oppRounds;
            rounds = myRounds + oppRounds || rounds;
          } else if (teamResults.red && teamResults.blue) {
            const myTeam = teamResults[team];
            won = myTeam.has_won ?? false;
            teamRoundsWon = myTeam.rounds_won ?? 0;
            teamRoundsLost = myTeam.rounds_lost ?? 0;
            rounds = teamRoundsWon + teamRoundsLost || rounds;
          }

          const trackedTier = actualPlayer.currenttier ?? player.tier ?? 0;
          const rosterTiers = (match.players?.all_players ?? [])
            .map((p) => p.currenttier ?? 0).filter((n) => n > 0);
          const anchorTier = rosterTiers.length ? Math.max(...rosterTiers) : (player.tier ?? 0);

          acc.players.push({
            region, match_id: matchId, map: mapName, mode_id: "competitive",
            game_start: gameStart, total_rounds: rounds,
            puuid: actualPlayer.puuid, character: actualPlayer.character,
            agent_image_url: actualPlayer.assets?.agent?.small ?? "",
            won: won ? 1 : 0,
            score: actualPlayer.stats.score, headshots: actualPlayer.stats.headshots,
            bodyshots: actualPlayer.stats.bodyshots, legshots: actualPlayer.stats.legshots,
            damage_made: actualPlayer.damage_made ?? 0,
            c_cast: actualPlayer.ability_casts?.c_cast ?? 0,
            q_cast: actualPlayer.ability_casts?.q_cast ?? 0,
            e_cast: actualPlayer.ability_casts?.e_cast ?? 0,
            x_cast: actualPlayer.ability_casts?.x_cast ?? 0,
            econ_spent_avg: actualPlayer.economy?.spent?.average ?? 0,
            econ_loadout_avg: actualPlayer.economy?.loadout_value?.average ?? 0,
            patch, current_tier: trackedTier,
          });

          if (teamResults.red && teamResults.blue && match.players?.all_players) {
            for (const side of ["red", "blue"]) {
              const sideAgents = match.players.all_players
                .filter((p) => p.team?.toLowerCase() === side)
                .map((p) => p.character).filter(Boolean).sort();
              if (sideAgents.length !== 5) continue;
              let sideWon;
              if (typeof teamResults[side] === "number") {
                sideWon = teamResults[side] > (teamResults[side === "red" ? "blue" : "red"] ?? 0);
              } else {
                sideWon = teamResults[side]?.has_won ?? false;
              }
              acc.comps.push({
                region, match_id: matchId, team: side, map: mapName,
                agents_sorted: sideAgents.join(","), archetype: computeArchetype(sideAgents),
                won: sideWon ? 1 : 0, patch, anchor_tier: anchorTier, game_start: gameStart,
              });
            }
          }
          regionMatches++;
        } catch (matchErr) {
          log(`    ⚠ match error: ${matchErr.message}`);
        }
      }
    } catch (playerErr) {
      log(`    ⚠ player ${player.name}#${player.tag}: ${playerErr.message}`);
    }
    playersChecked++;
    if (playersChecked % 25 === 0) {
      log(`  Progress: ${i + 1}/${players.length}, ${regionMatches} new matches`);
    }
    // Mid-region checkpoint: persist progress + resume cursor so a timeout here
    // resumes from ~here next run instead of re-scanning from the top.
    if (CHECKPOINT_EVERY > 0 && playersChecked % CHECKPOINT_EVERY === 0) {
      acc.progress[region] = i + 1;
      saveAccumulator(acc);
      uploadAccumulator();
      log(`  ⏱ Checkpoint saved at ${i + 1}/${players.length}`);
    }
    // Early-stop once we hit a wall of already-collected data.
    if (regionMatches === matchesBefore) {
      zeroStreak++;
      if (EARLY_STOP_STREAK > 0 && zeroStreak >= EARLY_STOP_STREAK) {
        log(`  ⏹ Caught up: ${zeroStreak} players in a row with no new matches — stopping ${region.toUpperCase()} at ${i + 1}/${players.length}.`);
        break;
      }
    } else {
      zeroStreak = 0;
    }
  }
  // Sweep concluded (finished the list or caught up) — reset the cursor so the
  // next run starts a fresh top-down sweep and picks up new games from top players.
  acc.progress[region] = 0;

  log(`  ✓ ${region.toUpperCase()}: ${playersChecked} players, ${regionMatches} new matches`);
  return regionMatches;
}

// ─── Seed fallback (all-time aggregate) ──────────────────────────────────────

const PERF_KEYS = [
  "games", "wins", "total_rounds", "total_damage", "total_score",
  "total_hs", "total_bs", "total_ls", "total_c_casts", "total_q_casts",
  "total_e_casts", "total_x_casts", "total_econ_spent", "total_loadout_value",
];

let SEED = [];
try {
  SEED = JSON.parse(readFileSync(SEED_FILE, "utf-8"));
} catch {
  log("No seed file found — retired maps will be empty until fresh data arrives");
}

// Seed rows for a region. "all" aggregates the per-region rows (they carry perf).
function seedRows(region) {
  if (region !== "all") return SEED.filter((r) => r.region === region);
  const acc = {};
  for (const r of SEED) {
    if (r.region === "all") continue;
    const key = `${r.map}::${r.agent}`;
    const a = (acc[key] ??= {
      region: "all", map: r.map, agent: r.agent, agent_image_url: r.agent_image_url || "",
      updated_at: 0, ...Object.fromEntries(PERF_KEYS.map((k) => [k, 0])),
    });
    if (!a.agent_image_url && r.agent_image_url) a.agent_image_url = r.agent_image_url;
    for (const k of PERF_KEYS) a[k] += r[k] || 0;
    a.updated_at = Math.max(a.updated_at, r.updated_at || 0);
  }
  return Object.values(acc);
}

// ─── Compute bundles from the accumulator ────────────────────────────────────

const NOW = Math.floor(Date.now() / 1000);

// Windowed agent-map stats for one region + rank band (mirrors computeAgentMapStats).
function computeMapStats(region, minTier, comps, players) {
  const isAll = region === "all";
  const winSecs = RECENT_WINDOW_DAYS * 86400;

  const compMax = {}, playerMax = {};
  for (const c of comps) {
    if (!(isAll || c.region === region) || !c.game_start) continue;
    compMax[c.map] = Math.max(compMax[c.map] || 0, c.game_start);
  }
  for (const p of players) {
    if (!(isAll || p.region === region) || !p.game_start) continue;
    playerMax[p.map] = Math.max(playerMax[p.map] || 0, p.game_start);
  }

  const picks = {};
  for (const c of comps) {
    if (!(isAll || c.region === region) || !c.game_start) continue;
    if ((c.anchor_tier || 0) < minTier) continue;
    if (c.game_start < compMax[c.map] - winSecs) continue;
    for (const a of c.agents_sorted.split(",").filter(Boolean)) {
      const k = `${c.map}::${a}`;
      const o = (picks[k] ??= { games: 0, wins: 0 });
      o.games++; if (c.won) o.wins++;
    }
  }

  const perf = {};
  for (const p of players) {
    if (!(isAll || p.region === region) || !p.game_start) continue;
    if ((p.current_tier || 0) < minTier) continue;
    if (p.game_start < playerMax[p.map] - winSecs) continue;
    const k = `${p.map}::${p.character}`;
    const o = (perf[k] ??= { img: "", rd: 0, dmg: 0, sc: 0, hs: 0, bs: 0, ls: 0, cc: 0, qc: 0, ec: 0, xc: 0, esp: 0, elv: 0 });
    if (p.agent_image_url && !o.img) o.img = p.agent_image_url;
    o.rd += p.total_rounds || 0; o.dmg += p.damage_made || 0; o.sc += p.score || 0;
    o.hs += p.headshots || 0; o.bs += p.bodyshots || 0; o.ls += p.legshots || 0;
    o.cc += p.c_cast || 0; o.qc += p.q_cast || 0; o.ec += p.e_cast || 0; o.xc += p.x_cast || 0;
    o.esp += p.econ_spent_avg || 0; o.elv += p.econ_loadout_avg || 0;
  }

  const rows = [];
  for (const [k, pk] of Object.entries(picks)) {
    const [map, agent] = k.split("::");
    const pf = perf[k] || {};
    rows.push({
      region: isAll ? "all" : region, map, agent, agent_image_url: pf.img || "",
      games: pk.games, wins: pk.wins,
      total_rounds: pf.rd || 0, total_damage: pf.dmg || 0, total_score: pf.sc || 0,
      total_hs: pf.hs || 0, total_bs: pf.bs || 0, total_ls: pf.ls || 0,
      total_c_casts: pf.cc || 0, total_q_casts: pf.qc || 0, total_e_casts: pf.ec || 0, total_x_casts: pf.xc || 0,
      total_econ_spent: pf.esp || 0, total_loadout_value: pf.elv || 0, updated_at: NOW,
    });
  }
  return { rows, compMax };
}

function groupByMap(rows) {
  const g = {};
  for (const r of rows) (g[r.map] ??= []).push(r);
  return g;
}

// Merge fresh (windowed) rows with the seed, per map. Fresh wins once a map has
// a real sample; otherwise the map falls back to the all-time seed aggregate.
function mergeAgentMapStats(region, minTier, comps, players) {
  const { rows: fresh, compMax } = computeMapStats(region, minTier, comps, players);
  const byFresh = groupByMap(fresh);
  const bySeed = groupByMap(seedRows(region));
  const activeCutoff = NOW - RECENT_WINDOW_DAYS * 86400;
  const out = [];
  for (const map of new Set([...Object.keys(byFresh), ...Object.keys(bySeed)])) {
    const f = byFresh[map] || [];
    const freshGames = f.reduce((s, r) => s + r.games, 0);
    if (freshGames >= MIN_MAP_GAMES) {
      const window = (compMax[map] || 0) >= activeCutoff ? "recent" : "all";
      for (const r of f) out.push({ ...r, window });
    } else {
      for (const r of bySeed[map] || []) out.push({ ...r, window: "all" });
    }
  }
  out.sort((a, b) => a.map.localeCompare(b.map) ||
    (b.wins / Math.max(b.games, 1)) - (a.wins / Math.max(a.games, 1)));
  return out;
}

// Ability stats per character (fresh preferred, else seed).
function mergeAbilityStats(region, minTier, players) {
  const isAll = region === "all";
  const agg = {};
  for (const p of players) {
    if (!(isAll || p.region === region) || p.mode_id !== "competitive") continue;
    if ((p.current_tier || 0) < minTier) continue;
    const a = (agg[p.character] ??= { character: p.character, img: "", games: 0, rd: 0, c: 0, q: 0, e: 0, x: 0 });
    if (p.agent_image_url && !a.img) a.img = p.agent_image_url;
    a.games++; a.rd += p.total_rounds || 0;
    a.c += p.c_cast || 0; a.q += p.q_cast || 0; a.e += p.e_cast || 0; a.x += p.x_cast || 0;
  }

  // Seed abilities (all-time, per character).
  const seedAgg = {};
  for (const r of seedRows(region)) {
    const a = (seedAgg[r.agent] ??= { character: r.agent, img: r.agent_image_url || "", games: 0, rd: 0, c: 0, q: 0, e: 0, x: 0 });
    a.games += r.games; a.rd += r.total_rounds;
    a.c += r.total_c_casts; a.q += r.total_q_casts; a.e += r.total_e_casts; a.x += r.total_x_casts;
  }

  const toRow = (a) => ({
    character: a.character, agent_image_url: a.img, games: a.games, total_rounds: a.rd,
    avg_c_per_game: a.c / Math.max(a.games, 1), avg_q_per_game: a.q / Math.max(a.games, 1),
    avg_e_per_game: a.e / Math.max(a.games, 1), avg_x_per_game: a.x / Math.max(a.games, 1),
    c_per_round: a.c / Math.max(a.rd, 1), q_per_round: a.q / Math.max(a.rd, 1),
    e_per_round: a.e / Math.max(a.rd, 1), x_per_round: a.x / Math.max(a.rd, 1),
  });

  const out = [];
  for (const ch of new Set([...Object.keys(agg), ...Object.keys(seedAgg)])) {
    const f = agg[ch];
    if (f && f.games >= MIN_ABILITY_GAMES && f.rd > 0) out.push(toRow(f));
    else if (seedAgg[ch] && seedAgg[ch].rd > 0 && seedAgg[ch].games >= 3) out.push(toRow(seedAgg[ch]));
  }
  return out.sort((a, b) => b.games - a.games);
}

// Team comps + archetypes from the accumulator (min 2 games), plus agent images.
function computeTeamComps(region, comps, players) {
  const isAll = region === "all";
  const rc = comps.filter((c) => isAll || c.region === region);
  const compAcc = {}, archAcc = {};
  for (const c of rc) {
    const ck = `${c.map}::${c.agents_sorted}`;
    const co = (compAcc[ck] ??= { agents_sorted: c.agents_sorted, archetype: c.archetype, map: c.map, games: 0, wins: 0 });
    co.games++; if (c.won) co.wins++;
    const ak = `${c.map}::${c.archetype}`;
    const ao = (archAcc[ak] ??= { archetype: c.archetype, map: c.map, games: 0, wins: 0 });
    ao.games++; if (c.won) ao.wins++;
  }
  const topComps = Object.values(compAcc).filter((c) => c.games >= 2)
    .map((c) => ({ ...c, win_rate: c.wins / c.games }))
    .sort((a, b) => a.map.localeCompare(b.map) || b.win_rate - a.win_rate || b.games - a.games);
  const archetypes = Object.values(archAcc).filter((a) => a.games >= 2)
    .map((a) => ({ ...a, win_rate: a.wins / a.games }))
    .sort((a, b) => a.map.localeCompare(b.map) || b.win_rate - a.win_rate || b.games - a.games);

  const agentImages = {};
  for (const p of players) {
    if (!(isAll || p.region === region)) continue;
    if (p.agent_image_url && !agentImages[p.character]) agentImages[p.character] = p.agent_image_url;
  }
  for (const r of seedRows(region)) {
    if (r.agent_image_url && !agentImages[r.agent]) agentImages[r.agent] = r.agent_image_url;
  }
  return { topComps, archetypes, agentImages };
}

function computeStatus(region, players, updatedAt) {
  const isAll = region === "all";
  const rp = players.filter((p) => (isAll || p.region === region) && p.mode_id === "competitive");
  const matchIds = new Set(rp.map((p) => p.match_id));
  const starts = rp.map((p) => p.game_start).filter((n) => n > 0);
  const recent = rp.filter((p) => p.game_start >= 1735689600); // >= 2025-01-01
  const recentIds = new Set(recent.map((p) => p.match_id));
  const totalGames = matchIds.size || Math.round(seedRows(region).reduce((s, r) => s + r.games, 0) / 10);
  const latest = starts.length ? Math.max(...starts) : updatedAt;
  return {
    totalGames,
    dateRange: {
      earliest: starts.length ? Math.min(...starts) : null,
      latest: latest || null,
      recentEarliest: recent.length ? Math.min(...recent.map((p) => p.game_start)) : null,
      recentGames: recentIds.size || totalGames,
    },
    latestRefresh: { id: 0, region, started_at: NOW, completed_at: NOW, players_fetched: 0, matches_processed: totalGames, status: "done" },
    nextRefreshAt: null,
    currentPatch: "current",
  };
}

function buildBundle(region, acc) {
  const updatedAt = NOW;
  const lb = acc.leaderboards[region];
  return {
    region, updatedAt,
    agentMapStats: {
      all: mergeAgentMapStats(region, RANK_BANDS.all, acc.comps, acc.players),
      radiant: mergeAgentMapStats(region, RANK_BANDS.radiant, acc.comps, acc.players),
    },
    abilityStats: {
      all: mergeAbilityStats(region, RANK_BANDS.all, acc.players),
      radiant: mergeAbilityStats(region, RANK_BANDS.radiant, acc.players),
    },
    leaderboard: region === "all"
      ? { updatedAt: null, players: [] }
      : { updatedAt: lb?.updatedAt ?? null, players: lb?.players ?? [] },
    teamComps: computeTeamComps(region, acc.comps, acc.players),
    status: computeStatus(region, acc.players, updatedAt),
  };
}

function writeBundles(acc) {
  mkdirSync(OUT_DIR, { recursive: true });
  for (const region of OUT_REGIONS) {
    const bundle = buildBundle(region, acc);
    writeFileSync(resolve(OUT_DIR, `${region}.json`), JSON.stringify(bundle));
    log(`  ${region.padEnd(6)} -> ${bundle.agentMapStats.all.length} map rows, ` +
      `${bundle.abilityStats.all.length} abilities, ${bundle.teamComps.topComps.length} comps, ` +
      `~${bundle.status.totalGames} games`);
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  log("=".repeat(60));
  log("Valorant Data Refresh (database-free)");
  log("=".repeat(60));
  log(`Regions: ${REGIONS.join(", ")} | COMPUTE_ONLY=${COMPUTE_ONLY}`);

  const acc = downloadAccumulator();
  pruneAccumulator(acc);

  if (!COMPUTE_ONLY) {
    const start = Date.now();
    let total = 0;
    for (const region of REGIONS) {
      total += await fetchRegion(region, acc);
      // Persist after each region so a timeout mid-run doesn't lose progress —
      // the next run resumes from the last saved accumulator.
      saveAccumulator(acc);
      uploadAccumulator();
    }
    log(`\nFetched ${total} new matches in ${((Date.now() - start) / 60000).toFixed(1)} min`);
  }

  if (!FETCH_ONLY) {
    log("\nComputing static bundles...");
    writeBundles(acc);
  }

  log("\nDone!");
}

main().catch((err) => {
  console.error(`FATAL ERROR: ${err.message}`);
  console.error(err);
  process.exit(1);
});

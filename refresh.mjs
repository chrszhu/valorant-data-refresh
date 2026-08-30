#!/usr/bin/env node
/**
 * Valorant data refresh script.
 *
 * Fetches Immortal+ match data from the Henrik API and stores it in Postgres.
 * Designed to run as a GitHub Actions cron in a public repo (unlimited minutes).
 *
 * Rate limit: 30 requests per 60 seconds
 * Strategy: 2.5s between requests = 24 req/min (safe margin)
 */

import { Pool, types } from "pg";

types.setTypeParser(20, (val) => parseInt(val, 10));
types.setTypeParser(1700, (val) => parseFloat(val));

const ALL_REGIONS = ["na", "eu", "ap", "kr", "br", "latam"];
const REGIONS = process.env.REGION ? [process.env.REGION] : ALL_REGIONS;
const HENRIK_BASE = "https://api.henrikdev.xyz";
const DELAY_MS = 2500;
// No player cap — scan ALL Immortal+ players from the leaderboard
const MAX_PLAYERS_PER_REGION = parseInt(process.env.MAX_PLAYERS || "0", 10);

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function getHeaders() {
  const key = process.env.HENRIKDEV_API_KEY ?? "";
  const headers = { "Content-Type": "application/json" };
  if (key && key !== "your_api_key_here") headers["Authorization"] = key;
  return headers;
}

async function safeFetch(url, maxAttempts = 5) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    await sleep(DELAY_MS);
    try {
      const res = await fetch(url, {
        headers: getHeaders(),
        cache: "no-store",
      });
      if (res.status === 429) {
        const resetSec = parseInt(
          res.headers.get("x-ratelimit-reset") || "60",
          10,
        );
        log(
          `  Rate limited (attempt ${attempt}/${maxAttempts}). Waiting ${resetSec + 5}s...`,
        );
        await sleep((resetSec + 5) * 1000);
        continue;
      }
      return res;
    } catch (err) {
      log(`  Fetch error (attempt ${attempt}/${maxAttempts}): ${err.message}`);
      if (attempt === maxAttempts) return null;
      await sleep(5000);
    }
  }
  return null;
}

function extractPatch(gameVersion) {
  const m = gameVersion?.match(/release-0*(\d+\.\d+)/i);
  return m ? m[1] : "unknown";
}

const AGENT_ROLES = {
  Brimstone: "Controller",
  Viper: "Controller",
  Omen: "Controller",
  Astra: "Controller",
  Harbor: "Controller",
  Clove: "Controller",
  Tejo: "Controller",
  Vyse: "Sentinel",
  Jett: "Duelist",
  Phoenix: "Duelist",
  Reyna: "Duelist",
  Raze: "Duelist",
  Yoru: "Duelist",
  Neon: "Duelist",
  Iso: "Duelist",
  Waylay: "Duelist",
  Sova: "Initiator",
  Breach: "Initiator",
  Skye: "Initiator",
  "KAY/O": "Initiator",
  Fade: "Initiator",
  Gekko: "Initiator",
  Sage: "Sentinel",
  Cypher: "Sentinel",
  Killjoy: "Sentinel",
  Chamber: "Sentinel",
  Deadlock: "Sentinel",
};

function computeArchetype(agents) {
  const counts = { Controller: 0, Duelist: 0, Initiator: 0, Sentinel: 0 };
  for (const a of agents) counts[AGENT_ROLES[a] ?? "Duelist"]++;
  return ["Controller", "Duelist", "Initiator", "Sentinel"]
    .filter((r) => counts[r] > 0)
    .map((r) => `${counts[r]}${r[0]}`)
    .join("+");
}

// === DATABASE ===
let pool = null;

function getPool() {
  if (pool) return pool;
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) throw new Error("DATABASE_URL not set");
  const cleanUrl = connectionString
    .replace(/[&?](sslmode|channel_binding)=[^&]*/g, "")
    .replace(/\?$/, "");
  pool = new Pool({
    connectionString: cleanUrl,
    ssl: { rejectUnauthorized: true },
    max: 5,
  });
  return pool;
}

async function query(sql, params = []) {
  return (await getPool().query(sql, params)).rows;
}

async function execute(sql, params = []) {
  await getPool().query(sql, params);
}

async function initSchema() {
  await execute(`
    CREATE TABLE IF NOT EXISTS cached_leaderboards (
      region TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at INTEGER NOT NULL DEFAULT floor(extract(epoch from now()))::integer
    )
  `);
  // Per-player competitive tier + team rank anchor (24=Imm1 … 27=Radiant, 0=unknown)
  await execute(`ALTER TABLE radiant_match_players ADD COLUMN IF NOT EXISTS current_tier INTEGER DEFAULT 0`).catch(() => {});
  await execute(`ALTER TABLE team_compositions ADD COLUMN IF NOT EXISTS anchor_tier INTEGER DEFAULT 0`).catch(() => {});
  await execute(`ALTER TABLE team_compositions ADD COLUMN IF NOT EXISTS game_start INTEGER DEFAULT 0`).catch(() => {});
}

async function setCachedLeaderboard(region, players) {
  const now = Math.floor(Date.now() / 1000);
  await execute(
    `INSERT INTO cached_leaderboards (region, data, updated_at)
     VALUES ($1, $2, $3)
     ON CONFLICT (region) DO UPDATE SET data = $2, updated_at = $3`,
    [region, JSON.stringify(players), now],
  );
}

async function rebuildStats(region) {
  log(`  Rebuilding stats for ${region}...`);

  const comps = await query(
    "SELECT map, agents_sorted, won FROM team_compositions WHERE region = $1",
    [region],
  );
  const acc = {};
  for (const row of comps) {
    const agents = row.agents_sorted.split(",").filter(Boolean);
    if (!acc[row.map]) acc[row.map] = {};
    for (const agent of agents) {
      if (!acc[row.map][agent])
        acc[row.map][agent] = { games: 0, wins: 0 };
      acc[row.map][agent].games++;
      if (parseInt(String(row.won), 10)) acc[row.map][agent].wins++;
    }
  }

  const perfRows = await query(
    `SELECT map, character, MAX(agent_image_url) as img,
      SUM(total_rounds) as rds, SUM(damage_made) as dmg, SUM(score) as sc,
      SUM(headshots) as hs, SUM(bodyshots) as bs, SUM(legshots) as ls,
      SUM(c_cast) as cc, SUM(q_cast) as qc, SUM(e_cast) as ec, SUM(x_cast) as xc,
      SUM(econ_spent_avg) as esp, SUM(econ_loadout_avg) as elv
    FROM radiant_match_players WHERE region = $1 AND mode_id = 'competitive'
    GROUP BY map, character`,
    [region],
  );

  const perf = {};
  const imageMap = {};
  for (const r of perfRows) {
    perf[`${r.map}::${r.character}`] = r;
    if (r.img && !imageMap[r.character]) imageMap[r.character] = r.img;
  }

  // For maps with player data but no team_compositions, derive from player rows
  const playerGameRows = await query(
    `SELECT map, character, COUNT(*) as games, SUM(won) as wins
     FROM radiant_match_players WHERE region = $1 AND mode_id = 'competitive'
     GROUP BY map, character`,
    [region],
  );

  for (const r of playerGameRows) {
    if (acc[r.map]?.[r.character]) continue;
    if (!acc[r.map]) acc[r.map] = {};
    acc[r.map][r.character] = {
      games: parseInt(String(r.games), 10),
      wins: parseInt(String(r.wins), 10),
    };
  }

  await execute("DELETE FROM agent_map_stats WHERE region = $1", [region]);
  const now = Math.floor(Date.now() / 1000);

  for (const [map, agents] of Object.entries(acc)) {
    for (const [agent, { games, wins }] of Object.entries(agents)) {
      const p = perf[`${map}::${agent}`];
      await execute(
        `INSERT INTO agent_map_stats
          (region, map, agent, agent_image_url, games, wins,
           total_rounds, total_damage, total_score,
           total_hs, total_bs, total_ls,
           total_c_casts, total_q_casts, total_e_casts, total_x_casts,
           total_econ_spent, total_loadout_value, updated_at, patch)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
        ON CONFLICT (region, map, agent) DO NOTHING`,
        [
          region, map, agent, imageMap[agent] ?? "",
          games, wins,
          parseInt(p?.rds ?? "0", 10), parseInt(p?.dmg ?? "0", 10),
          parseInt(p?.sc ?? "0", 10),
          parseInt(p?.hs ?? "0", 10), parseInt(p?.bs ?? "0", 10),
          parseInt(p?.ls ?? "0", 10),
          parseFloat(p?.cc ?? "0"), parseFloat(p?.qc ?? "0"),
          parseFloat(p?.ec ?? "0"), parseFloat(p?.xc ?? "0"),
          parseFloat(p?.esp ?? "0"), parseFloat(p?.elv ?? "0"),
          now, "current",
        ],
      );
    }
  }

  log(`  Stats rebuilt: ${Object.keys(acc).length} maps`);
}

// === MAIN ===
async function main() {
  log("=".repeat(60));
  log("Valorant Data Refresh");
  log("=".repeat(60));
  log(`Regions: ${REGIONS.join(", ")}`);
  log(`Max players per region: ${MAX_PLAYERS_PER_REGION}`);
  log(`Delay between requests: ${DELAY_MS}ms`);
  log("");

  await initSchema();

  const startTime = Date.now();
  let totalMatches = 0;

  for (const region of REGIONS) {
    log(`\n--- ${region.toUpperCase()} ---`);

    const lbRes = await safeFetch(
      `${HENRIK_BASE}/valorant/v3/leaderboard/${region}/pc`,
    );
    if (!lbRes || !lbRes.ok) {
      log(`  Failed to fetch leaderboard`);
      continue;
    }

    const lbData = await lbRes.json();
    const allPlayers = lbData.data?.players ?? [];

    const cachedPlayers = allPlayers.map((p, idx) => ({
      rank: idx + 1,
      name: p.name ?? "",
      tag: p.tag ?? "",
      tier: p.tier,
      rr: p.rankedRating ?? 0,
      wins: p.numberOfWins ?? 0,
      is_anonymized: p.is_anonymized,
    }));
    await setCachedLeaderboard(region, cachedPlayers);
    log(`  Cached ${cachedPlayers.length} leaderboard entries`);

    // tier >= 24 = Immortal 1 and above (Imm1=24, Imm2=25, Imm3=26, Radiant=27)
    let players = allPlayers
      .filter((p) => !p.is_anonymized && p.name && p.tag && p.tier >= 24);
    if (MAX_PLAYERS_PER_REGION > 0) players = players.slice(0, MAX_PLAYERS_PER_REGION);

    log(`  Checking ${players.length} Immortal+ players`);

    let regionMatches = 0;
    let playersChecked = 0;

    for (const player of players) {
      try {
        const matchRes = await safeFetch(
          `${HENRIK_BASE}/valorant/v3/matches/${region}/${encodeURIComponent(player.name)}/${encodeURIComponent(player.tag)}?filter=competitive&size=10`,
        );

        if (!matchRes || !matchRes.ok) {
          playersChecked++;
          continue;
        }

        const matchData = await matchRes.json();
        const matches = matchData.data ?? [];

        for (const match of matches) {
          try {
            if (
              !match?.metadata ||
              match.metadata.mode_id !== "competitive"
            )
              continue;

            const mapName = match.metadata.map;
            const patch = extractPatch(match.metadata.game_version ?? "");

            const actualPlayer = match.players.all_players.find(
              (p) =>
                p.name?.toLowerCase() === player.name.toLowerCase() &&
                p.tag?.toLowerCase() === player.tag.toLowerCase(),
            );
            if (!actualPlayer) continue;

            const team = actualPlayer.team?.toLowerCase();
            const teamResults = match.teams ?? {};
            if (!teamResults[team]) continue;

            const existing = await query(
              "SELECT 1 FROM radiant_match_players WHERE match_id = $1 AND puuid = $2",
              [match.metadata.matchid, actualPlayer.puuid],
            );
            if (existing.length > 0) continue;

            let won = false;
            let teamRoundsWon = 0;
            let teamRoundsLost = 0;
            let rounds = match.metadata.rounds_played || 1;

            if (
              typeof teamResults.red === "number" &&
              typeof teamResults.blue === "number"
            ) {
              const myRounds = teamResults[team] ?? 0;
              const oppTeam = team === "red" ? "blue" : "red";
              const oppRounds = teamResults[oppTeam] ?? 0;
              won = myRounds > oppRounds;
              teamRoundsWon = myRounds;
              teamRoundsLost = oppRounds;
              rounds = myRounds + oppRounds || rounds;
            } else if (teamResults.red && teamResults.blue) {
              const myTeam = teamResults[team];
              if (myTeam) {
                won = myTeam.has_won ?? false;
                teamRoundsWon = myTeam.rounds_won ?? 0;
                teamRoundsLost = myTeam.rounds_lost ?? 0;
                rounds = teamRoundsWon + teamRoundsLost || rounds;
              }
            }
            const imageUrl = actualPlayer.assets?.agent?.small ?? "";

            // Rank capture: tracked player's tier at match time (fallback to leaderboard tier);
            // anchor = highest-ranked player in the lobby (proxy for the match's rank level).
            const trackedTier = actualPlayer.currenttier ?? player.tier ?? 0;
            const rosterTiers = (match.players?.all_players ?? [])
              .map((p) => p.currenttier ?? 0).filter((n) => n > 0);
            const anchorTier = rosterTiers.length ? Math.max(...rosterTiers) : (player.tier ?? 0);

            await execute(
              `INSERT INTO radiant_match_players
                (region, match_id, map, mode_id, game_start, total_rounds,
                 puuid, player_name, player_tag, team, won, character, agent_image_url,
                 score, kills, deaths, assists, headshots, bodyshots, legshots, damage_made,
                 c_cast, q_cast, e_cast, x_cast, econ_spent_avg, econ_loadout_avg,
                 team_rounds_won, team_rounds_lost, patch, current_tier)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31)
              ON CONFLICT (match_id, puuid) DO NOTHING`,
              [
                region, match.metadata.matchid, mapName,
                match.metadata.mode_id, match.metadata.game_start, rounds,
                actualPlayer.puuid, player.name, player.tag,
                team, won ? 1 : 0, actualPlayer.character, imageUrl,
                actualPlayer.stats.score, actualPlayer.stats.kills,
                actualPlayer.stats.deaths, actualPlayer.stats.assists,
                actualPlayer.stats.headshots, actualPlayer.stats.bodyshots,
                actualPlayer.stats.legshots, actualPlayer.damage_made ?? 0,
                actualPlayer.ability_casts?.c_cast ?? 0,
                actualPlayer.ability_casts?.q_cast ?? 0,
                actualPlayer.ability_casts?.e_cast ?? 0,
                actualPlayer.ability_casts?.x_cast ?? 0,
                actualPlayer.economy?.spent?.average ?? 0,
                actualPlayer.economy?.loadout_value?.average ?? 0,
                teamRoundsWon, teamRoundsLost, patch, trackedTier,
              ],
            );

            // Insert team compositions for both sides
            if (
              teamResults.red &&
              teamResults.blue &&
              match.players?.all_players
            ) {
              for (const side of ["red", "blue"]) {
                const sideAgents = match.players.all_players
                  .filter((p) => p.team?.toLowerCase() === side)
                  .map((p) => p.character)
                  .filter(Boolean)
                  .sort();
                if (sideAgents.length !== 5) continue;

                let sideWon = false;
                if (typeof teamResults[side] === "number") {
                  const oppSide = side === "red" ? "blue" : "red";
                  sideWon =
                    teamResults[side] > (teamResults[oppSide] ?? 0);
                } else {
                  sideWon = teamResults[side]?.has_won ?? false;
                }

                await execute(
                  `INSERT INTO team_compositions (region, match_id, team, map, agents_sorted, archetype, won, patch, anchor_tier, game_start)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                   ON CONFLICT (match_id, team) DO NOTHING`,
                  [
                    region, match.metadata.matchid, side, mapName,
                    sideAgents.join(","), computeArchetype(sideAgents),
                    sideWon ? 1 : 0, patch, anchorTier, match.metadata.game_start ?? 0,
                  ],
                );
              }
            }

            regionMatches++;
          } catch (matchErr) {
            log(
              `    ⚠ Error processing match: ${matchErr.message}. Skipping.`,
            );
            continue;
          }
        }
      } catch (playerErr) {
        log(
          `    ⚠ Error fetching player ${player.name}#${player.tag}: ${playerErr.message}. Skipping.`,
        );
        continue;
      }

      playersChecked++;
      if (playersChecked % 25 === 0) {
        log(
          `  Progress: ${playersChecked}/${players.length} players, ${regionMatches} new matches`,
        );
      }
    }

    log(
      `  ✓ ${region.toUpperCase()}: ${playersChecked} players, ${regionMatches} new matches`,
    );
    await rebuildStats(region);
    totalMatches += regionMatches;
  }

  const elapsed = (Date.now() - startTime) / 1000 / 60;

  log("");
  log("=".repeat(60));
  log("REFRESH COMPLETE");
  log("=".repeat(60));
  log(`Total time: ${elapsed.toFixed(1)} minutes`);
  log(`Total new matches: ${totalMatches}`);

  const mapCounts = await query(`
    SELECT map, COUNT(DISTINCT match_id) as matches
    FROM radiant_match_players
    GROUP BY map
    ORDER BY matches DESC
  `);
  log("");
  log("Database totals by map:");
  for (const row of mapCounts) {
    log(`  ${row.map}: ${row.matches}`);
  }

  await getPool().end();
  log("");
  log("Done!");
}

main().catch((err) => {
  console.error(`FATAL ERROR: ${err.message}`);
  console.error(err);
  process.exit(1);
});

#!/usr/bin/env python3
"""
Fresh Wallet Launch Detector
============================
Listens to new token launches on Solana (via PumpPortal's free WebSocket),
checks whether the creator wallet is "fresh" (no meaningful on-chain history
before this launch, and no prior pump.fun interaction), and pushes an alert
to Telegram with the token name + contract address.

Pipeline:

    PumpPortal WS (subscribeNewToken)
            |
            v
      cheap pre-filters          (dedupe mint, known creator, pool, min dev buy)
            |
            v
      freshness check (RPC)      1) getSignaturesForAddress(creator, before=create_tx)
                                 2) if few prior txs -> did any touch pump.fun?
            |
            v
      Telegram alert (queued + rate limited)

Everything is configured through environment variables / a .env file.
See .env.example and README.md.
"""

import asyncio
import html
import json
import logging
import os
import re
import sqlite3
import sys
import time

import aiohttp
import websockets

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional; plain environment variables work too

log = logging.getLogger("freshbot")

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
# pump.fun mayhem mode routes vaults/fees through this program (set at
# creation, immutable) - its presence in the create tx marks a mayhem launch
MAYHEM_PROGRAM = "MAyhSmzXzV1pTf7LsNkrNwkWKTo4ougAJ1PPg47MD4e"
PUMP_VIRTUAL_SOL_RESERVE = 30.0  # bonding curve starts with 30 virtual SOL


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

def _env_str(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


class Config:
    def __init__(self):
        self.telegram_token = _env_str("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = _env_str("TELEGRAM_CHAT_ID")
        self.rpc_url = _env_str("RPC_URL", "https://api.mainnet-beta.solana.com")
        self.ws_url = _env_str("PUMPPORTAL_WS", "wss://pumpportal.fun/api/data")
        self.db_path = _env_str("DB_PATH", "fresh_bot.db")

        # A wallet is "fresh" if it has at most this many transactions BEFORE
        # the create tx. Brand-new wallets usually have 1-3 (funding, ATA setup).
        self.fresh_max_prior_txs = _env_int("FRESH_MAX_PRIOR_TXS", 5)

        # If a candidate wallet has 1..N prior txs, fetch them and reject the
        # wallet if any touched the pump.fun program (bought/launched before).
        self.check_pumpfun_history = _env_bool("CHECK_PUMPFUN_HISTORY", True)

        # Funding-provenance controls: the funder is whoever sent the fresh
        # wallet its SOL. If one funder has bankrolled this many creator
        # wallets, treat it as a launch factory.
        self.funder_factory_threshold = _env_int("FUNDER_FACTORY_THRESHOLD", 2)
        self.skip_factory_funded = _env_bool("SKIP_FACTORY_FUNDED", True)

        # Suppress a launch if its (normalized) name already launched this
        # many times in the last 24h. 0 disables the skip (flag only).
        self.name_repeat_skip = _env_int("NAME_REPEAT_SKIP", 3)

        # Skip launches created in pump.fun mayhem mode (AI-agent-traded
        # first 24h, 2B supply) - detected from the create transaction.
        self.skip_mayhem_mode = _env_bool("SKIP_MAYHEM_MODE", True)

        # Noise controls
        self.min_dev_buy_sol = _env_float("MIN_DEV_BUY_SOL", 0.0)
        self.pools = {
            p.strip().lower()
            for p in _env_str("POOLS", "pump").split(",")
            if p.strip()
        }

        # Plumbing
        self.rpc_concurrency = _env_int("RPC_CONCURRENCY", 4)
        self.workers = _env_int("WORKERS", 4)
        self.alert_min_interval = _env_float("ALERT_MIN_INTERVAL", 3.0)
        self.alert_queue_size = _env_int("ALERT_QUEUE_SIZE", 25)
        self.event_queue_size = _env_int("EVENT_QUEUE_SIZE", 300)
        # Reconnect if the feed sends nothing for this many seconds
        # (pump.fun launches nonstop, so silence means a dead connection).
        self.ws_silence_timeout = _env_int("WS_SILENCE_TIMEOUT", 120)
        # Log a liveness line this often so "is it running?" is answerable.
        self.heartbeat_secs = _env_int("HEARTBEAT_SECS", 180)
        self.log_level = _env_str("LOG_LEVEL", "INFO").upper()

    def validate(self):
        missing = []
        if not self.telegram_token:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not self.telegram_chat_id:
            missing.append("TELEGRAM_CHAT_ID")
        if missing:
            sys.exit(
                "Missing required environment variables: "
                + ", ".join(missing)
                + "\nCopy .env.example to .env and fill them in."
            )


# --------------------------------------------------------------------------
# Persistence (SQLite): remembers creators + alerted mints across restarts
# --------------------------------------------------------------------------

class Store:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS creators ("
            " address TEXT PRIMARY KEY, first_mint TEXT, ts INTEGER)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS alerted ("
            " mint TEXT PRIMARY KEY, ts INTEGER)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS funders ("
            " funder TEXT, creator TEXT, ts INTEGER,"
            " PRIMARY KEY (funder, creator))"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS names ("
            " name TEXT, mint TEXT, ts INTEGER)"
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_names ON names (name)")
        self.db.commit()

    def creator_seen(self, address: str) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM creators WHERE address = ?", (address,)
        ).fetchone()
        return row is not None

    def remember_creator(self, address: str, mint: str):
        self.db.execute(
            "INSERT OR IGNORE INTO creators (address, first_mint, ts) VALUES (?, ?, ?)",
            (address, mint, int(time.time())),
        )
        self.db.commit()

    def mint_alerted(self, mint: str) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM alerted WHERE mint = ?", (mint,)
        ).fetchone()
        return row is not None

    def remember_mint(self, mint: str):
        self.db.execute(
            "INSERT OR IGNORE INTO alerted (mint, ts) VALUES (?, ?)",
            (mint, int(time.time())),
        )
        self.db.commit()

    def record_funder(self, funder: str, creator: str) -> int:
        """Remember that `funder` bankrolled `creator`. Returns how many
        distinct creator wallets this funder has bankrolled in total -
        this is the launch-factory detector."""
        self.db.execute(
            "INSERT OR IGNORE INTO funders (funder, creator, ts) VALUES (?, ?, ?)",
            (funder, creator, int(time.time())),
        )
        self.db.commit()
        row = self.db.execute(
            "SELECT COUNT(DISTINCT creator) FROM funders WHERE funder = ?",
            (funder,),
        ).fetchone()
        return int(row[0]) if row else 1

    def record_name(self, name_norm: str, mint: str):
        """Log a launch name (normalized) and prune entries older than 24h."""
        now = int(time.time())
        self.db.execute("DELETE FROM names WHERE ts < ?", (now - 86400,))
        self.db.execute(
            "INSERT INTO names (name, mint, ts) VALUES (?, ?, ?)",
            (name_norm, mint, now),
        )
        self.db.commit()

    def name_repeats(self, name_norm: str, mint: str) -> int:
        """How many OTHER launches used this name in the last 24h."""
        row = self.db.execute(
            "SELECT COUNT(*) FROM names WHERE name = ? AND mint != ?",
            (name_norm, mint),
        ).fetchone()
        return int(row[0]) if row else 0


# --------------------------------------------------------------------------
# Solana RPC client (standard JSON-RPC, works with any provider)
# --------------------------------------------------------------------------

class Rpc:
    def __init__(self, session: aiohttp.ClientSession, url: str, concurrency: int):
        self.session = session
        self.url = url
        self.sem = asyncio.Semaphore(max(1, concurrency))

    async def call(self, method: str, params: list):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        for attempt in range(3):
            try:
                async with self.sem:
                    async with self.session.post(
                        self.url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=12),
                    ) as resp:
                        if resp.status == 429:
                            wait = 2.0 * (attempt + 1)
                            log.debug("RPC 429, backing off %.1fs", wait)
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                        data = await resp.json(content_type=None)
                if isinstance(data, dict) and "error" in data:
                    log.debug("RPC error for %s: %s", method, data["error"])
                    return None
                return data.get("result") if isinstance(data, dict) else None
            except Exception as exc:
                if attempt == 2:
                    log.warning("RPC %s failed after retries: %s", method, exc)
                    return None
                await asyncio.sleep(1.5 * (attempt + 1))
        return None

    async def recent_signatures(self, address: str, limit: int):
        """Most recent transaction entries for `address` (newest first).
        Each entry is a dict with at least `signature`, usually also
        `blockTime`. Returns None on RPC failure."""
        params = [address, {"limit": limit, "commitment": "confirmed"}]
        result = await self.call("getSignaturesForAddress", params)
        if result is None:
            return None
        return [
            item
            for item in result
            if isinstance(item, dict) and item.get("signature")
        ]

    async def get_transaction(self, signature: str):
        """Fetch a confirmed transaction, or None on failure."""
        return await self.call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "json",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        )


def tx_account_keys(tx: dict) -> list:
    """All account keys of a fetched tx (static + address-table loaded),
    index-aligned with meta.preBalances / meta.postBalances."""
    keys = []
    message = (tx.get("transaction") or {}).get("message") or {}
    for key in message.get("accountKeys") or []:
        keys.append(key.get("pubkey") if isinstance(key, dict) else key)
    loaded = (tx.get("meta") or {}).get("loadedAddresses") or {}
    keys.extend(loaded.get("writable") or [])
    keys.extend(loaded.get("readonly") or [])
    return keys


def tx_includes_program(tx: dict, program_id: str) -> bool:
    return program_id in tx_account_keys(tx)


def parse_funding(tx: dict, creator: str):
    """If this tx credited SOL to `creator`, return (funder, sol_amount).
    The funder is the account whose balance dropped the most - robust for
    plain transfers, CEX batch payouts, and program-routed sends alike."""
    if not isinstance(tx, dict):
        return None
    keys = tx_account_keys(tx)
    meta = tx.get("meta") or {}
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    if len(pre) != len(keys) or len(post) != len(keys):
        return None
    try:
        creator_index = keys.index(creator)
    except ValueError:
        return None
    credited = post[creator_index] - pre[creator_index]
    if credited <= 0:
        return None
    funder, biggest_drop = None, 0
    for i, key in enumerate(keys):
        if key == creator:
            continue
        drop = pre[i] - post[i]
        if drop > biggest_drop:
            biggest_drop, funder = drop, key
    if funder is None:
        return None
    return funder, credited / 1e9


async def profile_wallet(rpc: "Rpc", address: str, limit: int = 50):
    """Cheap activity profile of a wallet: tx count (capped at `limit`),
    whether the page was full (high-activity), and age when knowable."""
    entries = await rpc.recent_signatures(address, limit)
    if entries is None:
        return None
    count = len(entries)
    page_full = count >= limit
    age = None
    if not page_full and entries:
        block_time = entries[-1].get("blockTime")
        if isinstance(block_time, (int, float)):
            age = max(0.0, time.time() - float(block_time))
    return {"count": count, "page_full": page_full, "age": age}


def normalize_name(name) -> str:
    """Lowercase and strip non-alphanumerics so 'Dog Wif Cap' == 'dogwifcap'."""
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


# --------------------------------------------------------------------------
# Telegram sender (queued, rate limited, honours 429 retry_after)
# --------------------------------------------------------------------------

class Telegram:
    def __init__(self, session, token, chat_id, min_interval, queue_size):
        self.session = session
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.min_interval = max(0.5, min_interval)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)

    def enqueue(self, text: str, buttons=None):
        """Queue a message. `buttons` is an optional list of rows, each row
        a list of (label, url) tuples shown as tappable inline buttons."""
        try:
            self.queue.put_nowait((text, buttons))
        except asyncio.QueueFull:
            log.warning("Alert queue full - dropping an alert. "
                        "Tighten filters (MIN_DEV_BUY_SOL / FRESH_MAX_PRIOR_TXS).")

    async def run(self):
        while True:
            text, buttons = await self.queue.get()
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if buttons:
                payload["reply_markup"] = {
                    "inline_keyboard": [
                        [{"text": label, "url": url} for label, url in row]
                        for row in buttons
                    ]
                }
            for attempt in range(3):
                try:
                    async with self.session.post(
                        self.url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        body = await resp.json(content_type=None)
                        if resp.status == 429:
                            retry_after = 5
                            if isinstance(body, dict):
                                retry_after = (body.get("parameters") or {}).get(
                                    "retry_after", 5
                                )
                            log.info("Telegram rate limit, sleeping %ss", retry_after)
                            await asyncio.sleep(float(retry_after) + 1.0)
                            continue
                        if resp.status == 200 and body.get("ok"):
                            break
                        log.error("Telegram API error %s: %s", resp.status, body)
                        break
                except Exception as exc:
                    if attempt == 2:
                        log.error("Telegram send failed: %s", exc)
                    else:
                        await asyncio.sleep(2.0)
            await asyncio.sleep(self.min_interval)


# --------------------------------------------------------------------------
# Event handling
# --------------------------------------------------------------------------

def dev_buy_sol(event: dict):
    """Best-effort dev buy size in SOL from a PumpPortal create event."""
    value = event.get("solAmount")
    if isinstance(value, (int, float)):
        return float(value)
    # Fallback for pump.fun pool: curve starts with 30 virtual SOL, so
    # anything above that right after create is the dev's initial buy.
    if (event.get("pool") or "pump").lower() == "pump":
        vsol = event.get("vSolInBondingCurve")
        if isinstance(vsol, (int, float)) and vsol >= PUMP_VIRTUAL_SOL_RESERVE:
            return max(0.0, float(vsol) - PUMP_VIRTUAL_SOL_RESERVE)
    return None


def short_addr(address: str) -> str:
    """Shortened address for display, e.g. 9acmq8..DqBy."""
    if len(address) <= 12:
        return address
    return f"{address[:6]}..{address[-4:]}"


def humanize_age(seconds: float) -> str:
    """42s / 6m / 3h / 2d style age string."""
    if seconds < 90:
        return f"{int(seconds)}s"
    minutes = seconds / 60
    if minutes < 90:
        return f"{int(minutes)}m"
    hours = minutes / 60
    if hours < 48:
        return f"{int(hours)}h"
    return f"{int(hours / 24)}d"


def funding_lines(funding) -> list:
    """Tree lines for the Funding section of an alert."""
    if not funding:
        return ["\u2514 Funding tx: not found"]
    funder = funding["funder"]
    lines = [
        f"\u251C {funding['sol']:.2f} SOL from "
        f'<a href="https://solscan.io/account/{funder}">'
        f"{html.escape(short_addr(funder))}</a>"
    ]
    if funding.get("factory_flag"):
        lines.append(
            f"\u2514 \U0001F6A9 Funder bankrolled "
            f"{funding['factory_count']} launch wallets"
        )
        return lines
    profile = funding.get("profile")
    if not profile:
        lines.append("\u2514 Funder history: unavailable")
    elif profile["page_full"]:
        lines.append(f"\u2514 Funder: {profile['count']}+ txs (high-activity)")
    else:
        desc = f"{profile['count']} txs"
        if profile["age"] is not None:
            desc += f", {humanize_age(profile['age'])} old"
        if (profile["count"] <= 20 and profile["age"] is not None
                and profile["age"] < 48 * 3600):
            desc += " \U0001F6A9 fresh funder"
        lines.append(f"\u2514 Funder: {desc}")
    return lines


def format_alert(event: dict, prior_count: int, wallet_age,
                 first_launch_verified: bool, funding=None, name_repeats=0):
    """Build (message_html, inline_buttons) for a fresh-launch alert.

    inline_buttons is a list of rows; each row is a list of (label, url)
    tuples rendered as tappable Telegram buttons under the message.
    """
    name = html.escape(str(event.get("name") or "Unknown"))
    symbol = html.escape(str(event.get("symbol") or "?"))
    mint = str(event.get("mint") or "")
    creator = str(event.get("traderPublicKey") or "")
    signature = str(event.get("signature") or "")
    pool = str(event.get("pool") or "pump").lower()

    deployer_lines = [f"\u251C {prior_count} prior txs"]
    if wallet_age:
        deployer_lines.append(f"\u251C Wallet age: {wallet_age}")
    deployer_lines.append(
        "\u2514 First pump.fun launch \u2705" if first_launch_verified
        else "\u2514 Prior launches: unverified"
    )

    deploy_stats = []
    if name_repeats > 0:
        deploy_stats.append(
            f"\U0001F6A9 Name seen {name_repeats + 1}x in 24h"
        )
    buy = dev_buy_sol(event)
    if buy is not None:
        deploy_stats.append(f"Dev buy: {buy:.2f} SOL")
    market_cap = event.get("marketCapSol")
    if isinstance(market_cap, (int, float)):
        deploy_stats.append(f"MC: {market_cap:.1f} SOL")

    lines = [
        "\U0001F7E2 <b>FRESH WALLET LAUNCH</b>",
        "",
        f"<b>{name}</b> ({symbol}) \u2014 {html.escape(pool)}",
        f"CA: <code>{html.escape(mint)}</code>",
        "",
        f"<b>Deployer</b> | "
        f'<a href="https://solscan.io/account/{creator}">'
        f"{html.escape(short_addr(creator))}</a>",
        *deployer_lines,
    ]
    if deploy_stats:
        lines += ["", "<b>Deploy</b>"]
        lines += [
            ("\u251C " if i < len(deploy_stats) - 1 else "\u2514 ") + stat
            for i, stat in enumerate(deploy_stats)
        ]

    lines += ["", "<b>Funding</b>", *funding_lines(funding)]

    buttons_row = []
    if pool == "pump" and mint:
        buttons_row.append(("pump.fun", f"https://pump.fun/coin/{mint}"))
    if mint:
        buttons_row.append(("GMGN", f"https://gmgn.ai/sol/token/{mint}"))
    if signature:
        buttons_row.append(("Create tx", f"https://solscan.io/tx/{signature}"))

    return "\n".join(lines), ([buttons_row] if buttons_row else None)


async def handle_event(event: dict, store: Store, rpc: Rpc,
                       telegram: Telegram, cfg: Config, stats=None):
    mint = event.get("mint")
    creator = event.get("traderPublicKey")
    signature = event.get("signature")
    if not mint or not creator or not signature:
        return

    pool = str(event.get("pool") or "pump").lower()
    if cfg.pools and pool not in cfg.pools:
        return

    # Free fast-path: if the feed ever labels mayhem launches directly.
    if cfg.skip_mayhem_mode and event.get("mayhemMode"):
        log.info("Skip %s: launched in mayhem mode (feed flag)", mint)
        return

    if store.mint_alerted(mint):
        return

    # Copycat-name tracking: log EVERY launch name so the 24h window sees
    # the whole market, then suppress obvious metaspam waves early (free).
    name_norm = normalize_name(event.get("name"))
    name_hits = 0
    if name_norm:
        store.record_name(name_norm, mint)
        name_hits = store.name_repeats(name_norm, mint)
        if cfg.name_repeat_skip > 0 and name_hits + 1 >= cfg.name_repeat_skip:
            log.info("Skip %s: name '%s' launched %d times in 24h",
                     mint, event.get("name"), name_hits + 1)
            return

    # Serial deployer we've already seen since the bot started running.
    if store.creator_seen(creator):
        log.debug("Skip %s: creator %s already seen", mint, creator[:8])
        return
    store.remember_creator(creator, mint)

    # Cheap filter before spending RPC credits.
    buy = dev_buy_sol(event)
    if cfg.min_dev_buy_sol > 0 and buy is not None and buy < cfg.min_dev_buy_sol:
        log.debug("Skip %s: dev buy %.3f < min %.3f", mint, buy, cfg.min_dev_buy_sol)
        return

    # --- Freshness check -------------------------------------------------
    # Fetch the creator's recent signatures (newest first) and locate the
    # create tx among them; everything AFTER it in the list is a prior tx.
    # We anchor this way instead of using the RPC `before=` parameter,
    # because `before=<sig>` silently returns an EMPTY list when the node
    # hasn't indexed <sig> yet - which looks exactly like a fresh wallet
    # and produces false prior=0 results.
    page_limit = cfg.fresh_max_prior_txs + 5
    entries = None
    create_index = None
    for attempt in range(4):
        entries = await rpc.recent_signatures(creator, page_limit)
        if entries is None:
            log.warning("RPC failed for creator %s, skipping %s", creator[:8], mint)
            return
        sig_list = [entry["signature"] for entry in entries]
        if signature in sig_list:
            create_index = sig_list.index(signature)
            break
        # Node hasn't indexed the create tx yet - give it a moment.
        await asyncio.sleep(1.5 * (attempt + 1))
    if create_index is None:
        log.warning("Create tx for %s not indexed after retries - "
                    "cannot verify freshness, skipping", mint)
        return

    prior_entries = entries[create_index + 1:]
    prior_sigs = [entry["signature"] for entry in prior_entries]
    prior_count = len(prior_sigs)

    # Wallet age = time since the oldest visible tx. Wallets this fresh fit
    # their whole history in one page, so this is the true wallet age.
    oldest = prior_entries[-1] if prior_entries else entries[create_index]
    oldest_time = oldest.get("blockTime")
    wallet_age = None
    if isinstance(oldest_time, (int, float)):
        wallet_age = humanize_age(max(0.0, time.time() - float(oldest_time)))
    if prior_count > cfg.fresh_max_prior_txs:
        log.debug("Skip %s: creator has >%d prior txs", mint, cfg.fresh_max_prior_txs)
        return

    # --- Mayhem-mode filter ----------------------------------------------
    # Mayhem launches route through the Mayhem program; since the mode is
    # fixed at creation, the create tx must list that program's ID.
    if cfg.skip_mayhem_mode:
        create_tx = await rpc.get_transaction(signature)
        if create_tx is None:
            log.debug("Could not fetch create tx for %s (mayhem check skipped)",
                      mint)
        elif tx_includes_program(create_tx, MAYHEM_PROGRAM):
            log.info("Skip %s: launched in mayhem mode", mint)
            return

    prior_txs = {}
    if cfg.check_pumpfun_history and prior_count > 0:
        for sig in prior_sigs:
            tx = await rpc.get_transaction(sig)
            prior_txs[sig] = tx
            if tx is None:
                # couldn't fetch; be permissive but log it.
                log.debug("Could not verify prior tx %s for %s", sig[:16], creator[:8])
                continue
            if tx_includes_program(tx, PUMP_FUN_PROGRAM):
                log.info("Skip %s: creator %s touched pump.fun before",
                         mint, creator[:8])
                return

    # --- Funding provenance ----------------------------------------------
    # Walk the wallet's prior txs oldest-first and find who sent it SOL.
    # Reuses transactions already fetched for the pump.fun history check.
    funding = None
    for entry in reversed(prior_entries):
        sig = entry["signature"]
        tx = prior_txs.get(sig)
        if tx is None:
            tx = await rpc.get_transaction(sig)
        parsed = parse_funding(tx, creator) if tx else None
        if parsed:
            funder, sol_in = parsed
            factory_count = store.record_funder(funder, creator)
            funding = {
                "funder": funder,
                "sol": sol_in,
                "factory_count": factory_count,
                "factory_flag": factory_count >= cfg.funder_factory_threshold,
                "profile": None,
            }
            break

    if funding and funding["factory_flag"] and cfg.skip_factory_funded:
        log.info("Skip %s: funder %s already bankrolled %d launch wallets",
                 mint, short_addr(funding["funder"]), funding["factory_count"])
        return

    if funding and not funding["factory_flag"]:
        funding["profile"] = await profile_wallet(rpc, funding["funder"])

    first_verified = cfg.check_pumpfun_history or prior_count == 0
    log.info("FRESH LAUNCH: %s (%s) mint=%s creator=%s prior=%d age=%s",
             event.get("name"), event.get("symbol"), mint, creator,
             prior_count, wallet_age or "?")
    text, buttons = format_alert(event, prior_count, wallet_age, first_verified,
                                 funding=funding, name_repeats=name_hits)
    telegram.enqueue(text, buttons)
    store.remember_mint(mint)
    if stats is not None:
        stats.alerts += 1


async def worker(events: asyncio.Queue, store, rpc, telegram, cfg, stats=None):
    while True:
        event = await events.get()
        try:
            await handle_event(event, store, rpc, telegram, cfg, stats)
        except Exception:
            log.exception("Unhandled error while processing event")
        finally:
            events.task_done()


# --------------------------------------------------------------------------
# WebSocket loop with reconnect + silence watchdog, and a heartbeat
# --------------------------------------------------------------------------

class Stats:
    """Shared liveness counters for the heartbeat log line."""

    def __init__(self):
        self.connected = False
        self.last_msg = None   # unix time of last websocket message
        self.creates = 0       # create events seen
        self.alerts = 0        # alerts actually sent


async def heartbeat(stats: Stats, cfg: Config):
    """Logs a liveness line so the logs are never ambiguously silent.
    If these lines keep appearing, the bot is alive (just filtering).
    If the logs stop entirely, the host has put the service to sleep."""
    prev_creates = 0
    while True:
        await asyncio.sleep(cfg.heartbeat_secs)
        if stats.last_msg is None:
            last = "never"
        else:
            last = f"{int(time.time() - stats.last_msg)}s ago"
        log.info(
            "Heartbeat: feed=%s | last message %s | creates seen=%d (+%d) | "
            "alerts sent=%d",
            "connected" if stats.connected else "DISCONNECTED",
            last, stats.creates, stats.creates - prev_creates, stats.alerts,
        )
        prev_creates = stats.creates


async def ws_loop(events: asyncio.Queue, cfg: Config, stats: Stats):
    backoff = 5
    while True:
        try:
            async with websockets.connect(
                cfg.ws_url, ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("Connected to %s - watching new token creates", cfg.ws_url)
                backoff = 5
                stats.connected = True
                while True:
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=cfg.ws_silence_timeout
                        )
                    except asyncio.TimeoutError:
                        # pump.fun never goes this quiet; the connection is
                        # dead even though it never errored. Reconnect.
                        log.warning(
                            "No data from feed for %ds - reconnecting (watchdog)",
                            cfg.ws_silence_timeout,
                        )
                        break
                    stats.last_msg = time.time()
                    try:
                        event = json.loads(raw)
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(event, dict):
                        continue
                    if event.get("txType") != "create":
                        continue
                    stats.creates += 1
                    try:
                        events.put_nowait(event)
                    except asyncio.QueueFull:
                        log.warning("Event queue full - dropping a create event")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("WebSocket dropped (%s). Reconnecting in %ds", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        stats.connected = False
        await asyncio.sleep(1)


# --------------------------------------------------------------------------
# Optional keep-alive web server
# (for hosts like Render that require an open HTTP port on web services)
# --------------------------------------------------------------------------

async def start_keepalive_server(port: int):
    from aiohttp import web

    async def ok(_request):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Keep-alive HTTP server listening on port %d", port)


# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------

async def main():
    cfg = Config()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    cfg.validate()

    store = Store(cfg.db_path)
    events: asyncio.Queue = asyncio.Queue(maxsize=cfg.event_queue_size)

    async with aiohttp.ClientSession() as session:
        rpc = Rpc(session, cfg.rpc_url, cfg.rpc_concurrency)
        telegram = Telegram(
            session,
            cfg.telegram_token,
            cfg.telegram_chat_id,
            cfg.alert_min_interval,
            cfg.alert_queue_size,
        )

        # Hosting platforms like Render set PORT and expect something to be
        # listening on it. Harmless everywhere else (Mac, EC2: no PORT set).
        port = os.environ.get("PORT")
        if port:
            try:
                await start_keepalive_server(int(port))
            except Exception:
                log.exception("Keep-alive server failed to start")

        stats = Stats()
        tasks = [
            asyncio.create_task(telegram.run(), name="telegram-sender"),
            asyncio.create_task(heartbeat(stats, cfg), name="heartbeat"),
        ]
        for i in range(max(1, cfg.workers)):
            tasks.append(
                asyncio.create_task(
                    worker(events, store, rpc, telegram, cfg, stats),
                    name=f"worker-{i}",
                )
            )

        telegram.enqueue(
            "\U0001F916 Fresh-launch bot online \u2014 watching for coins "
            "launched by fresh wallets."
        )

        try:
            await ws_loop(events, cfg, stats)
        finally:
            for task in tasks:
                task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
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
        """Most recent signatures for `address` (newest first).
        Returns a list of signature strings, or None on RPC failure."""
        params = [address, {"limit": limit, "commitment": "confirmed"}]
        result = await self.call("getSignaturesForAddress", params)
        if result is None:
            return None
        return [
            item.get("signature")
            for item in result
            if isinstance(item, dict) and item.get("signature")
        ]

    async def tx_touches_program(self, signature: str, program_id: str):
        """True/False if the tx includes `program_id` in its account keys.
        Returns None if the tx could not be fetched."""
        result = await self.call(
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
        if not isinstance(result, dict):
            return None
        keys = []
        message = (result.get("transaction") or {}).get("message") or {}
        for key in message.get("accountKeys") or []:
            keys.append(key.get("pubkey") if isinstance(key, dict) else key)
        loaded = (result.get("meta") or {}).get("loadedAddresses") or {}
        keys.extend(loaded.get("writable") or [])
        keys.extend(loaded.get("readonly") or [])
        return program_id in keys


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

    def enqueue(self, text: str):
        try:
            self.queue.put_nowait(text)
        except asyncio.QueueFull:
            log.warning("Alert queue full - dropping an alert. "
                        "Tighten filters (MIN_DEV_BUY_SOL / FRESH_MAX_PRIOR_TXS).")

    async def run(self):
        while True:
            text = await self.queue.get()
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
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


def format_alert(event: dict, prior_count: int) -> str:
    name = html.escape(str(event.get("name") or "Unknown"))
    symbol = html.escape(str(event.get("symbol") or "?"))
    mint = str(event.get("mint") or "")
    creator = str(event.get("traderPublicKey") or "")
    signature = str(event.get("signature") or "")
    pool = str(event.get("pool") or "pump").lower()

    lines = [
        "\U0001F7E2 <b>Fresh wallet launch</b>",
        f"<b>{name}</b> ({symbol}) \u2014 pool: {html.escape(pool)}",
        f"CA: <code>{html.escape(mint)}</code>",
        f"Creator: <code>{html.escape(creator)}</code> ({prior_count} prior txs)",
    ]

    stats = []
    buy = dev_buy_sol(event)
    if buy is not None:
        stats.append(f"Dev buy: {buy:.2f} SOL")
    market_cap = event.get("marketCapSol")
    if isinstance(market_cap, (int, float)):
        stats.append(f"MC: {market_cap:.1f} SOL")
    if stats:
        lines.append(" | ".join(stats))

    links = []
    if pool == "pump" and mint:
        links.append(f'<a href="https://pump.fun/coin/{mint}">pump.fun</a>')
    if signature:
        links.append(f'<a href="https://solscan.io/tx/{signature}">create tx</a>')
    if mint:
        links.append(f'<a href="https://gmgn.ai/sol/token/{mint}">gmgn</a>')
    if links:
        lines.append(" \u00B7 ".join(links))

    return "\n".join(lines)


async def handle_event(event: dict, store: Store, rpc: Rpc,
                       telegram: Telegram, cfg: Config):
    mint = event.get("mint")
    creator = event.get("traderPublicKey")
    signature = event.get("signature")
    if not mint or not creator or not signature:
        return

    pool = str(event.get("pool") or "pump").lower()
    if cfg.pools and pool not in cfg.pools:
        return

    if store.mint_alerted(mint):
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
    sigs = None
    create_index = None
    for attempt in range(4):
        sigs = await rpc.recent_signatures(creator, page_limit)
        if sigs is None:
            log.warning("RPC failed for creator %s, skipping %s", creator[:8], mint)
            return
        if signature in sigs:
            create_index = sigs.index(signature)
            break
        # Node hasn't indexed the create tx yet - give it a moment.
        await asyncio.sleep(1.5 * (attempt + 1))
    if create_index is None:
        log.warning("Create tx for %s not indexed after retries - "
                    "cannot verify freshness, skipping", mint)
        return

    prior_sigs = sigs[create_index + 1:]
    prior_count = len(prior_sigs)
    if prior_count > cfg.fresh_max_prior_txs:
        log.debug("Skip %s: creator has >%d prior txs", mint, cfg.fresh_max_prior_txs)
        return

    if cfg.check_pumpfun_history and prior_count > 0:
        for sig in prior_sigs:
            touched = await rpc.tx_touches_program(sig, PUMP_FUN_PROGRAM)
            if touched:
                log.info("Skip %s: creator %s touched pump.fun before",
                         mint, creator[:8])
                return
            # touched is None -> couldn't fetch; be permissive but log it.
            if touched is None:
                log.debug("Could not verify prior tx %s for %s", sig[:16], creator[:8])

    log.info("FRESH LAUNCH: %s (%s) mint=%s creator=%s prior=%d",
             event.get("name"), event.get("symbol"), mint, creator, prior_count)
    telegram.enqueue(format_alert(event, prior_count))
    store.remember_mint(mint)


async def worker(events: asyncio.Queue, store, rpc, telegram, cfg):
    while True:
        event = await events.get()
        try:
            await handle_event(event, store, rpc, telegram, cfg)
        except Exception:
            log.exception("Unhandled error while processing event")
        finally:
            events.task_done()


# --------------------------------------------------------------------------
# WebSocket loop with reconnect
# --------------------------------------------------------------------------

async def ws_loop(events: asyncio.Queue, cfg: Config):
    backoff = 5
    while True:
        try:
            async with websockets.connect(
                cfg.ws_url, ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("Connected to %s - watching new token creates", cfg.ws_url)
                backoff = 5
                async for raw in ws:
                    try:
                        event = json.loads(raw)
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(event, dict):
                        continue
                    if event.get("txType") != "create":
                        continue
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

        tasks = [asyncio.create_task(telegram.run(), name="telegram-sender")]
        for i in range(max(1, cfg.workers)):
            tasks.append(
                asyncio.create_task(
                    worker(events, store, rpc, telegram, cfg), name=f"worker-{i}"
                )
            )

        telegram.enqueue(
            "\U0001F916 Fresh-launch bot online \u2014 watching for coins "
            "launched by fresh wallets."
        )

        try:
            await ws_loop(events, cfg)
        finally:
            for task in tasks:
                task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
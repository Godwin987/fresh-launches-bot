# Fresh Wallet Launch Detector

Telegram bot that watches every new token launch on Solana (pump.fun via
PumpPortal's free WebSocket) and alerts you when the coin was launched by a
**fresh wallet** — one with essentially no on-chain history and no prior
pump.fun interaction before the create transaction.

Each alert contains: token name, ticker, **contract address (tap-to-copy)**,
creator wallet, prior tx count, dev buy size, and quick links
(pump.fun / Solscan / gmgn).

## How "fresh" is decided

1. `getSignaturesForAddress(creator, before = create_tx)` — how many
   transactions did this wallet make *before* launching?
2. If that count is `<= FRESH_MAX_PRIOR_TXS` (default 5), the few prior txs
   are fetched and rejected if any of them touched the pump.fun program
   (meaning the wallet bought or launched there before).
3. Creators the bot has already seen launch (stored in SQLite) are skipped
   instantly — the database becomes a serial-deployer blacklist over time.

**Honest caveat:** most pump.fun deployers rotate brand-new wallets on
purpose, so "fresh wallet" alone matches a *lot* of launches. That is why
`MIN_DEV_BUY_SOL` exists — a fresh wallet putting real SOL behind its own
launch is a much more interesting signal than a zero-buy spam deploy. Start
at `0.5` and tune from there.

## Setup

Requires Python 3.10+.

```bash
git clone <your-repo> fresh-launch-bot && cd fresh-launch-bot   # or just copy the files
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill in `.env`:

1. **TELEGRAM_BOT_TOKEN** — talk to [@BotFather](https://t.me/BotFather) on
   Telegram → `/newbot` → copy the token.
2. **TELEGRAM_CHAT_ID** — message [@userinfobot](https://t.me/userinfobot) and
   it replies with your numeric id. (For a group: add your bot to the group,
   send a message, then open
   `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser and read
   `chat.id` — group ids are negative numbers.)
3. **RPC_URL** — get a free API key at [Helius](https://dashboard.helius.dev)
   and paste the RPC URL they give you. The public Solana RPC will
   rate-limit you at pump.fun launch volume.
4. Send your bot `/start` in Telegram once (bots can't message you first).

Run it:

```bash
python bot.py
```

You should immediately get a "bot online" message in Telegram, and fresh
launches will start arriving. `Ctrl+C` to stop.

## Running 24/7 on an EC2 box

Create `/etc/systemd/system/freshbot.service`:

```ini
[Unit]
Description=Fresh wallet launch bot
After=network-online.target
Wants=network-online.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/fresh-launch-bot
ExecStart=/home/ubuntu/fresh-launch-bot/venv/bin/python bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now freshbot
journalctl -u freshbot -f        # tail logs
```

**Disk-safety note:** this bot logs continuously. Cap the journal so it can
never fill the disk:

```bash
sudo sed -i 's/^#\?SystemMaxUse=.*/SystemMaxUse=200M/' /etc/systemd/journald.conf
sudo systemctl restart systemd-journald
```

## Tuning

| Variable | Default | Effect |
|---|---|---|
| `FRESH_MAX_PRIOR_TXS` | 5 | Stricter (e.g. 3) = only truly untouched wallets |
| `MIN_DEV_BUY_SOL` | 0.5 | Raise to cut spam launches dramatically |
| `CHECK_PUMPFUN_HISTORY` | true | Extra RPC calls, but rejects wallets that used pump.fun before |
| `POOLS` | pump | Add other pools PumpPortal streams, e.g. `pump,bonk` |
| `ALERT_MIN_INTERVAL` | 3.0 | Seconds between Telegram messages |

## Ideas for v2

- **Funding-source tracing** — where did the fresh wallet's SOL come from?
  Funded straight from a CEX withdrawal reads very differently from funded
  by another deployer wallet. This is the real edge on top of freshness.
- Pull socials (Twitter/Telegram/website) from the token metadata `uri`.
- Subscribe to the token's first 30s of trades (`subscribeTokenTrade`) and
  attach unique-buyer / buy-ratio stats to the alert.
- Inline buttons (open chart, copy CA, mute creator).

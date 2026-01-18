# Ingestion Scheduling (Daily Backfill + Live Loop)

TrafficPulse ingestion has two complementary modes:

1) **Daily backfill** (yesterday): pulls historical VD observations for yesterday (Asia/Taipei) and appends them.
2) **Live loop**: polls the VDLive snapshot feed periodically and appends only *new* snapshots.

Scripts:
- `scripts/ingest_daily_backfill.py` (runs once)
- `scripts/ingest_live_loop.py` (runs continuously)
- `scripts/ingest_runner.py` (runs both in sequence)

## Recommended Settings (Rate-Limit Friendly)

- Start conservative:
  - `--min-request-interval 1.0` (1 request/second)
  - `--interval-seconds 60` (poll once per minute)
- If you see frequent 429s, increase `--min-request-interval` (slower).

## Run Manually

Backfill yesterday:

```bash
python scripts/ingest_daily_backfill.py --min-request-interval 1.0 --no-cache
```

Run live loop:

```bash
python scripts/ingest_live_loop.py --interval-seconds 60 --min-request-interval 1.0 --no-cache
```

Run both (backfill then live):

```bash
python scripts/ingest_runner.py --interval-seconds 60 --min-request-interval 1.0 --no-cache
```

## systemd (Recommended)

### 1) Live loop service

Create `~/.config/systemd/user/trafficpulse-live.service`:

```ini
[Unit]
Description=TrafficPulse VD live ingestion loop

[Service]
Type=simple
WorkingDirectory=/home/justin/web-projects/traffic-pulse
ExecStart=/home/justin/web-projects/traffic-pulse/.venv/bin/python scripts/ingest_live_loop.py --interval-seconds 60 --min-request-interval 1.0 --no-cache
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
```

Enable + start:

```bash
systemctl --user daemon-reload
systemctl --user enable --now trafficpulse-live.service
```

Logs:

```bash
journalctl --user -u trafficpulse-live.service -f
```

### 2) Daily backfill timer

Create `~/.config/systemd/user/trafficpulse-backfill.service`:

```ini
[Unit]
Description=TrafficPulse daily backfill (yesterday)

[Service]
Type=oneshot
WorkingDirectory=/home/justin/web-projects/traffic-pulse
ExecStart=/home/justin/web-projects/traffic-pulse/.venv/bin/python scripts/ingest_daily_backfill.py --min-request-interval 1.0 --no-cache
```

Create `~/.config/systemd/user/trafficpulse-backfill.timer`:

```ini
[Unit]
Description=TrafficPulse daily backfill timer

[Timer]
OnCalendar=*-*-* 08:15:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:

```bash
systemctl --user daemon-reload
systemctl --user enable --now trafficpulse-backfill.timer
```

## cron (Alternative)

Live loop (not ideal in cron; prefer systemd):

```cron
@reboot cd /home/justin/web-projects/traffic-pulse && .venv/bin/python scripts/ingest_live_loop.py --interval-seconds 60 --min-request-interval 1.0 --no-cache >> outputs/ingest_live.log 2>&1
```

Daily backfill at 08:15 (Asia/Taipei):

```cron
15 8 * * * cd /home/justin/web-projects/traffic-pulse && .venv/bin/python scripts/ingest_daily_backfill.py --min-request-interval 1.0 --no-cache >> outputs/ingest_backfill.log 2>&1
```


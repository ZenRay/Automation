# Compose File Contract

**Base file**: `deploy/compose.yml`
**Overrides**: `deploy/compose.staging.yml`, `deploy/compose.production.yml`

## Service Definitions

### `worker`

| Property | Value |
|----------|-------|
| Image | `ghcr.io/${GHCR_REPO}/automation:${IMAGE_TAG}` |
| Command | `bash /app/workers/cron_task.sh` |
| Restart | `unless-stopped` (base) / `always` (production override) |
| Volumes | `${CONFIG_DIR}/_lark.ini:/app/automation/conf/_lark.ini:ro` |
|          | `${CONFIG_DIR}/_maxcomputer.ini:/app/automation/conf/_maxcomputer.ini:ro` |
| Health Check | `test: ["CMD", "python", "-c", "import automation, workers"]` |
|              | `interval: 30s, timeout: 10s, retries: 3, start_period: 15s` |
| Environment | `TZ=Asia/Shanghai` |
|              | `PYTHONLOGFORMAT=text` |
| Resource Limits (base) | `cpus: 2.0, memory: 4G` |
| Resource Limits (staging) | `cpus: 2.0, memory: 2G` |
| Resource Limits (production) | `cpus: 4.0, memory: 8G` |
| Depends On | — (standalone) |

### `scheduler`

| Property | Value |
|----------|-------|
| Image | Same as `worker` |
| Command | `bash /app/deploy/scripts/scheduler.sh` (shell loop: run cron_task.sh, sleep, repeat) |
| Restart | `always` |
| Volumes | Same config mounts as `worker` |
| Health Check | `test: ["CMD", "pgrep", "-f", "scheduler.sh"]` |
|              | `interval: 60s, timeout: 10s, retries: 3` |
| Depends On | `worker` (service_started) |

**Scheduler implementation**: `python:3.12-slim` does not include cron. The scheduler is a shell loop script (`deploy/scripts/scheduler.sh`) that:
1. Runs `bash /app/workers/cron_task.sh`
2. Sleeps for `SCHEDULE_INTERVAL` seconds (default: 3600 = 1 hour)
3. Repeats

The interval is configurable via `SCHEDULE_INTERVAL` environment variable in compose.

## Volume Contract

| Volume | Host Path | Container Path | Mode |
|--------|-----------|----------------|------|
| Lark config | `${CONFIG_DIR}/_lark.ini` | `/app/automation/conf/_lark.ini` | `ro` |
| MC config | `${CONFIG_DIR}/_maxcomputer.ini` | `/app/automation/conf/_maxcomputer.ini` | `ro` |

**Constraints**:
- `CONFIG_DIR` must not point to source code directories in production
- All mounted files must be readable by UID 10001
- Mount mode is always `ro` (read-only)

## Environment Variables

| Variable | Source | Description |
|----------|--------|-------------|
| `GHCR_REPO` | `.env` or shell | GHCR repository (e.g., `org/automation`) |
| `IMAGE_TAG` | `.env` or shell | Image tag to deploy |
| `CONFIG_DIR` | `.env` or shell | Host path to config files |
| `TZ` | compose.yml | Timezone: `Asia/Shanghai` |
| `PYTHONLOGFORMAT` | compose.yml | Log format: `text` (reserved for future `json` activation) |

## Override File Contract

Override files (`compose.staging.yml`, `compose.production.yml`) MUST only modify:
- `deploy.resources.limits` (CPU, memory)
- `deploy.restart_policy`
- Environment variables (log level, debug flags)

Override files MUST NOT modify:
- Service names
- Volume mount paths
- Health check definitions
- Image references (controlled via `IMAGE_TAG` env var)

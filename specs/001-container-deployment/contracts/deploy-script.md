# Deployment Script Contract

**Script**: `deploy/scripts/deploy.sh`
**Runtime**: Deployment server (Podman host)

## Interface

```bash
deploy.sh <command> [options]
```

## Commands

### `deploy`

Deploy or update the application stack.

```bash
deploy.sh deploy --env <staging|production> [--tag <image-tag>]
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--env` | Yes | — | Target environment (`staging` or `production`) |
| `--tag` | No | `<env>` (mutable pointer) | Specific image tag to deploy |

**Behavior**:
1. Pull the specified image from GHCR
2. Select compose override: `compose.yml` + `compose.<env>.yml`
3. Run `podman compose -f compose.yml -f compose.<env>.yml up -d`
4. Wait for health checks to pass (timeout: 60s)
5. Report deployment status

**Exit codes**:
- `0`: Success — all services healthy
- `1`: Failure — health check timeout or compose error
- `2`: Invalid arguments

### `rollback`

Rollback to a previous image version.

```bash
deploy.sh rollback --env <staging|production> --tag <image-tag>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--env` | Yes | — | Target environment |
| `--tag` | Yes | — | Image tag to roll back to (must exist in GHCR) |

**Behavior**:
1. Verify the specified tag exists in GHCR
2. Redeploy with the specified tag (same flow as `deploy`)
3. Report rollback status

**Exit codes**:
- `0`: Rollback successful
- `1`: Rollback failed (tag not found or health check failed)
- `2`: Invalid arguments

### `status`

Show current deployment status.

```bash
deploy.sh status --env <staging|production>
```

**Output**:
```
Environment: production
Image tag:   sha-a1b2c3d
Services:    worker (healthy), scheduler (healthy)
Uptime:      3d 12h 45m
Last deploy: 2026-06-19T10:30:00Z
```

**Exit codes**:
- `0`: Status retrieved
- `1`: Error (compose not running or env invalid)

### `list-tags`

List available image tags from GHCR.

```bash
deploy.sh list-tags [--limit <n>]
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--limit` | No | 10 | Number of tags to display |

**Output**: Recent tags sorted by creation date, newest first.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GHCR_TOKEN` | Private repos only | GHCR Personal Access Token with `read:packages` scope. Required on deploy servers for private image repos. Not needed if GHCR packages are set to public. (Note: this is a PAT, not `GITHUB_TOKEN` — `GITHUB_TOKEN` is only available inside GitHub Actions.) |
| `DEPLOY_CONFIG_DIR` | No | Host path to `.ini` config files (default: `/opt/automation/config`) |

## Prerequisites

- Podman installed and configured (rootless)
- `podman compose` available (podman-compose or podman compose plugin)
- `GHCR_TOKEN` set in environment
- Config files present at `DEPLOY_CONFIG_DIR`

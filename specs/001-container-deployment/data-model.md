# Data Model: Container Deployment

**Feature**: 001-container-deployment
**Date**: 2026-06-19

## Entity Relationship Diagram

```
┌──────────────────┐       ┌──────────────────────┐
│  Container Image  │       │  Configuration Volume │
│  (build artifact) │       │  (runtime mount)      │
├──────────────────┤       ├──────────────────────┤
│ sha_tag          │       │ lark_ini             │
│ semver_tag       │       │ maxcomputer_ini      │
│ base_image       │       │ mount_path           │
│ registry         │       │ read_only            │
│ build_date       │       └──────────┬───────────┘
│ multi_stage      │                  │
│ oci_compliant    │                  │ mounted_to
│ non_root_user    │                  │
└────────┬─────────┘                  │
         │                            │
         │  deployed_as               │
         ▼                            ▼
┌──────────────────┐       ┌──────────────────────┐
│   Service Stack   │       │   Environment Config  │
│   (compose def)   │       │   (per-environment)   │
├──────────────────┤       ├──────────────────────┤
│ worker_service   │       │ name (staging|prod)  │
│ scheduler_svc    │       │ branch               │
│ storage_service  │       │ config_files[]       │
│ health_checks    │       │ resource_limits      │
│ depends_on       │       │ compose_override     │
│ resource_limits  │       │ image_tag            │
│ compose_base     │       └──────────────────────┘
│ compose_override │
└──────────────────┘

┌──────────────────┐
│    CI Pipeline    │
│   (workflow def)  │
├──────────────────┤
│ trigger_branches │
│ jobs[]           │
│ gates[]          │
│ image_tags[]     │
│ registry_target  │
└──────────────────┘
```

## Entity Details

### Container Image

The immutable build artifact containing application code, Python runtime, and locked dependencies.

| Field | Type | Description |
|-------|------|-------------|
| `sha_tag` | string | Git commit short SHA (e.g., `sha-a1b2c3d`) |
| `semver_tag` | string | Semantic version from git tag (e.g., `v1.2.3`) |
| `env_tag` | string | Environment pointer (`staging` or `production`) |
| `base_image` | string | Pinned base image (`python:3.12-slim`) |
| `registry` | string | GHCR repository URL |
| `build_date` | datetime | Image build timestamp |
| `multi_stage` | boolean | Always `true` (Constitution VI) |
| `oci_compliant` | boolean | Always `true` (FR-019) |
| `non_root_user` | string | Runtime user (`appuser`, UID 10001) |

**Lifecycle**: Built on `staging`/`production` merge → pushed to GHCR → pulled by deploy host → runs with mounted config → retained for 2 release cycles → pruned.

**Validation rules**:
- MUST NOT contain `.env`, `*.ini`, `.venv/` (FR-010)
- MUST run as non-root (FR-009)
- MUST use `requirements.lock.txt` for dependencies (FR-007)

### Configuration Volume

Runtime-mounted `.ini` files providing environment-specific credentials and parameters.

| Field | Type | Description |
|-------|------|-------------|
| `lark_ini` | file path | Path to `_lark.ini` on host |
| `maxcomputer_ini` | file path | Path to `_maxcomputer.ini` on host |
| `mount_path` | string | Container path: `/app/automation/conf/` |
| `read_only` | boolean | Always `true` (security) |

**Constraints**:
- Not part of the image (excluded by `.dockerignore`)
- Loaded at startup only — runtime modifications require container restart
- Must be readable by UID 10001 (appuser)
- Production: mounted as independent secret volumes (not bind mount of source tree)

### Service Stack

The orchestrated set of containers defined in compose files.

| Field | Type | Description |
|-------|------|-------------|
| `worker_service` | service def | Runs ETL pipelines (`cron_task.sh` or specific worker) |
| `scheduler_service` | service def | Cron/systemd timer triggering worker execution |
| `storage_service` | service def | (Optional) Local data store if needed |
| `health_checks` | healthcheck[] | Per-service health verification |
| `depends_on` | dependency map | Service startup ordering |
| `resource_limits` | resource spec | CPU/memory per service (override per env) |
| `compose_base` | file path | `deploy/compose.yml` |
| `compose_override` | file path | `deploy/compose.{env}.yml` |

**Service definitions**:

| Service | Image | Command | Restart | Health Check |
|---------|-------|---------|---------|-------------|
| `worker` | `ghcr.io/.../automation:<tag>` | `bash /app/workers/cron_task.sh` | `unless-stopped` | `python -c "import automation, workers"` |
| `scheduler` | `ghcr.io/.../automation:<tag>` | `bash /app/deploy/scripts/scheduler.sh` | `always` | Process alive check |

### Environment Config

Per-environment deployment parameters.

| Field | Type | Description |
|-------|------|-------------|
| `name` | enum | `staging` or `production` |
| `branch` | string | Git branch that triggers deployment |
| `config_files` | file[] | Set of `.ini` files for this environment |
| `resource_limits` | resource spec | CPU/memory overrides |
| `compose_override` | file path | Override compose file |
| `image_tag` | string | Active image tag (mutable pointer) |

**Environment differences**:

| Property | Staging | Production |
|----------|---------|------------|
| Branch | `staging` | `production` |
| CPU limit | 2.0 | 4.0 |
| Memory limit | 2G | 8G |
| Log level | DEBUG | INFO |
| Smoke tests | No | Yes (gate) |
| Restart policy | `unless-stopped` | `always` |

### CI Pipeline

GitHub Actions workflow definitions.

| Field | Type | Description |
|-------|------|-------------|
| `trigger_branches` | string[] | `[main, workers, staging, production]` |
| `jobs` | job[] | Ordered execution steps |
| `gates` | gate[] | Pass/fail checkpoints |
| `image_tags` | string[] | Tags applied to built images |
| `registry_target` | string | GHCR repository |

**Workflow: ci.yml**
- Trigger: push to `main`, `workers`, `staging`, `production`
- Jobs: `lint` → `test`
- Gates: lint pass, test pass

**Workflow: deploy.yml**
- Trigger: push to `staging`, `production`
- Jobs: `lint` → `test` → `build` → `push` → `deploy` (→ `smoke` for production)
- Gates: all must pass sequentially

## State Transitions

### Image Lifecycle

```
[Built] ──push──→ [In Registry] ──pull──→ [Deployed] ──run──→ [Completed/Failed]
                      │                                              │
                      │  retain 2 cycles                             │
                      ▼                                              ▼
                  [Retained]                                    [Logs Collected]
                      │
                      │  prune
                      ▼
                  [Deleted]
```

### Deployment Flow

```
[Code Push] ──trigger──→ [CI Lint] ──pass──→ [CI Test] ──pass──→ [Build Image]
                                                                        │
    (staging/production only)                           ──push──→ [GHCR]
                                                                        │
                                                        ──pull──→ [Deploy Host]
                                                                        │
                                                        ──mount config──→ [Start Container]
                                                                        │
                                                        (production only) ──→ [Smoke Test]
                                                                        │
                                                        ──pass──→ [Live]
```

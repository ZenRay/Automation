# CI Workflow Contract

**Files**: `.github/workflows/ci.yml`, `.github/workflows/deploy.yml`

## ci.yml — Lint & Test

### Trigger

```yaml
on:
  push:
    branches: [main, workers, staging, production]
  pull_request:
    branches: [main, workers, staging, production]
```

### Jobs

| Job | Step | Description | Gate |
|-----|------|-------------|------|
| `lint` | `black --check .` | Code formatting check | Must pass |
| `lint` | `flake8 .` | Style/lint check | Must pass |
| `test` | `pytest tests/` | Unit tests | Must pass |

**Dependencies**: `test` depends on `lint` (sequential).

**Runtime**: `ubuntu-latest`, Python 3.12.

**No image build** in this workflow — image building is in `deploy.yml`.

## deploy.yml — Build, Push & Deploy

### Trigger

```yaml
on:
  push:
    branches: [staging, production]
```

### Jobs

| Job | Step | Description | Gate |
|-----|------|-------------|------|
| `lint` | `black --check .` | Code formatting | Must pass |
| `lint` | `flake8 .` | Style/lint | Must pass |
| `test` | `pytest tests/` | Unit tests | Must pass |
| `build` | `docker/build-push-action` | Build OCI image (no push) | Must succeed |
| `scan` | `trivy image --scanners secret --exit-code 1 <image>` | Secret detection in image layers | Must pass (zero secrets) |
| `verify` | `docker run --rm <image> find /app -name "*.ini" -o -name ".env"` | Verify no credential files in image | Must return empty |
| `push` | Push to GHCR | Registry upload (blocked if scan/verify fail) | Must succeed |
| `smoke` | (production only) Run smoke test | Pre-production validation | Must pass |

**Dependencies**:
- `test` depends on `lint`
- `build` depends on `test`
- `scan` depends on `build` (runs trivy secret scan on built image)
- `verify` depends on `build` (runs file presence check, can parallel with scan)
- `push` depends on `scan` AND `verify` (blocked if either fails)
- `smoke` depends on `push` (production branch only)

### Image Tagging Strategy

| Tag | Format | Mutable | Description |
|-----|--------|---------|-------------|
| SHA | `sha-<short-sha>` | No | Unique per commit |
| Semver | `v<major>.<minor>.<patch>` | No | From git tag |
| Branch | `staging` / `production` | Yes | Latest for environment |

### GHCR Authentication

```yaml
permissions:
  packages: write
  contents: read
```

Uses `GITHUB_TOKEN` (built-in, no PAT required).

### Smoke Test (Production Only)

```bash
# Run the worker with --dry-run or a lightweight pipeline check
docker run --rm ghcr.io/${{ env.REGISTRY }}/automation:${{ env.SHA_TAG }} \
  python -c "from workers.okr import run_okr_pipeline; print('import OK')"
```

**Pass criteria**: Exit code 0, no import errors, all packages loadable.

### Rollback Strategy

- Previous image tags retained in GHCR for minimum 2 release cycles
- Rollback executed via `deploy.sh rollback --env production --tag <previous-tag>`
- GHCR retention managed by scheduled cleanup (or manual until automated)

## Workflow Diagram

```
ci.yml (push to any branch):
  lint ──pass──→ test ──pass──→ ✅ Done (no image)
                  │
                  └──fail──→ ❌ Blocked

deploy.yml (push to staging/production):
  lint ──pass──→ test ──pass──→ build ──success──→ scan ──pass──┐
                  │                │                  │           │
                  └──fail──→ ❌    └──fail──→ ❌      │      verify ──pass──→ push ──→ [smoke*] ──→ ✅
                                                       │           │              │          │
                                                       └──fail──→ ❌ └──fail──→ ❌  └──fail→ ❌ └──fail→ ❌

  * smoke test only for production branch
  * scan and verify run in parallel after build
```

# Research: Container Deployment

**Feature**: 001-container-deployment
**Date**: 2026-06-19

## 1. OCI Compatibility: Docker Build ↔ Podman Runtime

**Decision**: Use Docker on GitHub Actions for building, Podman on deployment servers for running. Both engines produce OCI-compliant images — interoperability is guaranteed by the OCI Image Specification.

**Rationale**: GitHub Actions runners come with Docker pre-installed (zero setup cost). Podman is the preferred rootless runtime (no daemon, enhanced security). The OCI standard ensures images built by either engine are fully portable.

**Alternatives considered**:
- **Podman on CI**: Requires additional setup on GitHub Actions (not pre-installed). Adds CI complexity for no runtime benefit.
- **Docker on servers**: Requires daemon process, root privileges by default. Conflicts with security requirements.

**Key findings**:
- `docker build` and `podman build` produce identical OCI image manifests
- GHCR supports images from both engines — `podman pull ghcr.io/...` works on OCI images pushed by Docker
- Multi-stage builds work identically in both engines
- Layer caching behavior is equivalent

## 2. Multi-Stage Build Optimization for Python

**Decision**: Two-stage build — `builder` stage installs dependencies into a virtualenv, `runtime` stage copies only the virtualenv and application code.

**Rationale**: Build tools (gcc, pip cache, wheel files) add ~200MB+ to image size. Multi-stage eliminates these from the final image, producing a lean runtime container.

**Stage layout**:

```
Stage 1 (builder):
  FROM python:3.12-slim AS builder
  → Install system build deps (gcc, libffi-dev if needed)
  → Create virtualenv at /opt/venv
  → COPY requirements.lock.txt
  → pip install --no-cache-dir -r requirements.lock.txt
  → Build artifacts: /opt/venv/

Stage 2 (runtime):
  FROM python:3.12-slim
  → Create non-root user (appuser, UID 10001)
  → COPY --from=builder /opt/venv /opt/venv
  → COPY application code (automation/, workers/, pyproject.toml)
  → Set PATH to include /opt/venv/bin
  → HEALTHCHECK
  → ENTRYPOINT
```

**Alternatives considered**:
- **Single stage with pip --user**: Larger image (includes pip cache, build tools). Rejected.
- **Three-stage with separate test stage**: Adds complexity. Testing runs in CI, not in image build. Rejected.

**Layer caching strategy**:
1. COPY `requirements.lock.txt` first (changes rarely)
2. `pip install` (cached unless lockfile changes)
3. COPY application code last (changes frequently)

## 3. GitHub Actions Docker Build

**Decision**: Use `docker/build-push-action@v6` for building and pushing to GHCR. Use `docker/metadata-action@v5` for tag management.

**Rationale**: Official Docker GitHub Actions are maintained, well-documented, and support GHCR natively via `GITHUB_TOKEN`.

**Tag strategy**:
- `sha-<git-sha-short>`: Unique per commit, enables rollback
- `v<major>.<minor>.<patch>`: Semantic version from git tag
- `staging`: Latest staging image (mutable pointer)
- `production`: Latest production image (mutable pointer)

**Alternatives considered**:
- **Manual docker build + push**: More control but more YAML. Rejected for maintainability.
- **Buildx with cache registry**: Overkill for 12-package Python project with fast builds. Rejected.

## 4. Compose Override Pattern

**Decision**: Base `compose.yml` defines services, volumes, health checks, and dependency ordering. Environment-specific overrides (`compose.staging.yml`, `compose.production.yml`) adjust resource limits and replica counts.

**File precedence**: `compose.yml` + `compose.staging.yml` (or `compose.production.yml`)

**Usage**:
```bash
# Staging
docker compose -f compose.yml -f compose.staging.yml up -d

# Production
docker compose -f compose.yml -f compose.production.yml up -d
```

**Override contents**:
| Property | compose.yml (base) | compose.staging.yml | compose.production.yml |
|----------|-------------------|---------------------|------------------------|
| CPU limit | 2.0 | 2.0 | 4.0 |
| Memory limit | 4G | 2G | 8G |
| Replicas | 1 | 1 | 1 |
| Restart policy | unless-stopped | unless-stopped | always |
| Log level | INFO | DEBUG | INFO |

**Alternatives considered**:
- **Single compose file with variable substitution**: Harder to read, requires `.env` per host. Rejected.
- **Kubernetes manifests**: Over-engineered for single-server deployment. Rejected.

## 5. GHCR Authentication & Image Retention

**Decision**: Use `GITHUB_TOKEN` for GHCR push (built-in, no PAT needed). Implement retention policy: keep last 2 version tags + all `staging`/`production` mutable tags.

**Rationale**: `GITHUB_TOKEN` has `packages:write` permission by default in GitHub Actions. No additional secrets needed.

**Retention enforcement**: Add a scheduled workflow (or step in deploy.yml) that lists GHCR package versions and deletes tags older than 2 releases, except `staging` and `production`.

**Alternatives considered**:
- **Personal Access Token**: Requires manual rotation. Rejected.
- **No retention policy**: GHCR storage grows unbounded. Rejected.

## 6. Structured Logging

**Decision**: Initial deployment uses existing text-format logging (stdout). JSON structured logging (FR-016) is deferred to a follow-up feature that will add a dedicated logging configuration module in `automation/` (requires user approval per Constitution Principle I).

**Rationale**: The existing `workers/__init__.py` configures a `StreamHandler` with text format (`[%(levelname)s]:%(asctime)s %(message)s`). Injecting a JSON formatter without modifying this file requires a fragile wrapper approach (e.g., `sitecustomize.py` or PYTHONSTARTUP injection) that adds complexity and maintenance burden for marginal benefit at initial deployment.

**Current approach (Phase 1)**:
- Container logs output to stdout in existing text format
- Container engine (Podman) collects stdout for log aggregation
- `PYTHONLOGFORMAT` environment variable is reserved for future activation

**Future approach (Phase 2, separate feature)**:
- Add a JSON formatter configuration in `automation/` layer (with user approval)
- Workers automatically pick up JSON format when `PYTHONLOGFORMAT=json` is set
- JSON log format:
  ```json
  {
    "timestamp": "2026-06-19T10:30:00.123Z",
    "level": "INFO",
    "module": "workers.okr.main",
    "message": "Pipeline completed successfully"
  }
  ```

**Alternatives considered**:
- **entrypoint.sh wrapper with sitecustomize.py**: Fragile, hard to debug, and depends on Python path ordering. Rejected for Phase 1.
- **structlog library**: Adds dependency. Project uses stdlib logging. Rejected.
- **Modifying `workers/__init__.py`**: Would change application code without explicit approval. Rejected per Constitution Principle I.

## 7. Non-Root User in Python Containers

**Decision**: Create `appuser` with UID 10001 (high UID to avoid host user conflicts). Set `USER appuser` in the runtime stage.

**Rationale**: UID 10001 is above typical host user ranges, avoiding accidental permission escalation on bind mounts.

**Implementation**:
```dockerfile
RUN groupadd -g 10001 appgroup && useradd -u 10001 -g appgroup -m appuser
USER appuser
```

**Volume mount permissions**: `.ini` config files mounted via volumes must be readable by UID 10001. The compose file sets `read_only: true` on config volumes. Host-side files should have world-readable permissions (mode 644) or the deploy script should `chown` them.

## 8. Health Check Design for Batch ETL Workers

**Decision**: Since workers are batch processes (not long-running services), health checks verify:
1. **Image-level**: `python -c "import automation, workers"` confirms all packages load
2. **Compose-level**: Check that the process exits cleanly (exit code 0) or is running (for scheduler)

**Rationale**: ETL workers run to completion (cron-triggered). Traditional HTTP health checks don't apply. The health check verifies the environment is valid, not that a service is "up".

**Alternatives considered**:
- **HTTP health endpoint**: Workers don't serve HTTP. Rejected.
- **No health check**: Fails SC-006. Rejected.
- **File-based health check**: Worker writes a health file on completion. Over-engineered for batch. Rejected.

## 9. Scheduler Implementation

**Decision**: Shell loop script (`deploy/scripts/scheduler.sh`) — runs `cron_task.sh`, sleeps for `SCHEDULE_INTERVAL`, repeats. No system cron dependency.

**Rationale**: `python:3.12-slim` does not include a cron daemon. Installing cron adds ~5MB and an init system dependency. A shell loop is simpler, more portable, and the interval is configurable via environment variable.

**Script behavior**:
```bash
#!/bin/bash
INTERVAL=${SCHEDULE_INTERVAL:-3600}
while true; do
    bash /app/workers/cron_task.sh
    sleep "$INTERVAL"
done
```

**Alternatives considered**:
- **Install cron in Dockerfile**: Adds system dependency, init complexity. Rejected.
- **Systemd timer**: Not applicable inside containers without systemd. Rejected.
- **Python schedule library**: Adds dependency for trivial use case. Rejected.
- **Host-level cron triggering `docker exec`**: Couples host and container lifecycle. Rejected.

## 10. Credential Security — Attack Surface & Defense in Depth

### Identified Risks

| Risk | Severity | Attack Vector | Mitigation |
|------|----------|--------------|------------|
| **R1: Plaintext .ini readable inside container** | Medium | `docker exec cat /app/automation/conf/_lark.ini` — any user with container access can read credentials | Non-root user (UID 10001) limits scope; `:ro` mount prevents tampering; restrict `docker exec` access via host OS ACLs |
| **R2: .ini residual in CI build context** | High | GitHub Actions checkout includes full repo; if secrets are restored for testing, they may be COPY'd into image | `.dockerignore` at project root excludes `automation/conf/*.ini`; CI build job must NOT restore `.ini` files before `docker build` step |
| **R3: Secret leakage in image layer history** | High | If a past build accidentally included `.ini`, the layer persists in GHCR for 2 release cycles | `trivy` image scan in CI detects secrets; post-build `find` verification; emergency cleanup procedure |
| **R4: Credential leakage via container logs** | Medium | Debug logging or exception tracebacks that include config objects may print APP_SECRET to stdout | Convention: never log config objects; future feature: add sensitive field filter to log formatter |

### Defense Layers

```
Layer 1: .gitignore          → prevents .ini from entering git repo
Layer 2: .dockerignore       → prevents .ini from entering build context
Layer 3: trivy scan (CI)     → detects secrets that bypassed layers 1-2
Layer 4: post-build find     → verifies no .ini files in final image
Layer 5: non-root user       → limits blast radius if container is compromised
Layer 6: :ro volume mount    → prevents credential tampering at runtime
Layer 7: log convention      → prevents accidental credential logging
Layer 8: emergency procedure → credential rotation + image deletion if breach occurs
```

### CI Integration

The `build` job in `deploy.yml` must include these steps **after** image build:

1. **Secret scan**: Run `trivy image --scanners secret <image>` — fail the pipeline if any secrets detected
2. **File verification**: Run `docker run --rm <image> find /app -name "*.ini" -o -name ".env"` — must return empty
3. Both steps are mandatory gates — image push is blocked if either fails

### Emergency Cleanup Procedure

If credentials are discovered in a published image:

1. **Immediately rotate** all affected credentials (Lark APP_SECRET, MaxCompute secret_access_key)
2. **Delete compromised images** from GHCR:
   ```bash
   # Via GitHub API or gh CLI
   gh api --method DELETE /orgs/<org>/packages/container/<package>/versions/<version_id>
   ```
3. **Rebuild and push** a clean image
4. **Audit access logs** — check GHCR pull logs for unauthorized downloads
5. **Post-incident review** — document root cause and update `.dockerignore`/CI pipeline as needed

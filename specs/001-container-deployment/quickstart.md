# Quickstart: Container Deployment Validation

**Feature**: 001-container-deployment
**Purpose**: Runnable validation scenarios proving the deployment feature works end-to-end.

## Prerequisites

- Docker installed (for local build testing)
- Podman installed (for runtime compatibility testing)
- Access to GHCR (GHCR_TOKEN or `docker login ghcr.io`)
- Sample `.ini` config files available (can use templates with test credentials)

## Scenario 1: Local Image Build

**Validates**: FR-006, FR-007, FR-009, FR-010, FR-011, FR-017, FR-018

```bash
# Build the image (context = project root, Dockerfile in deploy/)
docker build -f deploy/Dockerfile -t automation:test .

# Verify: secret scan (trivy)
trivy image --scanners secret --exit-code 1 automation:test
# Expected: exit code 0, no secrets detected

# Verify: no secret files in image
docker run --rm automation:test find /app -name "*.ini" -o -name ".env"
# Expected: no output (empty)

# Verify: non-root user
docker run --rm automation:test whoami
# Expected: appuser

# Verify: all packages load
docker run --rm automation:test python -c "import automation, workers; print('OK')"
# Expected: OK

# Verify: base image version
docker inspect automation:test | grep -i "python:3.12"
# Expected: python:3.12-slim in image history

# Verify: image size (should be lean, multi-stage benefit)
docker images automation:test
# Expected: < 500MB (vs ~1GB single-stage)
```

## Scenario 2: Compose Startup with Config Mount

**Validates**: FR-008, FR-012, FR-013, FR-016, SC-006

```bash
# Prepare config directory
mkdir -p /tmp/test-config
cp automation/conf/_lark.ini /tmp/test-config/
cp automation/conf/_maxcomputer.ini /tmp/test-config/

# Start with compose
cd deploy
CONFIG_DIR=/tmp/test-config IMAGE_TAG=test GHCR_REPO=local \
  docker compose -f compose.yml up -d

# Verify: services running
docker compose -f compose.yml ps
# Expected: worker and scheduler services "running" or "healthy"

# Verify: config loaded (check logs)
docker compose -f compose.yml logs worker | head -20
# Expected: Log entries with [LEVEL]:timestamp format (text mode, JSON deferred to Phase 2)

# Verify: health check passing
docker inspect --format='{{.State.Health.Status}}' $(docker compose ps -q worker)
# Expected: healthy

# Cleanup
docker compose -f compose.yml down
```

## Scenario 3: CI Pipeline Gate Verification

**Validates**: FR-004, FR-005, SC-004

```bash
# Simulate lint gate (should pass on clean code)
black --check .
flake8 .

# Simulate test gate
pytest tests/

# Simulate failed gate (create intentional lint error)
echo "x=1" > /tmp/lint_test.py
black --check /tmp/lint_test.py
# Expected: exit code != 0, would block image build

rm /tmp/lint_test.py
```

**CI-specific validation** (requires GitHub Actions):
1. Push a commit to `main` → verify ci.yml triggers, lint + test run, no image built
2. Merge to `staging` → verify deploy.yml triggers, image built and pushed to GHCR
3. Introduce a lint error on `staging` → verify deploy.yml fails at lint gate, no image produced

## Scenario 4: Rollback

**Validates**: FR-014, SC-003

```bash
# Deploy current version
deploy.sh deploy --env staging --tag sha-current

# Deploy previous version
deploy.sh rollback --env staging --tag sha-previous

# Verify: running previous version
deploy.sh status --env staging
# Expected: image tag shows sha-previous

# Roll forward again
deploy.sh deploy --env staging --tag sha-current
```

## Scenario 5: Podman Compatibility

**Validates**: FR-019, SC-010

```bash
# Pull image built by Docker (from GHCR)
podman pull ghcr.io/<org>/automation:sha-<test-sha>

# Run with Podman
podman run --rm \
  -v /tmp/test-config/_lark.ini:/app/automation/conf/_lark.ini:ro \
  -v /tmp/test-config/_maxcomputer.ini:/app/automation/conf/_maxcomputer.ini:ro \
  ghcr.io/<org>/automation:sha-<test-sha> \
  python -c "import automation, workers; print('Podman OK')"
# Expected: Podman OK

# Run with compose (Podman)
podman compose -f compose.yml -f compose.staging.yml up -d
podman compose ps
# Expected: services running
podman compose down
```

## Scenario 6: Multi-Environment Config Isolation

**Validates**: FR-008 (AC2), SC-002

```bash
# Deploy same image to staging and production with different configs
mkdir -p /tmp/staging-config /tmp/production-config
# Place different .ini files in each directory

# Staging
CONFIG_DIR=/tmp/staging-config IMAGE_TAG=test GHCR_REPO=local \
  docker compose -f compose.yml -f compose.staging.yml up -d

# Production (separate host or separate compose project)
CONFIG_DIR=/tmp/production-config IMAGE_TAG=test GHCR_REPO=local \
  docker compose -f compose.yml -f compose.production.yml up -d

# Verify: each environment reads its own config
# Check logs or application behavior for environment-specific values
```

## Checklist

| Scenario | Status | Notes |
|----------|--------|-------|
| 1. Local Build | ⬜ | |
| 2. Compose Startup | ⬜ | |
| 3. CI Pipeline Gates | ⬜ | |
| 4. Rollback | ⬜ | |
| 5. Podman Compatibility | ⬜ | |
| 6. Multi-Environment | ⬜ | |

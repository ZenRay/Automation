# Tasks: Container Deployment

**Input**: Design documents from `specs/001-container-deployment/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Not explicitly requested — test tasks are limited to quickstart validation scenarios.

**Organization**: 3 phases covering build → deploy → CI/CD, each with local verification.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)
- Include exact file paths in descriptions

## Phase 1: Container Foundation & Local Build (US1)

**Purpose**: Build the container image definition and verify it locally.

**Goal**: A working Dockerfile that produces a lean, secure, OCI-compliant image runnable on both Docker and Podman.

**Independent Test**: `docker build -f deploy/Dockerfile -t automation:test .` succeeds; image passes all verification steps below.

### Implementation

- [x] T001 [P] Create build context exclusion rules in `.dockerignore` — exclude `.venv/`, `automation/conf/*.ini`, `.env`, `.git/`, `__pycache__/`, `*.pyc`, `logs/`, `tests/`, `specs/`, `notes/`, `PrivateWork/`, `dispatcher/`, `.specify/`, `.qoder/`, `*.md` (except root README), `cr_trail_pricing_*.xlsx`
- [x] T002 [P] Create multi-stage Dockerfile in `deploy/Dockerfile` — builder stage: `python:3.12-slim`, create virtualenv at `/opt/venv`, `COPY requirements.lock.txt`, `pip install --no-cache-dir`; runtime stage: `python:3.12-slim`, create `appuser` (UID 10001), `COPY --from=builder /opt/venv`, `COPY automation/ workers/ pyproject.toml`, set `PATH=/opt/venv/bin:$PATH`, `HEALTHCHECK CMD python -c "import automation, workers"`, `USER appuser`, `ENTRYPOINT`
- [x] T003 [P] Create container scripts in `deploy/entrypoint.sh` and `deploy/scripts/scheduler.sh` — entrypoint: set `TZ=Asia/Shanghai`, exec passed arguments; scheduler: `while true; do bash /app/workers/cron_task.sh; sleep ${SCHEDULE_INTERVAL:-3600}; done`; ensure both executable (`chmod +x`)
- [x] T004 [US1] Local build validation per quickstart Scenario 1:
  - `docker build -f deploy/Dockerfile -t automation:test .`
  - `docker run --rm automation:test whoami` → `appuser`
  - `docker run --rm automation:test find /app -name "*.ini" -o -name ".env"` → empty
  - `docker run --rm automation:test python -c "import automation, workers; print('OK')"` → `OK`
  - `docker images automation:test` → < 500MB

**Checkpoint**: Image builds, runs as non-root, contains no secrets, all packages load.

---

## Phase 2: Orchestration, Deployment & Compose Validation (US2 + US3)

**Purpose**: Service orchestration with compose files, deployment script, and local compose validation.

**Goal**: A deployable stack with health checks, config injection, resource limits, and environment overrides.

**Independent Test**: `docker compose -f deploy/compose.yml up -d` with mounted `.ini` configs; services pass health checks.

### Implementation

- [x] T005 [P] [US2] Create base compose file in `deploy/compose.yml` — define `worker` service (image from `ghcr.io/${GHCR_REPO}/automation:${IMAGE_TAG}`, command `bash /app/workers/cron_task.sh`, healthcheck `python -c "import automation, workers"`, volumes for `_lark.ini` and `_maxcomputer.ini` at `/app/automation/conf/` with `:ro`, env `TZ=Asia/Shanghai`, `PYTHONLOGFORMAT=text`, `LOG_LEVEL=INFO`, resource limits `cpus: 2.0, memory: 4G`); define `scheduler` service (same image, command `bash /app/deploy/scripts/scheduler.sh`, restart `always`, healthcheck `pgrep -f scheduler.sh`, depends_on worker)
- [x] T006 [P] [US2] Create environment overrides in `deploy/compose.staging.yml` and `deploy/compose.production.yml` — staging: resources `cpus: 2.0, memory: 2G`, `LOG_LEVEL=DEBUG`, restart `unless-stopped`; production: resources `cpus: 4.0, memory: 8G`, `LOG_LEVEL=INFO`, restart `always`. Both keep `PYTHONLOGFORMAT=text` (format variable, not log level)
- [x] T007 [P] [US3] Create deployment script in `deploy/scripts/deploy.sh` — support `deploy`, `rollback`, `status`, `list-tags` commands per contracts/deploy-script.md; accept `--env` and `--tag` arguments; use `podman compose` with base + override file selection; include health check wait with 60s timeout
- [x] T008 [US2] Local compose validation per quickstart Scenario 2:
  - Prepare config: `mkdir -p /tmp/test-config && cp automation/conf/_lark.ini automation/conf/_maxcomputer.ini /tmp/test-config/`
  - Start: `CONFIG_DIR=/tmp/test-config IMAGE_TAG=test GHCR_REPO=local docker compose -f deploy/compose.yml up -d`
  - Verify services running: `docker compose -f deploy/compose.yml ps`
  - Verify config loaded: `docker compose -f deploy/compose.yml logs worker | head -20`
  - Verify health check: `docker inspect --format='{{.State.Health.Status}}' $(docker compose ps -q worker)` → `healthy`
  - Cleanup: `docker compose -f deploy/compose.yml down`

**Checkpoint**: Stack starts with mounted config, services healthy, config readable by appuser.

---

## Phase 3: CI/CD Pipeline & Full Validation (Cross-cutting)

**Purpose**: GitHub Actions workflows for lint/test/build/deploy gates, plus full quickstart validation.

**Goal**: Automated CI pipeline that gates image production on passing all checks including secret scanning.

**Independent Test**: Push to `main` triggers lint+test; merge to `staging` triggers build+scan+verify+push.

### Implementation

- [x] T009 [P] Create CI workflow in `.github/workflows/ci.yml` — trigger on push to `main`, `workers`, `staging`, `production`; jobs: `lint` (black --check, flake8) → `test` (pytest tests/); Python 3.12 on ubuntu-latest; no image build
- [x] T010 [P] Create deploy workflow in `.github/workflows/deploy.yml` — trigger on push to `staging`, `production`; jobs: `lint` → `test` → `build` (docker/build-push-action, no push) → `scan` (trivy image --scanners secret --exit-code 1) + `verify` (docker run find /app -name "*.ini" -o -name ".env", parallel with scan) → `push` (push to GHCR, depends on scan AND verify pass) → `smoke` (production only); image tags: `sha-<short>`, semver from git tag, branch pointer (`staging`/`production`); permissions: `packages: write`, `contents: read`
- [x] T011 [P] Create environment variable example in `deploy/.env.example` — document required variables: `GHCR_REPO` (e.g., `org/automation`), `IMAGE_TAG`, `CONFIG_DIR`, `SCHEDULE_INTERVAL` (default 3600)
- [x] T012 Post-deployment validation per quickstart Scenarios 4 + 5 — **requires CI to have pushed at least 2 image versions to GHCR**:
  - Podman pull: `podman pull ghcr.io/<org>/automation:sha-<test-sha>`
  - Podman run: `podman run --rm -v /tmp/test-config/_lark.ini:/app/automation/conf/_lark.ini:ro -v /tmp/test-config/_maxcomputer.ini:/app/automation/conf/_maxcomputer.ini:ro ghcr.io/<org>/automation:sha-<test-sha> python -c "import automation, workers; print('Podman OK')"`
  - Podman compose: `podman compose -f deploy/compose.yml -f deploy/compose.staging.yml up -d` → services running → `podman compose down`
  - Rollback: `bash deploy/scripts/deploy.sh deploy --env staging --tag sha-current` → `bash deploy/scripts/deploy.sh rollback --env staging --tag sha-previous` → `bash deploy/scripts/deploy.sh status --env staging` shows previous tag → restore with `sha-current`

**Checkpoint**: CI gates block bad builds, trivy detects secrets, Podman runs Docker-built images, rollback works.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1** (Container Foundation): No dependencies — can start immediately
- **Phase 2** (Orchestration): Depends on Phase 1 (Dockerfile must exist for compose to reference)
- **Phase 3** (CI/CD): Depends on Phase 1 (Dockerfile for build step) and Phase 2 (compose files for deploy script)

### Within Each Phase

- Phase 1: T001–T003 can run in parallel (different files). T004 depends on T001–T003.
- Phase 2: T005–T007 can run in parallel (different files). T008 depends on T005–T007.
- Phase 3: T009–T011 can run in parallel (different files). T012 depends on prior phases and a CI-pushed image.

### Parallel Opportunities

```bash
# Phase 1: All implementation tasks in parallel
Task: T001 (.dockerignore)
Task: T002 (Dockerfile)
Task: T003 (entrypoint.sh + scheduler.sh)
# Then: T004 (local build validation)

# Phase 2: All implementation tasks in parallel
Task: T005 (compose.yml)
Task: T006 (compose.staging.yml + compose.production.yml)
Task: T007 (deploy.sh)
# Then: T008 (compose validation)

# Phase 3: CI workflows in parallel
Task: T009 (ci.yml)
Task: T010 (deploy.yml)
Task: T011 (.env.example)
# Then: T012 (Podman + rollback validation, requires pushed image)
```

## Implementation Strategy

### MVP First (Phase 1 Only)

1. Complete Phase 1: Dockerfile + .dockerignore + entrypoint + scheduler
2. Run T004 local build validation
3. **STOP and VALIDATE**: Image builds, no secrets, non-root, packages load
4. This is the MVP — a working container image

### Incremental Delivery

1. Phase 1 → Local build works → Image is deployable
2. Phase 2 → Compose stack works → Full environment deployment
3. Phase 3 → CI/CD works → Automated pipeline with security gates

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- All file paths are relative to project root
- Local validation tasks (T004, T008) require Docker installed locally
- T012 requires Podman installed and at least 2 image versions pushed to GHCR by CI
- `PYTHONLOGFORMAT` controls output format (`text`/`json`); `LOG_LEVEL` controls verbosity (DEBUG/INFO/WARNING)

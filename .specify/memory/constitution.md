<!--
Sync Impact Report:
- Version change: v1.0.0 → v1.1.0
- Modified principles: II, V, VI, VII (Docker → Podman terminology)
- Modified sections: Technical Constraints, Development Workflow
  (all docker-compose.yml → podman-compose.yml,
   Dockerfile → Containerfile,
   .dockerignore → .containerignore,
   Docker image/build → container image/build)
- Added sections: None
- Removed sections: None
- Templates requiring updates:
  - .specify/templates/plan-template.md: ✅ no changes needed
  - .specify/templates/spec-template.md: ✅ no changes needed
  - .specify/templates/tasks-template.md: ✅ no changes needed
  - .specify/templates/commands/*.md: ✅ no files found
- Follow-up TODOs: None
-->

# Automation Constitution

## Core Principles

### I. Layer Isolation (NON-NEGOTIABLE)

- `automation/` is the infrastructure layer (client wrappers,
  configuration parsing, utility functions). Workers modules MUST NOT
  modify any code in this layer without explicit user approval.
- `workers/lib/` is the reusable framework layer (extractors,
  transformers, routers, loaders, configuration models).
- `workers/<module>/` (e.g. `okr/`, `cr_trail/`) is the business
  layer. Each business domain MUST be an independent module;
  cross-domain coupling is strictly forbidden.
- The business layer MUST call Lark and MaxCompute APIs through
  `automation/client/` wrappers; direct HTTP requests are forbidden.

**Rationale**: Prevents ad-hoc API calls and coupling that makes
refactoring unsafe and testing impossible at each layer boundary.

### II. Configuration-Driven (NON-NEGOTIABLE)

- All business parameters (app_token, table_id, SQL files,
  field_mappings, target table configs) MUST be centralized in each
  module's `config.py`.
- Hard-coding field names, table IDs, app tokens, or SQL strings in
  `main.py` or `transformer.py` is forbidden.
- Configuration items MUST use named constants; list-index access
  and magic numbers are forbidden.
- Runtime secrets MUST be injected via environment variables or
  mounted configuration files; packaging secrets into container images
  is forbidden.

**Rationale**: Ensures every deployment target is selectable via
configuration alone, eliminating environment-specific code paths.

### III. Date Type Unification (NON-NEGOTIABLE)

- All date columns MUST be converted to `datetime.date` at the
  extract stage. DataFrames in downstream stages MUST NOT contain
  datetime values with time components or Excel serial numbers.
- Date-filter cutoffs MUST use `date.today()` (NOT `datetime.now()`)
  and MUST be truncated to midnight (zero time component).
- Lark ExactDate timestamps MUST be constructed using UTC midnight:
  `calendar.timegm(date.timetuple()) * 1000`. Using local-timezone
  midnight converted to UTC is forbidden, to prevent date offset
  caused by system timezone / Lark document timezone mismatch.

**Rationale**: Eliminates an entire class of timezone-related data
corruption bugs that are silent in testing but surface in production.

### IV. Data Pipeline Integrity

- Every ETL module MUST follow the standard flow:
  Extract → Transform → Route → Load.
- New ETL modules MUST integrate through the two-phase registration
  architecture.
- Cleanup strategies (`CleanupCondition`) MUST be explicitly
  configured in `config.py`; silent no-cleanup is forbidden.
- Lark field types MUST be declared using the `LarkFieldType` enum;
  raw strings are forbidden.

**Rationale**: Standardized pipeline structure enables shared tooling,
consistent error handling, and predictable data flow across modules.

### V. Credential Security (NON-NEGOTIABLE)

- API credentials MUST be read from configuration files or
  environment variables; hard-coding is forbidden.
- Configuration files containing secrets MUST be listed in
  `.gitignore` and MUST NOT be included via Containerfile `COPY` directives.
- Container builds MUST use `.containerignore` to exclude `.venv/`,
  `automation/conf/*.ini`, and `.env` files.
- All Lark API requests MUST include a `timeout` parameter.
- Runtime configuration injection (in priority order):
  1. `podman-compose` volumes mounting `.ini` files to the
     container's designated path (recommended — compatible with
     existing ConfigParser logic, no code changes required).
  2. Environment variable injection, where the `conf` layer
     implements env-var-first / `.ini`-fallback read strategy.
- Production environments MUST NOT use bind mounts for source
  directories; configuration files MUST be mounted as independent
  secret volumes.

**Rationale**: Prevents credential leakage through image layers,
version control, or misconfigured production mounts.

### VI. Containerization Standards (NON-NEGOTIABLE)

- All runtime environments MUST be reproducible via Podman; local
  development and production MUST use the same base image.
- Containerfiles MUST use multi-stage builds to minimize image size.
- Base images MUST lock to a specific Python version tag
  (e.g. `python:3.12-slim`); `:latest` is forbidden.
- Dependency installation in container builds MUST use
  `requirements.lock.txt` (NOT `pyproject.toml`) to ensure
  deterministic environments.
- All long-running services MUST define health checks in both the
  Containerfile and `podman-compose.yml`.
- Container processes MUST run as a non-root user.

**Rationale**: Reproducible, minimal images with health checks
prevent configuration drift and reduce attack surface.

### VII. CI/CD Pipeline (NON-NEGOTIABLE)

- Every push to `main`/`workers` branches MUST automatically trigger
  linting (`black --check`, `flake8`) and unit tests.
- Container image builds MUST trigger on merge to `main`; images MUST
  be tagged with both the git SHA and a semantic version number.
- Production deployments MUST pass all gates:
  (1) linting, (2) unit tests, (3) container build,
  (4) pre-production smoke tests.
- A rollback strategy MUST be documented: the previous version's
  image tag MUST be retained for at least 2 release cycles.
- CI/CD configuration files (`.github/workflows/*.yml`) MUST be
  reviewed with the same rigor as application code.

**Rationale**: Automated gates prevent regressions; documented
rollback ensures rapid recovery from failed deployments.

## Technical Constraints

- Python >= 3.12; synchronous execution only — introducing `asyncio`
  requires explicit approval.
- `pandas` is the sole DataFrame manipulation library.
- MaxCompute queries MUST use parameterized SQL template files
  (`.sql`).
- Lark API `field_names` parameter MUST be JSON-serialized via
  `json.dumps()`.
- `podman-compose.yml` MUST separate services by responsibility:
  application worker, scheduler (Airflow), data storage.
- Production deployments MUST NOT mount source code via
  `podman-compose` volumes; code MUST be baked into the image.

## Development Workflow

- New ETL modules MUST include end-to-end tests.
- New Lark fields MUST be declared with `LarkFieldType` in
  `field_mappings` simultaneously.
- Containerfile changes MUST trigger rebuild verification in CI.
- Commit messages MUST follow Conventional Commits:
  `<type>(<scope>): <subject>`.

## Governance

- When this constitution conflicts with `AGENTS.md` or `README.md`,
  this constitution takes precedence.
- Amendments MUST be annotated in commit messages:
  `docs: amend constitution to vX.Y.Z`.
- Versioning follows Semantic Versioning:
  - **MAJOR**: Principle removal or backward-incompatible
    redefinition.
  - **MINOR**: New principle added or materially expanded guidance.
  - **PATCH**: Clarifications, wording fixes, non-semantic
    refinements.
- CI/CD pipeline definitions are governed artifacts; changes MUST
  undergo the same review process as application code.
- All PRs and reviews MUST verify compliance with this constitution.
- Complexity that violates a principle MUST be justified in the
  Complexity Tracking section of `plan.md`.

**Version**: 1.1.0 | **Ratified**: 2026-06-18 | **Last Amended**: 2026-06-18

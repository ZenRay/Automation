# Feature Specification: Container Deployment

**Feature Branch**: `001-container-deployment`

**Created**: 2026-06-18

**Status**: Draft

**Input**: User description: "为 Automation 项目设计独立容器化部署方案：Podman 容器引擎、GitHub Actions CI、COPY 模式不可变镜像、.ini volume 配置注入、GHCR 镜像仓库、git SHA + semver 标签策略"

## Clarifications

### Session 2026-06-18

- Q: Production deployment branch strategy? → A: Environment branches model — `main` (development) → `staging` → `production`, each branch corresponds to a deployment environment.
- Q: Container observability strategy? → A: Structured logging — JSON format to stdout with timestamp, log level, and module name.
- Q: Runtime container engine? → A: Podman on deployment hosts; Docker on CI (GitHub Actions). Both share OCI image format — images built by Docker run on Podman without modification.
- Q: Base image version policy? → A: Pinned to `python:3.12-slim`; `:latest` is FORBIDDEN (Constitution Principle VI NON-NEGOTIABLE).
- Q: Build optimization strategy? → A: Multi-stage builds required — build stage for dependency installation, runtime stage with only necessary artifacts (Constitution Principle VI NON-NEGOTIABLE).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Automated Image Build (Priority: P1)

A data pipeline developer pushes code to the `main` or `workers` branch. The CI system runs linting and tests. When code is promoted to the `staging` branch, an image is built and deployed to the staging environment. When promoted to the `production` branch, an image is built, smoke tests pass, and the image is deployed to production. All images are tagged with the git SHA and semantic version and pushed to the container registry.

**Why this priority**: Without a reliable, automated build pipeline, no deployment can happen. This is the foundation that all other stories depend on.

**Independent Test**: Push a commit to `main`, verify lint and tests run. Merge to `staging`, verify a tagged image appears in the registry within 15 minutes and is deployed to staging.

**Acceptance Scenarios**:

1. **Given** a push to `main` or `workers` branch, **When** CI pipeline runs, **Then** linting and unit tests execute; no image is built.
2. **Given** a merge to `staging` branch, **When** CI pipeline runs, **Then** a container image is built, tagged with git SHA and semantic version, pushed to the registry, and deployed to the staging environment.
3. **Given** a merge to `production` branch, **When** CI pipeline runs, **Then** a container image is built, tagged, pushed to the registry, pre-production smoke tests pass, and the image is deployed to the production environment.
4. **Given** a failed linting or unit test step at any branch, **When** CI pipeline runs, **Then** the pipeline stops and no image is produced or deployed.
5. **Given** a successful build, **When** the image is inspected, **Then** the container process runs as a non-root user and no secret files are present inside the image.

---

### User Story 2 - Configurable Deployment (Priority: P2)

A DevOps engineer deploys the containerized application to a target environment by providing environment-specific configuration files via volume mounts, without modifying the container image or application code.

**Why this priority**: The build pipeline (US1) must exist before deployment is possible. Configuration injection is the second critical capability that makes the image reusable across environments.

**Independent Test**: Pull a built image, mount a set of `.ini` configuration files, start the container, and verify the application reads the configuration correctly without any code changes.

**Acceptance Scenarios**:

1. **Given** a container image from the registry, **When** the operator mounts `.ini` configuration files to the designated path and starts the container, **Then** the application loads configuration from the mounted files with zero code changes.
2. **Given** two target environments (staging and production), **When** the same image is deployed with different `.ini` file sets, **Then** each environment runs with its own configuration independently.
3. **Given** a missing or invalid configuration file, **When** the container starts, **Then** the application fails fast with a clear error message indicating which configuration is missing.

---

### User Story 3 - Service Orchestration (Priority: P3)

A DevOps engineer deploys the full application stack — including the data pipeline worker, scheduler, and any supporting services — as a coordinated set of containers with defined health checks and dependency ordering.

**Why this priority**: Orchestration builds on the single-container deployment (US2) and adds multi-service coordination, which is needed for production but not for initial validation.

**Independent Test**: Start the full stack using the orchestration definition, verify all services pass health checks, and confirm that the scheduler triggers pipeline execution on the configured interval.

**Acceptance Scenarios**:

1. **Given** the orchestration definition file, **When** the operator starts the stack, **Then** all services (worker, scheduler, storage) start with correct dependency ordering and pass their health checks.
2. **Given** a running stack, **When** one service becomes unhealthy, **Then** the orchestration system detects the failure and reports it.
3. **Given** a production deployment, **When** the stack is running, **Then** application source code is NOT mounted from the host — all code is baked into the image.

---

### Edge Cases

- What happens when the container registry is unreachable during CI? The pipeline must fail with a clear error, and the previous image version must remain available for rollback.
- How does the system handle a deployment with an older image tag? The operator can pull any previously tagged image (retained for at least 2 release cycles) to roll back.
- What happens when the `.ini` file is modified while the container is running? Changes are not picked up until the container restarts — configuration is loaded at startup only.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST automatically build a container image on every merge to the `staging` and `production` branches.
- **FR-002**: System MUST tag each built image with both the git commit SHA and a semantic version number.
- **FR-003**: System MUST push built images to the designated container registry.
- **FR-004**: System MUST execute linting and unit tests on every push to `main`, `workers`, `staging`, and `production` branches.
- **FR-005**: Image builds MUST be blocked if linting or unit tests fail.
- **FR-006**: Container images MUST contain all application code and locked dependencies — no runtime code mounting.
- **FR-007**: Container images MUST be built using locked dependency manifests (not dynamic resolution).
- **FR-008**: Deployed containers MUST accept runtime configuration via mounted `.ini` files, compatible with the existing configuration parser with zero code changes.
- **FR-009**: Container processes MUST run as a non-root user.
- **FR-010**: Container images MUST NOT contain any secret files (`.env`, `.ini` credentials, virtual environments).
- **FR-011**: The build process MUST exclude sensitive files from the image context using an ignore file.
- **FR-012**: The full application stack MUST be deployable as coordinated services with health checks and dependency ordering.
- **FR-013**: Services MUST be separated by responsibility: application worker, scheduler, and data storage.
- **FR-014**: Previous image versions MUST be retained for at least 2 release cycles to support rollback.
- **FR-015**: Production branch deployments MUST pass all gates: linting, unit tests, image build, and pre-production smoke tests.
- **FR-016**: Container applications MUST emit structured logs in JSON format to stdout, including timestamp, log level, and module name fields.
- **FR-017**: Container images MUST be built from a base image pinned to a specific version tag; use of `:latest` is FORBIDDEN.
- **FR-018**: Containerfile MUST use multi-stage builds to minimize final image size.
- **FR-019**: Container images MUST conform to OCI image specification to ensure interoperability between Docker (build) and Podman (runtime).

### Key Entities

- **Container Image**: The immutable artifact containing application code, runtime, and locked dependencies. Identified by git SHA tag and semantic version tag.
- **Configuration Volume**: A set of `.ini` files mounted at runtime that define environment-specific parameters (API credentials, table IDs, connection strings). Not part of the image.
- **CI Pipeline**: The automated workflow triggered by git events that gates image production on passing linting, tests, and build steps.
- **Service Stack**: The orchestrated set of containers (worker, scheduler, storage) that constitute a complete deployment environment.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Code changes merged to `staging` produce a deployable image within 15 minutes.
- **SC-002**: The same image can be deployed to staging and production with only configuration file differences — zero image rebuilds.
- **SC-003**: Rollback to any previous version completes within 5 minutes by switching the image tag.
- **SC-004**: 100% of production deployments pass through all four gates (lint, test, build, smoke test) — no manual bypass is possible.
- **SC-005**: Container image contains zero secret files — verified by automated image scanning.
- **SC-006**: All container health checks report healthy status within 30 seconds of startup.
- **SC-007**: Container processes run exclusively as non-root users — verified in CI.
- **SC-008**: Container log output is valid JSON with required fields (timestamp, level, module) — verified by log format validation.
- **SC-009**: Containerfile base image uses a specific version tag — verified by CI linting of Containerfile.
- **SC-010**: Container images built in CI can be pulled and executed by Podman without modification — verified in staging deployment.

## Assumptions

- The container registry (GHCR) is already configured with appropriate access permissions for the CI system.
- Semantic version tags are managed externally (e.g., via git tags or a versioning tool) — the pipeline reads but does not compute the version.
- The existing configuration loading logic requires no changes to read from volume-mounted `.ini` files — the configuration parser reads from standard file paths.
- The CI environment (GitHub Actions) has Docker pre-installed and available for image construction.
- The host deployment environment uses **Podman** as the container runtime.
- CI builds use Docker; deployment hosts use Podman. Both engines share OCI image format compatibility — images built by Docker can be pulled and run by Podman without modification.
- The scheduler service is containerized separately from the application worker.
- Local development uses the same base image and build process as production to ensure parity.

## Technical Constraints

- Base image MUST be pinned to `python:3.12-slim` (or equivalent specific version tag). Use of `:latest` is FORBIDDEN.
- Containerfile MUST use multi-stage builds to minimize final image size (build stage for dependency installation, runtime stage with only necessary artifacts).
- Container images MUST conform to OCI image specification to ensure cross-engine compatibility between Docker (CI) and Podman (runtime).

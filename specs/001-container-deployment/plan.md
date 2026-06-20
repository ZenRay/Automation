# Implementation Plan: Container Deployment

**Branch**: `001-container-deployment` | **Date**: 2026-06-19 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-container-deployment/spec.md`

## Summary

为 Automation 项目构建基于 Docker/Podman 的容器化部署方案。CI 端使用 GitHub Actions + Docker 构建 OCI 标准镜像并推送至 GHCR；部署端使用 Podman（rootless）拉取运行。采用 COPY 模式打包不可变镜像，通过 volume 挂载 `.ini` 配置文件注入环境参数，零代码改动。分支策略 `main`/`workers`（lint + test）→ `staging`（build + deploy）→ `production`（build + smoke + deploy），全部门禁通过方可生产部署。

## Technical Context

**Language/Version**: Python 3.12 (`.python-version`: 3.12.13, `pyproject.toml`: `>=3.12`)

**Primary Dependencies**: pandas 3.0.3, numpy 2.4.6, pyodps 0.12.6, requests 2.34.2, requests-toolbelt 1.0.0, openpyxl 3.1.0 (via `requirements.lock.txt`, 12 packages total)

**Storage**: N/A (data stored in external MaxCompute and Lark services)

**Testing**: pytest (dev dependency), black --check, flake8 (linting)

**Target Platform**: Linux server (Podman rootless runtime), GitHub Actions (Docker build)

**Project Type**: Data pipeline worker (batch ETL, cron-driven execution)

**Performance Goals**: Image build < 15 min (SC-001), rollback < 5 min (SC-003), health check ready < 30s (SC-006)

**Constraints**: Non-root user, no secrets in image, OCI-compliant, multi-stage build, base image pinned to `python:3.12-slim`, dependencies locked via `requirements.lock.txt`

**Scale/Scope**: 2 worker modules (okr, cr_trail), 2 external APIs (Lark, MaxCompute), 3 deployment environments (dev/staging/production)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Requirement | Status | Notes |
|-----------|------------|--------|-------|
| I. Layer Isolation | `automation/` 不可被 workers 修改 | PASS | 部署方案不修改任何 `automation/` 代码 |
| II. Configuration-Driven | 运行时密钥通过环境变量或挂载配置注入 | PASS | `.ini` 文件通过 volume 挂载 |
| II. Configuration-Driven | 禁止硬编码 | PASS | 无新增代码逻辑 |
| V. Credential Security | `.containerignore`/`.dockerignore` 排除密钥文件 | PASS | 项目根目录 `.dockerignore` 排除 `.venv/`, `automation/conf/*.ini`, `.env` |
| V. Credential Security | 生产环境禁止 bind mount 源码 | PASS | compose.yml 仅挂载 `.ini` 配置卷 |
| V. Credential Security | 配置注入优先级：volume 挂载 > 环境变量 | PASS | 采用 volume 挂载方案 |
| V. Credential Security | 镜像密钥扫描验证 | PASS | CI build job 增加 `trivy` 扫描步骤（FR-020），自动检测镜像中的密钥文件 |
| V. Credential Security | 构建后文件验证 | PASS | CI build job 验证 `find /app -name "*.ini" -o -name ".env"` 为空（FR-021） |
| V. Credential Security | 容器日志脱敏 | PASS | 约定禁止日志打印 config 对象；JSON 日志过滤器推迟到后续 feature |
| V. Credential Security | 紧急镜像清理流程 | PASS | research.md 文档化凭据轮换 + 镜像删除流程 |
| VI. Containerization | 多阶段构建 | PASS | build stage + runtime stage |
| VI. Containerization | 基础镜像锁定版本 | PASS | `python:3.12-slim` |
| VI. Containerization | `requirements.lock.txt` 安装依赖 | PASS | 构建阶段 COPY + pip install |
| VI. Containerization | 健康检查 | PASS | Dockerfile HEALTHCHECK + compose healthcheck |
| VI. Containerization | 非 root 用户 | PASS | `appuser:appuser` (UID 10001) |
| VI. Containerization | Containerfile 命名 | **VIOLATION** | 使用 `Dockerfile` 命名，确保 CI 端 Docker 零配置识别 |
| VI. Containerization | `.containerignore` 命名 | **VIOLATION** | 使用 `.dockerignore` 命名，Docker 原生识别 |
| VII. CI/CD | push to main/workers 触发 lint + test | PASS | ci.yml 覆盖所有 4 个分支 |
| VII. CI/CD | 镜像双标签：git SHA + semver | PASS | deploy.yml 实现 |
| VII. CI/CD | 生产部署通过全部门禁 | PASS | lint → test → build → smoke 串行门禁 |
| VII. CI/CD | 回滚策略文档化 | PASS | deploy.sh 支持 tag 切换，保留 2 个版本 |
| VII. CI/CD | 镜像构建触发于 merge to main | PASS | 调整为 staging/production（更严格的环境分支模型） |

### Constitution Violations (Justified)

| Violation | Justification |
|-----------|--------------|
| `Dockerfile` 命名 (vs `Containerfile`) | GitHub Actions 预装 Docker 默认识别 `Dockerfile`；Podman 也完全兼容此命名。使用 `Containerfile` 需要在 CI 中额外配置 `-f Containerfile` 参数，增加不必要的复杂度。两个引擎的 OCI 兼容性不受文件名影响。 |
| `.dockerignore` 命名 (vs `.containerignore`) | Docker 默认读取 `.dockerignore`；Podman 也兼容此文件名。与 Dockerfile 命名保持一致，减少认知负担。 |
| 镜像构建触发于 staging/production (vs main) | Spec 澄清的环境分支模型比 `main` 直接构建更严格：`main` 仅运行 lint + test，镜像构建推迟到 `staging`/`production` 合并，增加了 staging 预验证环节。这是宪法原则 VII 精神的强化而非弱化。 |

## Project Structure

### Documentation (this feature)

```text
specs/001-container-deployment/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (deployment script contracts)
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
.dockerignore                        # 构建上下文排除规则（必须在项目根目录）
deploy/                              # 容器部署配置统一目录
├── Dockerfile                       # 多阶段镜像构建定义
├── entrypoint.sh                    # 容器入口脚本（非 root 用户启动）
├── compose.yml                      # 基础服务编排
├── compose.staging.yml              # staging 环境覆盖
├── compose.production.yml           # production 环境覆盖
└── scripts/
    ├── deploy.sh                    # 服务器端部署/回滚脚本
    └── scheduler.sh                 # 定时任务调度循环（shell loop）

.github/workflows/
├── ci.yml                           # lint + test（4 个分支触发）
└── deploy.yml                       # build + push GHCR + deploy（staging/production 触发）

# 现有文件（不修改）
automation/                          # 基础设施层（只读）
├── conf/                            # ConfigParser 读取 _lark.ini, _maxcomputer.ini
│   ├── __init__.py                  # 基于 __file__ 相对路径加载配置
│   ├── _lark.ini                    # 飞书凭据（被 .dockerignore 排除）
│   └── _maxcomputer.ini             # MaxCompute 凭据（被 .dockerignore 排除）
workers/                             # 应用层（只读）
├── cron_task.sh                     # 现有串行任务编排
├── okr/main.py                      # OKR 管道入口
└── cr_trail/main.py                 # CR Trail 管道入口

requirements.lock.txt                # 锁定的运行时依赖（12 packages）
pyproject.toml                       # 项目元数据 + dev 依赖
```

**Structure Decision**: 所有部署相关文件集中在 `deploy/` 目录下，CI 文件放在 `.github/workflows/`。不修改 `automation/` 或 `workers/` 下的任何现有代码。`dispatcher/` 是遗留 Airflow 栈，不在本 feature 范围内。

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| `Dockerfile` 命名 | CI 端 Docker 零配置识别 | `Containerfile` 需要 CI 中显式 `-f` 参数，Podman 兼容 `Dockerfile` 命名，无技术优势 |
| `.dockerignore` 命名 | 与 Dockerfile 命名一致 | `.containerignore` 仅 Podman 原生识别，Docker 不读取 |
| 构建触发于 staging/production | 环境分支模型增加预验证环节 | `main` 直接构建镜像缺少 staging 验证步骤，生产风险更高 |

# ============================================================
# Multi-stage Dockerfile for Automation worker
# Build context: project root (docker build -f deploy/Dockerfile .)
# ============================================================

# Stage 1: Build dependencies
FROM python:3.12-slim AS builder

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.lock.txt .
RUN pip install --no-cache-dir -r requirements.lock.txt

# Stage 2: Runtime
FROM python:3.12-slim

RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -m -s /bin/bash appuser

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai

WORKDIR /app

COPY automation/ automation/
COPY workers/ workers/
COPY pyproject.toml .
COPY deploy/entrypoint.sh deploy/entrypoint.sh
COPY deploy/scripts/scheduler.sh deploy/scripts/scheduler.sh

RUN chmod +x deploy/entrypoint.sh deploy/scripts/scheduler.sh && \
    ln -s /opt/venv /app/.venv && \
    chown -R appuser:appgroup /app

USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python --version && test -d /app/automation && test -d /app/workers || exit 1

ENTRYPOINT ["bash", "deploy/entrypoint.sh"]

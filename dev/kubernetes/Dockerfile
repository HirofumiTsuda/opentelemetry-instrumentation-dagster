# Build from the repo root: docker build -f dev/kubernetes/Dockerfile -t otel-instr-dagster-k8s-e2e:latest .
# (matches dagster-otel's own dev/kubernetes/Dockerfile convention)
FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml README.md /app/
COPY src /app/src
RUN pip install --no-cache-dir . dagster-postgres==0.29.22 dagster-k8s==0.29.22

COPY dev/kubernetes/workspace /workspace

# Baked into the image (not mounted via configmap) so that EVERY pod using this
# image -- the runner pod, and every step pod k8s_job_executor launches from the
# same job_image -- automatically has a working DagsterInstance config, without
# needing per-pod volume/configmap wiring for something this simple.
COPY dev/kubernetes/dagster_home /dagster-home
ENV DAGSTER_HOME=/dagster-home

# This is the entire point of this e2e test: PYTHONPATH set statically here, not
# via a per-command `opentelemetry-instrument` launcher wrapper (which only wraps
# whatever command it's put in front of -- k8s_job_executor constructs each step
# pod's own `dagster api execute_step ...` command internally, not something this
# Dockerfile or run_config.yaml controls). Baking it into the image instead means
# ANY Python process started in ANY container from this image -- the runner pod's
# `dagster job execute`, and every step pod's internal command, regardless of what
# it is -- auto-triggers opentelemetry-instrumentation's sitecustomize.py at
# interpreter startup, same mechanism `multiprocess`'s PYTHONPATH-inheritance
# relies on (dev/kubernetes's sibling doc, README.md's "Timing gets harder" section),
# just supplied statically here instead of propagated from a parent process (which
# doesn't happen between pods at all -- see README.md's k8s_job_executor section for
# why). Path is python:3.11-slim's own known site-packages location, not computed at
# build time -- pinned to this image's Python version; would need updating if the
# FROM line above ever changes.
ENV PYTHONPATH=/usr/local/lib/python3.11/site-packages/opentelemetry/instrumentation/auto_instrumentation

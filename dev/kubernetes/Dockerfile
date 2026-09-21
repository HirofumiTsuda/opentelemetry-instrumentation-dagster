# Build from the repo root: docker build -f dev/kubernetes/Dockerfile -t otel-instr-dagster-k8s-e2e:latest .
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# A venv at a path we choose (not wherever plain `pip install` happens to put things
# for this base image's Python) -- means the `find` below doesn't need to guess a
# Python-version-specific site-packages path, and stays correct even if the FROM
# line's Python version changes.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app
COPY pyproject.toml uv.lock README.md /app/
COPY src /app/src
COPY dev/kubernetes/pyproject.toml /app/dev/kubernetes/pyproject.toml
# hatchling needs workspace/ to already exist to build otel-instr-dagster-k8s-e2e's
# own (otherwise-empty-of-meaning) wheel -- see that pyproject.toml's own comment --
# so this has to land before `uv sync` runs, not after.
COPY dev/kubernetes/workspace /app/dev/kubernetes/workspace
COPY dev/kubernetes/workspace /workspace

# dev/kubernetes is its own uv workspace member (see its own pyproject.toml) --
# dagster-postgres/dagster-k8s (only needed for this real-cluster verification, not
# the package itself) are declared and locked there, not installed ad hoc.
RUN uv sync --frozen --package otel-instr-dagster-k8s-e2e

# Baked into the image (not mounted via configmap) so that EVERY pod using this
# image -- the runner pod, and every step pod k8s_job_executor launches from the
# same job_image -- automatically has a working DagsterInstance config, without
# needing per-pod volume/configmap wiring for something this simple.
COPY dev/kubernetes/dagster_home /dagster-home
ENV DAGSTER_HOME=/dagster-home
ENV PATH=/opt/venv/bin:$PATH

# This is the entire point of this e2e test: opentelemetry-instrumentation's
# sitecustomize.py made reachable statically, not via a per-command
# `opentelemetry-instrument` launcher wrapper (which only wraps whatever command
# it's put in front of -- k8s_job_executor constructs each step pod's own `dagster
# api execute_step ...` command internally, not something this Dockerfile or
# run_config.yaml controls). Python auto-imports any module literally named
# sitecustomize found directly in site-packages at interpreter startup -- so
# copying the real file there (found via `find`, not a hardcoded path, since the
# venv's own Python minor version isn't otherwise pinned here) is enough on its
# own, no PYTHONPATH or .pth file needed. Reaches EVERY container from this image --
# the runner pod's `dagster job execute`, and every step pod's internal command,
# regardless of what it is -- same mechanism `multiprocess`'s PYTHONPATH-inheritance
# relies on (dev/kubernetes's sibling doc, README.md's "Timing gets harder" section),
# just supplied statically here instead of propagated from a parent process (which
# doesn't happen between pods at all -- see README.md's k8s_job_executor section).
RUN site_packages="$(find /opt/venv -maxdepth 4 -type d -name site-packages)" && \
    cp "$site_packages/opentelemetry/instrumentation/auto_instrumentation/sitecustomize.py" \
       "$site_packages/sitecustomize.py"

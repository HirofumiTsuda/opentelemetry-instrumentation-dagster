# k8s_job_executor verification

Reproduces the k8s timing story documented in the top-level README.md's
"Timing gets harder with `multiprocess`/`k8s_job_executor`" section: real
trace context propagation across genuinely separate Kubernetes pods (one per
Dagster step, via `k8s_job_executor`), using a **static** approach (real
`sitecustomize.py` copied into site-packages' own root at image build time,
no `PYTHONPATH` involved at all) instead of the dynamic env-inheritance
`multiprocess` gets for free -- `k8s_job_executor` only forwards an
explicit, fixed allowlist of env vars into each step pod (checked
`dagster_k8s/executor.py`), and `PYTHONPATH` isn't among them.

Adapted from `dagster-otel`'s own `dev/kubernetes/` (same structure, same
Postgres/Jaeger/RBAC pattern) -- the one substantive difference is
`workspace/k8s_e2e_job.py` here has **zero** `@traced()` calls anywhere,
since proving that's unnecessary is the entire point of this package.

Kept as manifests, not wired into CI -- a full `kind` cluster spin-up is
heavy for every PR and this is a one-off verification, not a regression test
this package's logic needs re-run continuously. Rerun by hand if `dagster`/
`dagster-k8s`/`dagster-postgres` get bumped and this needs re-confirming
(`Dockerfile` builds its own `uv`-managed venv at a path it chooses, so
unlike a hardcoded `PYTHONPATH` this isn't pinned to any particular base
image's own site-packages layout -- see that file's own comments).

## Reproduce locally

```sh
kind create cluster --name otel-instr-dagster-e2e

docker build -f dev/kubernetes/Dockerfile -t otel-instr-dagster-k8s-e2e:latest .
kind load docker-image otel-instr-dagster-k8s-e2e:latest --name otel-instr-dagster-e2e

kubectl apply -f dev/kubernetes/manifests.yaml
kubectl wait --for=condition=available deployment/postgres deployment/jaeger --timeout=90s

kubectl apply -f dev/kubernetes/runner-pod.yaml
kubectl wait --for=condition=ready pod/dagster-runner --timeout=60s 2>/dev/null || true
kubectl logs -f dagster-runner   # wait for RUN_SUCCESS

# Confirm each step ran as its own separate Job/pod, not just a separate process:
kubectl get pods   # expect 2 "dagster-step-<hash>" pods, both Completed

# View traces:
kubectl port-forward svc/jaeger 16686:16686
# then open http://localhost:16686, service "k8s_e2e_test"
```

Teardown: `kind delete cluster --name otel-instr-dagster-e2e`.

## What each file is for

Same roles as `dagster-otel`'s own `dev/kubernetes/` -- see that project's
`dev/kubernetes/README.md` for the full per-file rationale (Postgres-backed
storage, why a `K8sRunLauncher` needs configuring even though nothing calls
`launch_run()` on it, the shared PVC, RBAC). `pyproject.toml` here is new --
a `uv` workspace member of the root project (`[tool.uv.workspace]` there),
declaring `dagster-postgres`/`dagster-k8s` as real, locked dependencies
instead of ad-hoc `pip install`s bolted onto the Dockerfile. The other
genuinely different thing: `Dockerfile`'s final `RUN`, which is this
verification's actual point -- see that file's own comments for why it
copies `sitecustomize.py` into site-packages directly rather than applying
it via a per-command `opentelemetry-instrument` wrapper (`k8s_job_executor`
constructs each step pod's own command internally; nothing here controls it
directly).

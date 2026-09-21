"""k8s_job_executor verification: each step in its own k8s pod, instead of
multiprocess's separate-processes-same-host (see tests/ -- that's already
verified against a real run). Deliberately NOT using @traced() anywhere in
this file, unlike dagster-otel's own equivalent
(dev/kubernetes/workspace/k8s_e2e_job.py) -- that's the entire point of this
package: zero code here, tracing applied entirely by the baked PYTHONPATH
(see ../Dockerfile)."""

from dagster import Definitions, OpExecutionContext, job, op
from dagster_k8s import k8s_job_executor


@op
def upstream_op(context: OpExecutionContext) -> int:
    return 1


@op
def downstream_op(context: OpExecutionContext, x: int) -> int:
    return x + 1


@job(executor_def=k8s_job_executor)
def k8s_e2e_job():
    downstream_op(upstream_op())


defs = Definitions(jobs=[k8s_e2e_job])

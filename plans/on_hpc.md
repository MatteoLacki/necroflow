# Necroflow on HPC: containers and resource scheduling

Status: discussion summary; not implemented by this document.
Date: 2026-10-06.

## Scope and current implementation

This plan concerns Necroflow. Nextflow already supports Podman, Apptainer, and
Singularity.

Necroflow currently supports native Docker execution on local Linux, with
digest-pinned images, explicit platform selection, automatic input/workdir
mounts, and the `{env}:` command prefix. Container configuration participates in
provenance and cache identity. Logs, retries, failures, and output validation use
the ordinary rule execution path.

Relevant implementation:

- `src/necroflow/containers.py`: Docker settings, prefix selection, launch argv.
- `src/necroflow/rules.py`: Docker-typed input detection and container capability.
- `src/necroflow/rule_call.py`: command resolution and execution dispatch.
- `tests/test_containers.py`: unit coverage and opt-in real Docker execution.
- `plans/native-container-execution.md`: existing Docker scope and semantics.

## Adding Podman, Apptainer, and Singularity

Estimated effort for one developer familiar with the codebase, with working test
runtimes available: **4–8 developer-days** for support matching the current local
Docker scope. These are planning estimates, not measured implementation times.

| Work | Estimate |
| --- | --- |
| Generalize container input selection and dispatch | 0.5–1 day |
| Podman backend and tests | 0.5–1.5 days |
| Apptainer backend and tests | 2–3 days |
| Singularity compatibility, integration checks, documentation | 1–2 days |

Preserve `{env}:` syntax. Share container selection and mount planning where
semantics match; keep runtime-specific launch builders and defaults small.
Scheduling and existing execution bookkeeping should remain largely reusable.

### Podman

Podman is the smaller addition because much of the Docker command line can be
reused. Rootless execution needs deliberate UID/GID mapping and output ownership
checks; `--userns=keep-id` is a candidate policy, not a verified choice yet.
Validate mount permissions, SELinux behavior, and cancellation on real runtimes.

Podman can also act as a local client for a remote Podman service, commonly over
SSH. In that mode bind-mount source paths refer to the remote host. Local inputs
are not automatically transferred, and outputs remain remote. Supporting this
requires shared storage or explicit staging and collection.

Running Necroflow and Podman together on a compute server is ordinary local
execution, even when the user accesses that server over SSH.

### Apptainer and Singularity

These need a separate launch builder based on `exec ... /bin/sh -c`. Apptainer
accepts Docker/OCI images, but its defaults differ from Docker. Design and test:

- Read-only input mounts, writable workdir, symlink behavior, and path handling.
- Host environment inheritance and automatic host mounts.
- Digest-pinned OCI image conversion to SIF, caching, and concurrent first use.
- Architecture selection and supported runtime versions.
- Compatibility between the selected Apptainer and Singularity versions.

Local SIF support, if added, needs an explicit content identity policy; a mutable
file path alone is insufficient for reproducible caching.

### Identity and verification

Current container settings fingerprint as mappings. Runtime identity must enter
provenance explicitly; distinct Python class names alone will not distinguish
otherwise identical settings. Preserve existing Docker cache identities where
its launch semantics remain unchanged.

Verify real execution for read-only inputs, host-visible user-owned outputs,
working directory, failures, cache reuse, and interruption. Unit tests alone
cannot establish runtime parity.

The estimate excludes general remote execution, broad HPC-site compatibility,
GPU integration, and per-rule Slurm submission. Suggested sequence: Podman first,
then Apptainer and a defined Singularity compatibility target.

## Slurm and Necroflow have complementary roles

| Component | Responsibility |
| --- | --- |
| Necroflow | Dependencies, caching, ready-task ordering, workflow resource accounting |
| Slurm | Allocate cluster resources across users; queue, launch, monitor, and constrain jobs |
| NFS or another shared filesystem | Make files accessible across machines |

NFS provides file access, not remote execution or resource reservation. Reading
shared files does not automatically stage a task onto local disk. Node-local
scratch requires explicit input staging and output copy-back.

Necroflow's scheduler hook selects/orders ready calls. The executor handles
admission and the runner executes calls. The default runner launches locally;
multi-node placement requires additional execution infrastructure, not merely a
different ready-task ordering function.

## Podman under Slurm

Two deployment models are possible:

1. **Podman inside an allocated job.** Submit through `sbatch` or `srun`, then
   execute `podman run` on the compute node. This is the simpler initial model.
   Podman must retain container processes within Slurm's cgroup hierarchy so
   resource limits remain effective; default rootless configuration needs review.
2. **Podman through `scrun`.** Configure Slurm's OCI runtime proxy as Podman's
   runtime, so container execution is scheduled through Slurm. This requires site
   integration, including `oci.conf`, `scrun.lua`, and storage configuration.

Both depend on site support for rootless containers, including user namespaces,
an appropriate UID/GID mapping configuration, and suitable container storage.

For an initial Necroflow deployment, request **one Slurm allocation and let
Necroflow orchestrate within it**. Set Necroflow's resource caps explicitly to the
allocation. Submitting each rule as its own Slurm job is a separate integration.
Allocating multiple nodes does not automatically distribute the current runner's
tasks across them.

## Dedicated VMs without Slurm

Slurm is optional on dedicated resources. In principle, Necroflow can place ready
tasks across VMs given per-worker CPU/RAM/GPU budgets and remote execution.
Required additions include worker selection, per-worker capacity accounting,
dispatch, cancellation, and worker-loss recovery. Shared storage can simplify
data access but does not implement these functions.

The central distinction is **tracking capacity versus reserving and enforcing
it**:

- With dedicated VMs and one coordinator, internal reservations can usually
  manage task concurrency; leave capacity for the OS and services.
- Multiple independent coordinators need shared reservations or disjoint resource
  partitions to avoid claiming the same capacity.
- Checking currently free CPU or RAM does not reserve it against unrelated
  processes. Resource partitions and OS-enforced limits are needed where those
  workloads coexist.
- CPU contention generally slows work; memory contention can cause OOM kills.
  Containers alone do not reserve resources. Configured cgroups can enforce
  limits, while placement/accounting decides which tasks receive capacity.

For dedicated worker VMs, a lightweight distributed Necroflow executor is a
viable direction. On shared institutional HPC, use the site's resource allocation
system and retain Necroflow for workflow orchestration.

## References

- [Nextflow container support](https://docs.seqera.io/nextflow/container)
- [Podman run reference](https://docs.podman.io/en/latest/markdown/podman-run.1.html)
- [Podman rootless setup](https://github.com/podman-container-tools/podman/blob/main/docs/tutorials/rootless_tutorial.md)
- [Podman remote client](https://github.com/podman-container-tools/podman/blob/main/docs/tutorials/remote_client.md)
- [Apptainer Docker/OCI support](https://apptainer.org/docs/user/latest/docker_and_oci.html)
- [Slurm overview](https://slurm.schedmd.com/overview.html)
- [Slurm containers guide](https://slurm.schedmd.com/containers.html)
- [Slurm scrun reference](https://slurm.schedmd.com/scrun.html)

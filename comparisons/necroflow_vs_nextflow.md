# Necroflow vs Nextflow: containerisation and reproducibility

Exploratory notes, 2026-09-15. Nothing here is implemented or decided; this
records the reasoning so it does not have to be re-derived.

The starting question was narrow — "can a containerised pipeline call tools that
live in other containers?" — and the useful answer turned out to be that the
question mixes two separate problems that Nextflow deliberately keeps apart.

## Nesting containers is the wrong axis

Docker has a daemon, so "container calls container" is an IPC problem:
mount `/var/run/docker.sock`, install the Docker CLI, and the spawned container
is a *sibling* on the host. This is Docker-outside-of-Docker. It works, with
three standing traps: `-v` paths are resolved by the **host** daemon (so a path
that exists only inside the caller silently becomes an empty auto-created dir),
outputs land root-owned without `--user`, and socket access is equivalent to
root on the host.

Apptainer has **no daemon**. There is nothing to talk to, so the same request
means literally running `apptainer` inside `apptainer` — a nested-namespace
problem rather than an IPC one. It is possible on Apptainer 1.1+ in unprivileged
userns mode, but every one of these must hold:

- outer container in userns mode, not setuid mode (`no_new_privs` is set, so the
  inner setuid helper is ignored and setuid nesting fails outright)
- `/dev/fuse` bound into the outer container — without a loop device, which is
  unavailable unprivileged, the inner SIF mounts via squashfuse
- inner `.sif` files on a host bind mount, not inside the outer image; nested
  FUSE-over-FUSE is the fragile part
- `APPTAINER_TMPDIR` / `APPTAINER_CACHEDIR` pointed at writable bound dirs, since
  the outer SIF root is read-only
- unprivileged user namespaces enabled in the kernel at all (some hardened
  distros disable them, and then nothing works)

Fragile, and it buys nothing over the alternative.

## What Nextflow actually does

No workflow engine nests. Nextflow, Snakemake and Cromwell all converge on:

> Engine on the host. One container per task. The engine emits
> `apptainer exec <sif> <cmd>` as the task's command line.

When the Nextflow head job *is* containerised — AWS Batch, `nextflow kuberun` —
it still never nests, because it does not create containers. It submits to an
external scheduler:

| Platform | Head job | Tasks |
|---|---|---|
| AWS Batch | container | separate Batch jobs |
| Kubernetes | head pod | task pods via the K8s API |
| Slurm + Apptainer | login node or Slurm job, bare metal | `srun apptainer exec img.sif cmd` |

That is what the executor abstraction buys. Container creation always belongs to
whoever already holds the privilege — the host, Batch, or kubelet. The nesting
question never arises because the orchestrator was never the thing entitled to
spawn. Note the Slurm row in particular: with Apptainer, the standard nf-core
HPC config keeps the head job off containers entirely.

## The asymmetry: orchestrator reproducibility is not containerisation

Nextflow pins itself three ways, none of them an image:

- `NXF_VER` — the launcher is a self-bootstrapping script that fetches the exact
  requested version's JAR
- `-r <revision>` — pipeline code is a git repo pinned by tag or commit
- plugin pins in `nextflow.config`, e.g. `plugins { id 'nf-amazon@2.1.4' }`

Telling detail: nf-core declares `manifest.nextflowVersion = '>=23.04.0'`, a
**range**. They are saying openly that the orchestrator's exact version is not
part of the scientific result.

The justification is that the orchestrator produces no scientific output. It
builds a DAG, computes hashes, and submits command lines; a patch-version
difference yields bit-identical results. With one exception, and it is the one
that bites Nextflow: **hashing semantics leak**. Change how task hashes are
computed and every cache entry invalidates, so `-resume` across Nextflow versions
silently re-runs everything. `NXF_VER` is doing double duty — a version pin used
as a proxy for a hash-semantics pin.

So the requirement splits three ways:

- **tools** — containers, content-addressed
- **engine** — version pin, plus an explicit pin on whatever computes rule hashes
- **container spawning** — whoever already has the privilege

Nested Apptainer is what you get from skipping that split and trying to solve one
problem with one hammer.

## Container identity in the cache key

Nextflow's task hash is built from the session id (skipped on `-resume`), the
process script body, input values and files, `conda`/`spack`/`module`/`arch`, and
the container image reference when containers are enabled. The hash names the
task work dir (`work/ab/cdef12...`); a hit on `-resume` means skip.

The weakness: it hashes the image **reference string**, not the image
**content**. Repush `biocontainers/samtools:1.19` with a different build and the
string is unchanged, so the hash is unchanged, so Nextflow resumes from stale
results. `:latest` is the pathological case. Mitigations, ascending:

- **digest pinning** — `quay.io/biocontainers/samtools@sha256:9c2b50e7...` makes
  the reference itself content-addressed. Works, but it is a convention, and a
  `:1.19` slipping in silently reverts to the broken behaviour.
- **Wave** — Seqera's provisioner builds images on demand and returns a
  digest-pinned reference, so content-addressing becomes a property of the system
  rather than a discipline.

Local `.sif` is the easy mode here: one file, so content hashing is a
`sha256sum`, with no registry manifests to chase and no mutable-tag concept to
begin with. Cached on `(path, mtime, size)` the cost disappears. A necroflow
implementation would land closer to Wave's guarantee than to stock Nextflow's,
for less work.

Snakemake exposes the same idea as a `software-env` rerun trigger
(`--rerun-triggers`), covering conda env content and container URI — user-
switchable, which is arguably the wrong default.

## Where necroflow already stands

Findings from reading the current tree:

- **The engine is pure Python with almost no dependencies.** `necroflow` depends
  on `tomlkit`; `ionmaidentools` on `tomlkit` and `dictodot`. No compiled
  extension anywhere, so the engine has zero ABI surface — nothing to match
  against a glibc, CPU arch or CUDA version. A wheel plus a lockfile is the
  entire reproducibility story, and containerising it would buy nothing a
  `uv.lock` does not. Lighter than Nextflow's position: no JVM, no plugins.
- **The only unpinned piece is the interpreter.** `requires-python = ">=3.11"` is
  a range, the same shrug as nf-core's `nextflowVersion`. That is the sole honest
  argument for ever imaging the engine, and it is weak — DAG construction and
  SHA-256 over an identity payload are not where 3.11-vs-3.13 behaviour drifts.
  `uv python pin` is the cheaper fix.
- **Hash semantics are already versioned independently.**
  `src/necroflow/fingerprints.py:19` defines
  `RULE_HASH_DOMAIN = f"necroflow.rule-hash/{IDENTITY_FORMAT}"`. This is strictly
  better than `NXF_VER`: Nextflow pins hash semantics only as a side effect of
  pinning the binary, so every patch bump risks a cache wipe, whereas
  `IDENTITY_FORMAT` is an explicit knob that only moves when the identity schema
  actually changes.
- **There is already a command-wrapping seam.** `RuleCall.run`
  (`src/necroflow/rule_call.py:216`) executes every rule as
  `subprocess.run(command, shell=True, executable=shellpath)`, and `--shellpath`
  is an existing CLI flag (`src/necroflow/cli.py:1041`) validated as an
  executable file. A wrapper script that execs `apptainer exec <sif> /bin/bash
  "$@"` routes every rule command into a single image with no code change at all.
- **Rule identity does not cover the environment.** `_rule_identity`
  (`src/necroflow/fingerprints.py:251`) hashes rule name, command identity,
  mutability, and declared input/output types; `_command_identity`
  (`:233`) returns `{"kind": "shell", "template": command}` for shell rules.
  Nothing about the execution environment. That is honest today, because the
  environment is the repo tree pinned globally by the caller's snapshot, but it
  stops being honest the moment rules carry their own containers.
- **`nodes_dir` is relative by default** (`src/necroflow/cli.py:200`,
  `Path("nodes")`), and no `cwd` is passed to `subprocess.run`, so commands are
  resolved against the launch directory. Any containerisation scheme has to
  decide whether tool paths and node paths share a root — they want different
  ones once tools live in an image.

## What a per-rule container feature would need

Two changes; the second is the one that matters.

1. **Wrapping.** A `container=` attribute on `Rule`, and `RuleCall.run` wrapping
   the resolved command as
   `apptainer exec {binds} {sif} /bin/bash -c {shlex.quote(command)}`.
   Mechanically easy. Exit codes, stdout/stderr redirection into the job log, and
   signal forwarding all pass through `apptainer exec` unchanged, so the
   executor's interrupt handling keeps working.
2. **Invalidation.** `_command_identity` needs a `container` key carrying the
   SIF's **content hash**, not its path or tag. Without it, swapping
   `sage-1.2.sif` for `sage-1.3.sif` leaves every downstream node "up to date"
   with output from the old binary — silently wrong results, the one failure mode
   a cache-based engine must never have.

The same conclusion holds for Docker: the transport differs, the caching hazard
does not.

A third, non-obvious cost: pipelines that invoke tools by repo-relative path
(e.g. `venvs/common/bin/some_tool {in} {out}`) must move to bare `$PATH` lookups
resolved inside the image, which is how Nextflow process bodies work and what
makes the container swappable. That changes every command template, hence every
rule hash, hence one full cache invalidation on the switch. Unavoidable and
correct — the hash *should* change when the execution environment becomes a
different thing — but it is a single big rebuild, not a gradual migration.

## Confidence

The necroflow observations are read directly from the tree at the commit this
file was added, with file and line references above.

The Nextflow claims — key-list composition of the task hash, reference-versus-
content gap, executor delegation, `NXF_VER`/`-r`/plugin pinning — are stated from
knowledge of the system, not verified against Nextflow source in this session.
The shape is solid; exact current method names in `TaskProcessor` were not
checked.

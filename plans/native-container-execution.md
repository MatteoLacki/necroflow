# Native Docker execution through a `Docker` config value

Status: implemented (Docker only).
Updated: 2026-09-16.

## Objective and scope

Run selected rule commands inside Docker with a plain `dag.run()`, without a custom
`rule_call_runner`. Orchestration stays host Python. The image is ordinary rule
config supplied from the job TOML, not a Node.

In scope: Docker on local Linux, digest-pinned OCI references, explicit platform,
string command templates and Python callbacks, overridable `docker run` arguments.

Out of scope for v1: Podman, Apptainer, image Nodes/descriptors, image-build
helpers, extra user mounts, GPU, remote engines, cluster submission, doctor/graph/provenance diagnostics beyond
what already exists, recording the installed Docker version.

Migration and acceptance fixture: `experiments/rnaseq_containers/`.

## Public interface

```toml
# job.toml
reference = "data/ggal_1_48850000_49020000.Ggal71.500bpflank.fa"
reads_1   = "data/ggal_gut_1.fq"
reads_2   = "data/ggal_gut_2.fq"

[images.salmon]
image    = "quay.io/biocontainers/salmon@sha256:abc…"
platform = "linux/amd64"

[images.fastqc]
image    = "quay.io/biocontainers/fastqc@sha256:def…"
platform = "linux/amd64"
run_args = ["--rm", "--init", "--pull=missing", "--user={uid}:{gid}",
            "--env=HOME=/tmp", "--network=none", "--memory=2g"]
```

```python
from necroflow import Docker, Pipeline, command, output

@command("{env}:salmon index --threads {threads} -t {fasta} -i {index}", threads=1, ram="2Gi")
def index(fasta: Reference, env: Docker):
    index = output(Index)
    return index

@command("{env}:salmon quant --threads {threads} -i {index} -1 {r1} -2 {r2} -o {quant}", threads=2)
def quant(index: Index, r1: Read1, r2: Read2, env: Docker):
    quant = output(Quant)
    return quant

@command("{env}:fastqc --threads {threads} -o {fastqc} {r1} {r2}", threads=1)
def fastqc(r1: Read1, r2: Read2, env: Docker):
    fastqc = output(FastQC)
    return fastqc

def pipeline(p: Pipeline, cfg):
    salmon = Docker(**cfg["images"]["salmon"])
    fastqc_env = Docker(**cfg["images"]["fastqc"])

    p.fasta = reference(p, cfg["reference"])   # host rule, no prefix
    p.r1 = read1(p, cfg["reads_1"])
    p.r2 = read2(p, cfg["reads_2"])
    p.index = index(p, p.fasta, env=salmon)
    p.quant = quant(p, p.index, p.r1, p.r2, env=salmon)
    p.fastqc = fastqc(p, p.r1, p.r2, env=fastqc_env)
```

## `Docker` value

- Small immutable class implementing `Mapping` with keys `image`, `platform`,
  `run_args`. The existing fingerprinter canonicalises Mappings, so no
  fingerprinting change is needed for the value itself.
- Constructor validates: `image` contains `@sha256:`; `platform` is non-empty;
  `run_args` is a sequence of strings. It performs no Docker or network calls.
- `run_args` is normalised to a `tuple` (TOML yields lists; the fingerprinter
  frames lists and tuples differently, so normalising keeps Python and TOML
  spellings identical).
- Default:

  ```python
  DEFAULT_RUN_ARGS = ("--rm", "--init", "--pull=missing",
                      "--user={uid}:{gid}", "--env=HOME=/tmp")
  ```

  Supplying `run_args` replaces the default entirely. No merge/extend syntax.
- `{uid}` and `{gid}` are the only placeholders in `run_args`. They are expanded at
  launch from `os.getuid()`/`os.getgid()`. The unexpanded strings are what gets
  hashed, so the same pipeline run by different users shares identities.
- `--pull=missing` pulls only when the pinned image is absent from the local Docker
  image store. Docker's default is the same; it is kept explicit so it is visible
  and hashed. Users wanting an offline prepare step set `--pull=never`.

## Command compilation

- At rule declaration, a string template whose first non-whitespace token is
  `{name}:` is a container command **only if** `name` is an input annotated
  exactly `Docker`. Otherwise the template is left untouched (existing templates
  starting with `{x}:` keep working).
- Strip the prefix before normal placeholder substitution. The remainder must be
  non-empty. The prefix applies to the whole shell command, including pipes and
  redirections.
- A `Docker` input without a prefix is ordinary config and does not switch
  execution.
- Python command callbacks select Docker by returning the literal prefix
  (`return "{env}:" + body`). `resolve()` strips it; the remainder is not
  formatted again and must be non-empty. Without the prefix the callback runs on
  the host.
- `call.resolve()` returns the body and records the selected input;
  `call.container` returns the `Docker` value, so the runner and diagnostics agree.

## Identity and freshness

- `env` is keyword config, so image, platform, and `run_args` enter each consumer's
  provenance hash and output paths, and propagate to descendants through normal
  parent lineage. Changing the digest, platform, or any run arg yields new paths;
  switching back reuses the old ones.
- The rule hash covers the template, including the prefix.
- Add a `CONTAINER_POLICY` version to the provenance payload of container-capable
  rules only: a prefixed template, or a callback with a `Docker` input (its prefix
  is only known once it runs).
  Bump it whenever framework-owned launch semantics change (fixed argv, mounts,
  shell). Host-only rules' fingerprints stay byte-identical.
- No new freshness machinery: consumed-hash classification of Node inputs works as
  today. Removing a local Docker image does not invalidate anything; the next real
  execution pulls again (under the default `--pull=missing`).

## Execution

Inside the standard RuleCall runner, for prefixed rules:

```text
docker run
  <run_args with {uid}/{gid} expanded>
  --platform=<platform>
  --mount type=bind,src=<resolved input>,dst=<input absolute path>,readonly   (per input)
  --mount type=bind,src=<workdir>,dst=<workdir>
  --workdir=<workdir>
  --entrypoint=/bin/sh
  <image> -c <resolved command body>
```

- Everything after `run_args` is framework-owned and not overridable; dropping it
  would break output validation and path identity.
- Inputs: every Node input (fixed and variadic) is bound read-only at its original
  absolute path, sourced from its resolved top-level path. A top-level symlink to
  `/external/reads.fq` therefore works without exposing `/external`. Directory
  inputs containing symlinks pointing outside the directory are unsupported
  (documented; the container fails loudly).
- Deduplicate identical mounts. Reject paths containing `,`, `"`, or newlines
  (Docker parses `--mount` as CSV) with a clear error. Spaces are fine because argv is a list.
- Workdir is created on the host first and bound writable; all co-outputs live there.
- Plain config values never create mounts. Shell text is never parsed for paths.
- Container shell is `/bin/sh -c`. `Pipeline.shellpath` remains host-only; rules
  needing Bash call it explicitly.
- Logs, retries (`repeat`), nonzero-exit failure, and missing-output checks reuse
  the existing runner path.
- Cancellation: no new machinery. Like host jobs, the `docker run` client receives
  the terminal's SIGINT; with the default `--rm --init` and Docker's signal
  proxying, the container stops and is removed. Users who drop `--rm` own the
  leftovers. (Not covered by an automated test.)
- Fully cached calls launch nothing and pull nothing.
- `rule_call_runner` remains an override that owns whatever it replaces.

## Implementation sequence

1. `Docker` value, exported from `necroflow`.
2. Prefix detection at rule declaration (decorator and factory forms), prefix
   stripping in `RuleCall.resolve()`, `CONTAINER_POLICY` in provenance for prefixed
   rules.
3. Docker argv builder and launch in the standard runner.
4. Migrate `experiments/rnaseq_containers/`: images move to its `job.toml` (per-image
   pins plus one shared `[docker]` table with platform and run args), rules take
   `env: Docker`, callbacks return the prefix and start with `set -eu` (the
   container shell is `/bin/sh`), delete `containers.py`, `images.json`,
   `container_policy` config, and the `rule_call_runner=` arguments.
5. Docs: `docs/rules.md`, `docs/executor.md`, `docs/caching.md`, `features.txt`,
   `AI.md`, and `docs/rule-call-lifecycle.md` in the same change as step 2.

## Verification

Offline (no Docker needed):

- `Docker` validation: missing digest, missing platform, list/tuple `run_args`
  hash identically, default applied when omitted.
- Prefix: detected only for `Docker`-typed inputs; non-`Docker` `{x}:` templates
  unchanged; empty body rejected; no-prefix `Docker` input runs on host.
- Identity: digest/platform/run_args changes alter consumer and descendant paths;
  switching back reuses paths; host-only fingerprints unchanged; `{uid}` hashed
  unexpanded.
- Argv builder: mounts for fixed, variadic, symlinked inputs; dedup; comma
  rejection; spaces; framework args after `run_args`.

Opt-in Docker integration (report skipped, never count as passed):

- Input read-only, outputs host-visible and user-owned, cwd, shell redirection,
  nonzero exit, missing output, interruption leaves no container.
- `docker rmi` then forced rerun pulls again.
- RNA-seq parity against the pinned Nextflow baseline, unchanged-run caching, new
  lineage after image change.

Ordinary unit tests must not require Docker or download images/data.

## References

- Existing experiment: `experiments/rnaseq_containers/README.md`.
- [docker run reference](https://docs.docker.com/reference/cli/docker/container/run/).

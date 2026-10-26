# Incus storage power-loss test environment

This infrastructure records ext4 writes using the host's `dm-log-writes` target,
replays selected prefixes onto fresh baseline copies, and runs Necroflow recovery
checks in an unprivileged sibling container. It does not change Necroflow itself.

## Install on the Incus host

The installer needs real host root access for loop devices, device mapper, mounts,
and its dedicated SSH listener. Run from this repository on the host:

```bash
sudo /usr/bin/python3 experiments/incus_powerloss/setup.py install
```

Defaults match the inspected host: development instance `aiwork` in project
`user-1000`, existing pool `default`, existing network `incusbr-1000`, host bridge
address `10.64.249.1`. Use `setup.py --help` for overrides. Installation refuses
existing resources with this environment's names. Interrupted installations leave
their receipt at `/etc/necroflow-powerloss/config.json`. Once the marked project
and container have been created, continue an interrupted setup on the host:

```bash
sudo /usr/bin/python3 experiments/incus_powerloss/setup.py resume
```

Resume retains the marked instance and project, repeats provisioning operations,
reuses existing keys and the Python environment, then runs the SSH smoke test.
It takes its settings from the root-owned receipt. Ownership checks prevent it
from adopting other resources. Setup and teardown exclude concurrent installers;
resume also refuses an active smoke run before stopping its own SSH listener.

The receipt records each command before launching it and its exit code afterward.
Both stdout and stderr are retained in `/etc/necroflow-powerloss/setup.log`;
failures also set `stage: failed` and record the error in the receipt. These files
remain outside the crash filesystem and can be read while setup is running.

Setup creates project `necroflow-powerloss` and instance `necroflow-crash-test`
from `images:ubuntu/24.04`, with **2 CPUs, 4 GiB RAM** and no inherited profiles or
host repository mounts. The exact image fingerprint, installed Python 3.14 and uv
versions, replay-tool revision, snapshot checksum, and limits are saved in the
receipt and each run report. Root disks receive a 12 GiB quota on quota-capable
pools; directory pools retain their existing root storage policy. The crash-test
filesystem is always bounded to **256 MiB** regardless of pool driver.

Setup snapshots tracked and untracked nonignored files directly from the
development working tree, preserving its uncommitted changes. Code and its
development environment are installed under `/opt/necroflow`, outside the
simulated crash filesystem. Run setup again after teardown to refresh that copy.

The replay tool is built from the kernel-documented upstream at commit
`7b70d8a6863c5de30933d42a7672d35d01d2dc6c`. Installation loads the matching
`dm-log-writes` module from the running kernel. Unsupported kernels stop setup.

Setup finishes by executing the **actual SSH preflight and storage smoke test**
from development. Only a receipt with `stage: verified` and a run report with
`verified: true` establishes that setup worked. See [STATUS.md](STATUS.md) for
what has actually been verified in this checkout.

## Control from development

The client private key is generated inside `aiwork` at
`/home/matteo/.ssh/necroflow-powerloss_ed25519` and never copied out. The host key
is pinned in `/home/matteo/.ssh/necroflow-powerloss_known_hosts`. From development:

```bash
ssh -i ~/.ssh/necroflow-powerloss_ed25519 -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=~/.ssh/necroflow-powerloss_known_hosts -p 22222 nf-powerloss@10.64.249.1 preflight

ssh -i ~/.ssh/necroflow-powerloss_ed25519 -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=~/.ssh/necroflow-powerloss_known_hosts -p 22222 nf-powerloss@10.64.249.1 smoke trial-1

ssh -i ~/.ssh/necroflow-powerloss_ed25519 -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=~/.ssh/necroflow-powerloss_known_hosts -p 22222 nf-powerloss@10.64.249.1 report trial-1

ssh -i ~/.ssh/necroflow-powerloss_ed25519 -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=~/.ssh/necroflow-powerloss_known_hosts -p 22222 nf-powerloss@10.64.249.1 cleanup trial-1
```

The receipt contains the exact SSH command if defaults were overridden. Allowed
operations are `preflight`, `smoke RUN_ID`, `report RUN_ID`, `cleanup RUN_ID`.
Run IDs match `[a-z0-9][a-z0-9-]{0,31}`. All work is serialized by a host lock.
SSH accepts no arbitrary shell commands, paths, forwarding, or terminal access.
The dedicated account's sole sudo permission is the root-owned runner with no
process arguments. No Incus socket or API is exposed to development.

## Layout and smoke checks

| Location | Contents |
| --- | --- |
| Host `/var/lib/necroflow-powerloss/runs/RUN_ID/` | Baseline, original recording image, write log, raw replay images, recovery copies, report |
| Container `/opt/necroflow/` | Separate source snapshot, Python environment, workload jobs and command logs |
| Container `/crash-test/nodes/` | Necroflow node store on the recorded/recovered ext4 filesystem |
| Container `/crash-test/results/` | Necroflow copied results on the same filesystem |
| Host `/etc/necroflow-powerloss/` | Setup receipt, SSH listener config, host key, authorized public key |
| Host `/usr/local/libexec/necroflow-powerloss/` | Root-owned fixed runner and replay binary |

Each smoke run starts with a freshly formatted, cleanly unmounted ext4 baseline.
Only exclusive files created beneath the runner's own directory become backing
devices. The baseline's loop device is detached before it is copied.

1. Run the `durable` pipeline, fsync a known file and its directory, then complete
   `syncfs` before marking the write log `durable`.
2. Run the `completed` pipeline and create a later file without adding a durability
   barrier. Save successful completion observations outside the crash filesystem,
   then mark the log `completed`.
3. Detach the container mount, unmount the recording filesystem, remove its mapper,
   and detach its loops to finalize the write log. Unmount writes occur after the
   selected marks and are excluded from both replays.
4. For each mark, copy the original baseline into a fresh image and replay through
   that mark. Confirm the mark exists first: upstream replay does not itself fail
   when a requested end mark is absent.
5. Preserve that raw image. Copy it again, recover the ext4 journal with `e2fsck -p`,
   attach it at `/crash-test`, inspect bytes and states, then rerun the corresponding
   Necroflow job. Requested node/result paths come from the live CLI's
   `outputs --json`; expected bytes use SHA-256 independently of `.rip` hashes.

The earlier replay must retain the flushed file and result and exclude the later
file. The completion replay reports pre-recovery contents without asserting that
observed completion was durable. Both jobs must produce correct bytes after
recovery. Any failed assertion leaves a failed report and retained images.

At most four runs are retained. Each uses seven bounded image files: a 256 MiB
baseline, recording image, two raw replay images, two recovery copies, and a 1 GiB
write log (2.5 GiB logical maximum per run; sparse/reflink usage may be lower).
Clean up a retained run before starting a fifth. No image is replayed while
attached. Cleanup refuses unrelated mounts, loop backing files, or mapper tables.

## Limits

This exercises two selected prefixes of the writes and completed flush/FUA
boundaries recorded by `dm-log-writes`; it does not enumerate every possible
device write reorder, torn sector, hardware cache failure, or crash timing. A
userspace mark is an observation, not a storage barrier. The durable mark is
preceded by an explicit barrier. Later unflushed writes may be absent or present
at the completion mark. Journal recovery is part of filesystem recovery and is
reported separately from the preserved raw replay image.

Process exit, container stop, and container restart are not used to simulate
power loss. The checks make no claim that current Necroflow completion guarantees
storage durability. Only the mounted scratch filesystem is crash-tested; the
host, container root, source, expected values, and reports remain intact.

## Local verification without root

```bash
git clone https://github.com/josefbacik/log-writes.git /tmp/nf-log-writes
git -C /tmp/nf-log-writes checkout --detach 7b70d8a6863c5de30933d42a7672d35d01d2dc6c
make -C /tmp/nf-log-writes
NF_REPLAY_LOG=/tmp/nf-log-writes/replay-log .venv/bin/python -m pytest -o addopts= experiments/incus_powerloss/test_infrastructure.py -q
.venv/bin/python experiments/incus_powerloss/runner.py preflight
```

The tests exercise request/path/device protections, the current real Necroflow
CLI workload and recovery of missing outputs, and the actual upstream replay
binary with a synthetic v1 log containing flush/FUA entries and named marks.
They do not establish host storage replay support; that requires the root smoke.

## Remove this environment

First collect any reports you want to retain. On the host:

```bash
sudo /usr/bin/python3 experiments/incus_powerloss/setup.py teardown
```

This stops only this environment's SSH listener, cleans its run devices and
files, deletes its marked instance and dedicated project, and removes its
account, sudo rule, scripts and configs. It preserves existing instances,
storage pools, networks, profiles, repositories, and the main SSH service. The
dedicated development client key and known-hosts file are retained for inspection.

References: [kernel log-writes ordering and replay](https://docs.kernel.org/admin-guide/device-mapper/log-writes.html),
[upstream replay source](https://github.com/josefbacik/log-writes/tree/7b70d8a6863c5de30933d42a7672d35d01d2dc6c),
[Incus disk mounts](https://linuxcontainers.org/incus/docs/main/reference/devices_disk/).

# Verification status

Host inspected: `pingu`, Linux `7.0.0-111031-tuxedo`, Incus client/server `6.0.0`.
Existing development container: `aiwork`, project `user-1000`, address
`10.64.249.64`; host bridge address `10.64.249.1`.

The matching module exists at:

```text
/lib/modules/7.0.0-111031-tuxedo/kernel/drivers/md/dm-log-writes.ko.zst
```

**Live setup remains incomplete; storage smoke is unverified.**

On 2026-10-20, the operator started installation with host sudo. The receipt
confirms creation of project `necroflow-powerloss` and instance
`necroflow-crash-test`, configured for 2 CPUs, 4 GiB RAM and a 12 GiB root disk.
The instance's LXC monitor is present. The receipt remains `stage: installing`,
the installer process is absent, and `/etc/necroflow-powerloss/sshd_config` does
not exist. The SSH and storage smoke stages were not reached. No kernel OOM or
process-kill message was found for the inspected period. The cause of the stop
is unknown; no error output was reported by the operator.

The agent still cannot perform host administration directly:

- `sudo -n true`: `sudo: a password is required`.
- `dmsetup targets`: `/dev/mapper/control: open failed: Permission denied`.
- Current account cannot open `/dev/loop-control` for writing.
- Current Incus certificate can access project `user-1000`, but global storage
  inspection returns `Error: Certificate is restricted`.

Existing unrelated containers and repository changes remain intact. The replay
binary and fixed runner have been installed on the host. The actual test image
fingerprint has not yet been recorded in the readable receipt; the requested
image is `images:ubuntu/24.04`.

The installer now supports `resume` for this marked partial installation. It
records each command before launch, both output streams and exit codes in
`/etc/necroflow-powerloss/setup.log`, and failures in the receipt. Resume preserves
the created container/project and continues provisioning without teardown.

The pinned upstream replay binary built successfully with host GCC.

Unprivileged verification completed:

- **31 tests passed** in 1.54 seconds, including dirty/untracked source transfer,
  SSH request/path/device guards, actual current Necroflow CLI execution and
  missing-output recovery, and the built upstream replay binary handling a
  synthetic v1 log with named marks and completed flush/FUA entries. Additional
  checks cover persistent command diagnostics and resume ownership guards.
- Black formatting check passed for all five Python files.
- All infrastructure scripts compiled with the host's Python interpreter.
- Before the operator's installation, the read-only runner preflight reported
  `root: false`, `mapper_access: false`, `loop_access: false`, `configured: false`,
  `ready: false`.
- The agent's noninteractive installation attempt used `sudo -n`; it stopped with
  `sudo: a password is required` before executing the installer.

Test command used:

```bash
NF_REPLAY_LOG=/tmp/necroflow-powerloss-log-writes/replay-log .venv/bin/python -m pytest -o addopts= experiments/incus_powerloss/test_infrastructure.py -q
```

To complete actual setup, run the documented root resume command on the host:

```bash
sudo /usr/bin/python3 experiments/incus_powerloss/setup.py resume
```

Resume finishes by driving preflight and the full recording/replay/recovery smoke over
the restricted SSH path from development and retains actual results outside
the crash filesystem.

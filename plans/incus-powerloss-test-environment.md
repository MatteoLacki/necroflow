# Incus power-loss test environment: host-agent handoff

Set up an Incus test environment for Necroflow power-loss recovery testing.

## Context

- Development agent already runs inside an Incus container.
- Repository inside that container:
  `/home/matteo/Projects/nectroflow/necroflow`
- Its working tree has uncommitted changes; preserve and use those changes.
- Create a sibling test container managed by host Incus.
- Implement test infrastructure only; do not modify Necroflow behavior.

## Container

- Name: `necroflow-crash-test`.
- Keep unprivileged; start with 2 CPUs, 4 GiB RAM, bounded scratch storage.
- Install git, build tools, uv, Python 3.14, and Necroflow development dependencies.
- Transfer a separate working copy from development container.
- Keep code, environment, and test reports outside crash-test filesystem.
- Mount dedicated test filesystem at `/crash-test`. Necroflow nodes and results
  must both live there.

## Power-loss simulation

- Host manages disposable, file-backed loop devices and `dm-log-writes`.
- Check host kernel support; install/build matching replay tools.
- Use ext4 initially. Record filesystem writes and flush/FUA boundaries.
- Save a clean baseline before each workload.
- Reconstruct selected crash states by replaying recorded writes into fresh
  baseline copies, respecting completed flushes.
- Detach original test mount; attach recovered filesystem to test container.
- Run recovery checks against recovered files.
- Keep baseline, write log, expected hashes, and completion observations
  outside simulated crash filesystem.
- Container stop/restart is process control, not the power-loss simulator.
- Never format existing disks or replay over a mounted/in-use image.
- Restrict all destructive operations to newly created test backing files.

## Control from development container

- Provide SSH access to a fixed host-side runner.
- Generate dedicated client key inside development container; private key
  stays there.
- Runner manages Incus, recording, replay, verification, and cleanup.
- Expose named operations and validated run IDs; do not accept arbitrary
  host shell commands or arbitrary device paths.
- No host Incus management socket or HTTPS API needs exposing to development
  container.

## Validation

- Demonstrate recording, replay, and recovery end to end.
- Show a flushed file surviving a selected crash state.
- Show a selected earlier crash state without later uncommitted changes.
- Verify recovered bytes independently of stored Necroflow hashes.
- Do not assume current Necroflow already guarantees durable completion.

## Deliver

- Actual container name, image/version, resource limits, filesystem layout.
- Runner scripts and reproducible setup instructions.
- Exact SSH commands usable from development container.
- Smoke-test results and replay limitations.
- Cleanup command scoped to this test environment.
- If kernel support or permissions block setup, report precise blocker;
  do not substitute container termination for storage-loss simulation.

Proceed with setup and verification within this scope. Preserve existing
instances, repositories, storage pools, and unrelated host configuration.

## Technical references

- [Incus filesystem mounts](https://linuxcontainers.org/incus/docs/main/reference/devices_disk/)
- [Linux disk-write replay](https://docs.kernel.org/admin-guide/device-mapper/log-writes.html)

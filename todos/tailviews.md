# Live Job Logs

## Goal

Make the live log of every running job easy to discover and follow. Today the
logger shows the log path after a failure, but a user who wants to watch a
running command must locate `.rip/job.log` manually.

## Minimum Behavior

When a job starts, include its absolute log path and a copyable follow command
in the normal Necroflow output:

```text
log: /absolute/path/to/node/.rip/job.log
follow: tail -f /absolute/path/to/node/.rip/job.log
```

The rendered command must quote the path safely. Avoid assuming that terminal
output can reliably launch a new terminal: terminal emulators, hyperlink
protocols, remote sessions, and operating systems differ.

## Preferred Follow-Up

Provide a portable Necroflow command so users do not need to know the internal
`.rip` layout:

```console
necroflow logs --follow <pipeline-label>
```

It should:

- resolve the label within the selected pipeline;
- print existing log content before following new output;
- wait for the log file if the job has not created it yet;
- stop cleanly on interrupt and when appropriate after the job ends;
- report a clear error for unknown labels or jobs without logs.

Clickable terminal integration can be considered separately as an optional UI
feature after the portable CLI behavior exists.


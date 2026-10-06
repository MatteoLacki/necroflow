"""
Print a compact execution summary with ``--reporter``.

In general, you should replace it with your custom logic of whatever you want to present user back.
Likely a RQ-based solution would not be a bad start.
"""


def report(job_label: str, summary: dict) -> None:
    """Display one completed job's rule calls in the terminal."""
    print(f"\n{job_label}")
    print(f"{'RULE':24} {'STATE':12} {'TIME':>9} {'SIZE':>10}")
    for rule in summary["rules"]:
        status = "cached" if rule["cached"] else rule["state"]
        seconds = rule.get("duration_seconds")
        elapsed = f"{seconds:.2f} s" if seconds is not None else "—"
        print(
            f"{rule['name'][:24]:24} {status[:12]:12} "
            f"{elapsed:>9} {rule.get('output_size_human', '—'):>10}"
        )
    print(
        f"Commands: {summary['command_wall_seconds']:.2f} s wall, "
        f"{summary['cumulative_duration_seconds']:.2f} s summed"
    )
    print(
        f"Invocation: {summary['invocation']['wall_seconds']:.2f} s wall, "
        f"{summary['invocation']['hash_seconds']:.2f} s hashing"
    )

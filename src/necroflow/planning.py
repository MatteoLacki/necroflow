"""Invocation-local RuleCall cache classification."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from necroflow.dag import DAG
from necroflow.fs import _content_hash, _output_mtime
from necroflow.nodes import Node
from necroflow.rule_call import RuleCall, RuleCallState

Reason = dict[str, object]


def current_output_hash(node: Node, memo: dict[Path, str]) -> str:
    """Return current bytes hash, trusting stored hash while mtime proves safety."""
    cached = memo.get(node.relative_path)
    if cached is not None:
        return cached
    hash_file = node.rule_call.workdir / ".rip" / (node.path.name + ".hash")
    digest = None
    if hash_file.exists() and _output_mtime(node.path) <= hash_file.stat().st_mtime_ns:
        stored = hash_file.read_text().strip()
        if len(stored) == 64 and all(char in "0123456789abcdef" for char in stored):
            digest = stored
    if digest is None:
        digest = _content_hash(node.path)
    memo[node.relative_path] = digest
    return digest


@dataclass
class ExecutionPlan:
    """Required RuleCalls, orphan calls, cache evidence, and hash memo."""

    active: list[RuleCall]
    orphans: list[RuleCall]
    reasons: dict[Path, tuple[Reason, ...]] = field(default_factory=dict)
    hash_cache: dict[Path, str] = field(default_factory=dict)
    forced_call_keys: set[Path] = field(default_factory=set)

    @property
    def active_keys(self) -> set[Path]:
        return {call.relative_path for call in self.active}


def _valid_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def classify_call(
    plan: ExecutionPlan,
    call: RuleCall,
    *,
    executed_call_keys: set[Path] | None = None,
) -> RuleCallState:
    """Classify one call after every parent has settled."""
    executed = executed_call_keys or set()
    reasons: list[Reason] = []
    missing = [str(output.path) for output in call.outputs if not output.path.exists()]
    if missing:
        call.state = RuleCallState.MISSING
        plan.reasons[call.relative_path] = (
            {"kind": "output_missing", "paths": missing},
        )
        return call.state

    if call.relative_path in plan.forced_call_keys:
        reasons.append({"kind": "forced_invalidation"})
    if call.is_compromised:
        reasons.append({"kind": "compromised_prior_state"})
    for output in call.outputs:
        invalidator = output.node_type.invalidator
        if invalidator is None:
            continue
        token = invalidator(output)
        if not isinstance(token, str):
            raise TypeError(
                f"invalidator for {output.node_type.__name__} must return str, "
                f"got {type(token).__name__}"
            )
        token_path = output.path.parent / ".rip" / (output.path.name + ".invalidation")
        if not token_path.exists() or token_path.read_text() != token:
            reasons.append(
                {
                    "kind": "invalidator_changed",
                    "output_key": output.relative_path.as_posix(),
                }
            )

    metadata = call.dag.dependencies(call) if call.parents else []
    if call.parents and metadata is None:
        reasons.append({"kind": "dependency_metadata_missing_or_invalid"})
    elif metadata is not None:
        if len(metadata) != len(call.parents):
            reasons.append({"kind": "dependency_metadata_mismatch"})
        else:
            for parent, recorded in zip(call.parents, metadata):
                parent_key = parent.relative_path.as_posix()
                if recorded.get("node_key") != parent_key:
                    reasons.append(
                        {
                            "kind": "dependency_metadata_mismatch",
                            "parent_key": parent_key,
                        }
                    )
                    continue
                consumed = recorded.get("consumed_sha256")
                if not _valid_sha256(consumed):
                    reasons.append(
                        {"kind": "consumed_hash_missing", "parent_key": parent_key}
                    )
                    continue
                current = current_output_hash(parent, plan.hash_cache)
                if current != consumed:
                    reasons.append(
                        {
                            "kind": "parent_content_changed",
                            "parent_key": parent_key,
                            "consumed_sha256": consumed,
                            "current_sha256": current,
                        }
                    )

    stale_reasons = list(reasons)
    if stale_reasons:
        call.state = RuleCallState.STALE
        plan.reasons[call.relative_path] = tuple(reasons)
    else:
        call.state = RuleCallState.UP_TO_DATE
        plan.reasons[call.relative_path] = (
            {"kind": "up_to_date"},
            *reasons,
        )
    return call.state


def classify_available(
    plan: ExecutionPlan, *, executed_call_keys: set[Path] | None = None
) -> list[RuleCall]:
    """Classify unclassified calls whose parents are all up to date."""
    classified: list[RuleCall] = []
    blocked = {RuleCallState.FAILED, RuleCallState.INTERRUPTED}
    changed = True
    while changed:
        changed = False
        for call in plan.active:
            if call.state is not None:
                continue
            if any(parent.state in blocked for parent in call.parent_calls):
                call.state = RuleCallState.FAILED
                plan.reasons[call.relative_path] = ({"kind": "dependency_failed"},)
                classified.append(call)
                changed = True
            elif all(
                parent.state == RuleCallState.UP_TO_DATE for parent in call.parent_calls
            ):
                classify_call(plan, call, executed_call_keys=executed_call_keys)
                classified.append(call)
                changed = True
    return classified


def plan_execution(
    dag: DAG,
    *,
    forced_stale_call_keys: set[Path] | None = None,
) -> ExecutionPlan:
    """Build call closure and classify only calls with settled parents."""
    required = dag.required_call_keys
    active = [call for key, call in dag.calls.items() if key in required]
    orphans = [
        call
        for key, call in dag.calls.items()
        if key not in required and call.workdir.exists()
    ]
    for call in dag.calls.values():
        call.state = RuleCallState.ORPHAN if call in orphans else None
    plan = ExecutionPlan(
        active=active,
        orphans=orphans,
        forced_call_keys=set(forced_stale_call_keys or ()),
    )
    classify_available(plan)
    for call in active:
        if call.state is None:
            plan.reasons[call.relative_path] = ({"kind": "parent_will_run"},)
    return plan

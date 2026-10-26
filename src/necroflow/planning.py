"""Invocation-local RuleCall cache classification."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from necroflow.dag import DAG
from necroflow.hashers import Hasher, OutputHashes, is_tagged_by, load_hasher
from necroflow.rule_call import RuleCall

Reason = dict[str, object]


class Verdict(Enum):
    """Whether a RuleCall's existing outputs can be reused."""

    MISSING = "missing"
    STALE = "stale"
    UP_TO_DATE = "up_to_date"
    UNDECIDED = "undecided"

    @property
    def will_run(self) -> bool | None:
        """True for missing or stale, False when cached, None when undecided."""
        if self is Verdict.UNDECIDED:
            return None
        return self is not Verdict.UP_TO_DATE


@dataclass
class ExecutionPlan:
    """Required calls, inactive calls with workdirs, and their cache verdicts.

    By default, rebuilding a parent also marks its descendants for execution.
    With unchanged-output reuse enabled, descendants wait for real parent
    bytes instead; `reasons` says why their verdict is still open.
    """

    active: list[RuleCall]
    orphans: list[RuleCall]
    requested_call_keys: set[Path]
    hashes: OutputHashes
    forced_call_keys: set[Path] = field(default_factory=set)
    verdicts: dict[Path, Verdict] = field(default_factory=dict)
    reasons: dict[Path, tuple[Reason, ...]] = field(default_factory=dict)

    @property
    def active_keys(self) -> set[Path]:
        return {call.relative_path for call in self.active}

    def classify(self, call: RuleCall, threads: int) -> Verdict:
        """Classify one call whose parent calls are all up to date.

        `threads` bounds any parent hash that must be read in full.
        """
        forced = call.relative_path in self.forced_call_keys
        verdict, reasons = classify_call(call, self.hashes, threads, forced=forced)
        self.verdicts[call.relative_path] = verdict
        self.reasons[call.relative_path] = reasons
        return verdict


def classify_call(
    call: RuleCall, hashes: OutputHashes, threads: int, *, forced: bool = False
) -> tuple[Verdict, tuple[Reason, ...]]:
    """Return one call's verdict and reasons; every parent call must be settled."""
    missing = [str(output.path) for output in call.outputs if not output.path.exists()]
    if missing:
        return Verdict.MISSING, ({"kind": "output_missing", "paths": missing},)
    stale = _own_state_reasons(call, forced) + _invalidator_reasons(call)
    lineage, unchecked = _lineage_reasons(call, hashes, threads)
    stale += lineage
    if stale:
        return Verdict.STALE, tuple(stale)
    if unchecked:
        return Verdict.UNDECIDED, tuple(unchecked)
    return Verdict.UP_TO_DATE, ({"kind": "up_to_date"},)


def _own_state_reasons(call: RuleCall, forced: bool) -> list[Reason]:
    reasons: list[Reason] = []
    if forced:
        reasons.append({"kind": "forced_invalidation"})
    if call.is_compromised:
        reasons.append({"kind": "compromised_prior_state"})
    return reasons


def _invalidator_reasons(call: RuleCall) -> list[Reason]:
    reasons: list[Reason] = []
    for output in call.outputs:
        token = output.invalidation_token()
        if token is None:
            continue
        recorded = output.invalidation_file
        if not recorded.exists() or recorded.read_text() != token:
            reasons.append(
                {
                    "kind": "invalidator_changed",
                    "output_key": output.relative_path.as_posix(),
                }
            )
    return reasons


def _lineage_reasons(
    call: RuleCall, hashes: OutputHashes, threads: int
) -> tuple[list[Reason], list[Reason]]:
    """Compare recorded consumed hashes with current parent output hashes.

    Returns stale reasons, then parents whose current hash could not be
    checked without reading them in full.
    """
    if not call.parents:
        return [], []
    recorded = call.read_dependencies()
    if recorded is None:
        return [{"kind": "dependency_metadata_missing_or_invalid"}], []
    if len(recorded) != len(call.parents):
        return [{"kind": "dependency_metadata_mismatch"}], []
    stale: list[Reason] = []
    unchecked: list[Reason] = []
    for parent, entry in zip(call.parents, recorded):
        parent_key = parent.relative_path.as_posix()
        consumed = entry.get("consumed_hash")
        if entry.get("node_key") != parent_key:
            stale.append(
                {"kind": "dependency_metadata_mismatch", "parent_key": parent_key}
            )
        elif consumed is None:
            stale.append({"kind": "consumed_hash_missing", "parent_key": parent_key})
        elif not is_tagged_by(consumed, hashes.hasher):
            stale.append(
                {
                    "kind": "consumed_hash_other_hasher",
                    "parent_key": parent_key,
                    "consumed_hash": consumed,
                    "hasher": hashes.hasher.name,
                }
            )
        else:
            current = hashes.current(parent, threads)
            if current is None:
                unchecked.append(
                    {"kind": "parent_hash_unchecked", "parent_key": parent_key}
                )
            elif current != consumed:
                stale.append(
                    {
                        "kind": "parent_content_changed",
                        "parent_key": parent_key,
                        "consumed_hash": consumed,
                        "current_hash": current,
                    }
                )
    return stale, unchecked


def plan_execution(
    dag: DAG,
    *,
    forced_stale_call_keys: set[Path] | None = None,
    hasher: Hasher | str | None = None,
    hash_threads: int = 1,
    hash_untrusted_parents: bool = True,
    store_refreshed_hashes: bool = False,
    reuse_unchanged: bool = False,
) -> ExecutionPlan:
    """Find required and orphan calls, then classify every call with cached parents.

    One pass in registration order suffices: a call is interned only after the
    calls producing its inputs, so parents are always decided first. Without
    `hash_untrusted_parents`, a parent whose stored hash is not trusted leaves
    its consumer UNDECIDED instead of being read in full.
    `store_refreshed_hashes` writes such fresh digests back; only node-store
    lock holders may set it. By default, a call that will run forces its
    descendants to run too. `reuse_unchanged` instead defers those descendants
    until the rebuilt parent bytes can be compared with consumed hashes.
    """
    required = dag.required_call_keys
    plan = ExecutionPlan(
        active=[call for key, call in dag.calls.items() if key in required],
        orphans=[
            call
            for key, call in dag.calls.items()
            if key not in required and call.workdir.exists()
        ],
        requested_call_keys={
            node.rule_call.relative_path for node in dag.required_nodes
        },
        hashes=OutputHashes(
            load_hasher(hasher),
            may_hash=hash_untrusted_parents,
            store=store_refreshed_hashes,
        ),
        forced_call_keys=set(forced_stale_call_keys or ()),
    )
    threads = max(1, hash_threads)
    will_run: set[Path] = set()
    for call in plan.active:
        key = call.relative_path
        if all(
            plan.verdicts.get(parent.relative_path) is Verdict.UP_TO_DATE
            for parent in call.parent_calls
        ):
            if plan.classify(call, threads).will_run:
                will_run.add(key)
        elif any(parent.relative_path in will_run for parent in call.parent_calls):
            will_run.add(key)
            plan.reasons[key] = ({"kind": "parent_will_run"},)
            if not reuse_unchanged:
                plan.verdicts[key] = (
                    Verdict.MISSING
                    if any(not output.path.exists() for output in call.outputs)
                    else Verdict.STALE
                )
        else:
            plan.reasons[key] = ({"kind": "parent_undecided"},)
    return plan

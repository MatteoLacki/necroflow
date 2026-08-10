from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from necroflow.rule_call import RuleCall

Scheduler = Callable[
    [list["RuleCall"], list["RuleCall"], dict[str, int]], list["RuleCall"]
]


def fifo_scheduler(
    ready: list[RuleCall],
    remaining: list[RuleCall],
    available_resources: dict[str, int],
) -> list[RuleCall]:
    """Return ready RuleCalls in DAG registration order."""
    return ready

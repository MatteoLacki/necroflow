from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

from necroflow.ascii_render import render_ascii
from necroflow.dag import DAG
from necroflow.nodes import Node

_LINUX_NAME_MAX = 255
_LINUX_PATH_MAX = 4096


def _validate_request_path(name: str, *, kind: str) -> PurePosixPath:
    """Return a canonical, portable relative request path."""
    if not isinstance(name, str):
        raise TypeError(f"{kind} must be a string")
    if not name:
        raise ValueError(f"{kind} must not be empty")
    if name.startswith("."):
        raise ValueError(f"{kind} {name!r} must not start with '.'")

    label_path = PurePosixPath(name)
    if label_path.is_absolute() or label_path.as_posix() != name:
        raise ValueError(f"{kind} {name!r} must be a canonical relative POSIX path")
    for component in label_path.parts:
        if "\0" in component:
            raise ValueError(f"{kind} {name!r} contains a null byte")
        if component in {".", ".."} or component.startswith("."):
            raise ValueError(
                f"{kind} {name!r} contains forbidden component {component!r}"
            )
        length = len(os.fsencode(component))
        if length > _LINUX_NAME_MAX:
            raise ValueError(
                f"{kind} component too long "
                f"({length} > NAME_MAX {_LINUX_NAME_MAX} bytes): {component!r}"
            )
    return label_path


def _validate_pipeline_label(name: str, output_filename: str) -> PurePosixPath:
    """Return a canonical, portable relative result path for a label."""
    label_path = _validate_request_path(name, kind="Pipeline label")

    result_path = label_path / output_filename
    length = len(os.fsencode(result_path.as_posix()))
    if length > _LINUX_PATH_MAX:
        raise ValueError(
            f"Pipeline result path too long "
            f"({length} > PATH_MAX {_LINUX_PATH_MAX} bytes): {result_path}"
        )
    return label_path


def _result_paths_conflict(left: PurePosixPath, right: PurePosixPath) -> bool:
    """Return whether either result path must be a directory for the other."""
    return left == right or left in right.parents or right in left.parents


class _PipelineState:
    """Mutable construction state shared by one root Pipeline and all its views."""

    def __init__(self, dag: DAG, shellpath: str | Path | None) -> None:
        self.dag = dag
        self.shellpath = _normalize_shellpath(shellpath)
        self.nodes_list: list[Node] = []
        self.node_paths: set[Path] = set()
        self.node_names: dict[str, Node] = {}
        self.finished = False


class Pipeline:
    """One compiled request namespace over a shared canonical DAG."""

    def __init__(
        self,
        dag: DAG,
        *,
        shellpath: str | Path | None = None,
    ):
        if not isinstance(dag, DAG):
            raise TypeError(
                f"Pipeline requires an owning DAG, got {type(dag).__name__}"
            )
        self._state = _PipelineState(dag, shellpath)
        self._request_prefix = ""

    @classmethod
    def _view(cls, state: _PipelineState, request_prefix: str) -> Pipeline:
        """Return a prefixed view over existing root construction state."""
        view = cls.__new__(cls)
        view._state = state
        view._request_prefix = request_prefix
        return view

    @property
    def nodes_dir(self) -> Path:
        return self._state.dag.nodes_dir

    @property
    def dag(self) -> DAG:
        return self._state.dag

    @property
    def shellpath(self) -> str | None:
        return self._state.shellpath

    @property
    def request_prefix(self) -> str | None:
        """Return this view's qualified request prefix, or None for the root."""
        return self._request_prefix or None

    @property
    def finished(self) -> bool:
        """Return whether construction of this shared Pipeline has finished."""
        return self._state.finished

    def _assert_open(self) -> None:
        """Reject mutation and rule compilation after the root is finished."""
        if self._state.finished:
            raise RuntimeError("Pipeline construction has finished")

    def _assert_finished(self) -> None:
        """Reject operations whose meaning requires the complete Pipeline."""
        if not self._state.finished:
            raise RuntimeError("Pipeline construction is not finished")

    def finish(self) -> None:
        """Freeze this root Pipeline and every prefixed view over it."""
        if self._request_prefix:
            raise RuntimeError("only the root Pipeline can finish construction")
        self._state.finished = True

    def subpipeline(self, request_prefix: str) -> Pipeline:
        """Return a view that qualifies assignments with a request prefix."""
        self._assert_open()
        prefix = _validate_request_path(
            request_prefix, kind="Subpipeline request prefix"
        ).as_posix()
        if self._request_prefix:
            prefix = f"{self._request_prefix}/{prefix}"
        return type(self)._view(self._state, prefix)

    def _qualified_label(self, name: str) -> str:
        """Return a view-local label qualified for the root namespace."""
        if self._request_prefix:
            return f"{self._request_prefix}/{name}"
        return name

    def labels_for(self, node: Node) -> tuple[str, ...]:
        """Return labels assigned to a canonical Node in this Pipeline.

        These are qualified request names owned by the shared root and returned
        in assignment order. A Node may have several labels in one Pipeline,
        and the same canonical Node may have different labels in other
        Pipelines sharing the DAG. This is the authoritative lookup when
        producing output for a particular Pipeline or job.
        """
        return tuple(
            name
            for name, candidate in self._state.node_names.items()
            if candidate is node
        )

    @property
    def labels(self) -> tuple[str, ...]:
        """Return all qualified root labels in assignment order."""
        return tuple(self._state.node_names)

    def sinks(self) -> list[Node]:
        """Return labeled Nodes with no labeled dependents after construction."""
        self._assert_finished()
        parent_paths = {
            parent.relative_path for node in self.nodes for parent in node.parents
        }
        return [node for node in self.nodes if node.relative_path not in parent_paths]

    @property
    def nodes(self) -> list[Node]:
        return self._state.nodes_list

    def __getattr__(self, name: str) -> Node:
        try:
            return self._state.node_names[self._qualified_label(name)]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __getitem__(self, name: str) -> Node:
        if not isinstance(name, str):
            raise TypeError("Pipeline label must be a string")
        return self._state.node_names[self._qualified_label(name)]

    def _assign_node(self, name: str, value: Node) -> None:
        self._assert_open()
        qualified_name = self._qualified_label(name)
        label_path = _validate_pipeline_label(qualified_name, value.path.name)
        if qualified_name in self._state.node_names:
            raise ValueError(f"Pipeline label {qualified_name!r} already assigned")
        if value.rule_call.dag is not self._state.dag:
            raise ValueError(
                f"Node assigned as {qualified_name!r} belongs to a different DAG"
            )
        result_path = label_path / value.path.name
        for existing_name, existing_node in self._state.node_names.items():
            existing_path = PurePosixPath(existing_name) / existing_node.path.name
            if _result_paths_conflict(result_path, existing_path):
                raise ValueError(
                    f"Pipeline result path {result_path!s} for label "
                    f"{qualified_name!r} "
                    f"conflicts with {existing_path!s} for label "
                    f"{existing_name!r}"
                )
        if value.relative_path not in self._state.node_paths:
            self._state.nodes_list.append(value)
            self._state.node_paths.add(value.relative_path)
        self._state.node_names[qualified_name] = value
        self._state.dag._record_binding(value, qualified_name)

    def __setitem__(self, name: str, value: Node) -> None:
        if not isinstance(value, Node):
            raise TypeError(
                f"Pipeline labels require Node values, got {type(value).__name__}"
            )
        self._assign_node(name, value)

    def __setattr__(self, name: str, value: object) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        if isinstance(value, Node):
            if any(name in cls.__dict__ for cls in type(self).__mro__):
                raise ValueError(
                    f"Pipeline attribute {name!r} is reserved; use item syntax "
                    f"P[{name!r}] if this label is intentional"
                )
            self._assign_node(name, value)
            # Labels live only in _state.node_names, read back through
            # __getattr__; drop any earlier plain attribute of the same name
            # instead of shadowing it with a second copy of the Node.
            self.__dict__.pop(name, None)
            return
        object.__setattr__(self, name, value)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return render_ascii(self.nodes, f"Pipeline  {len(self.nodes)} nodes")

    def save(self, path) -> None:
        """Write the ASCII DAG render to a file."""
        Path(path).write_text(str(self) + "\n", encoding="utf-8")


def _normalize_shellpath(shellpath: str | Path | None) -> str | None:
    if shellpath is None:
        return None
    path = Path(shellpath).expanduser()
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValueError(f"shellpath does not exist: {path}") from exc
    if not resolved.is_file():
        raise ValueError(f"shellpath is not a file: {resolved}")
    if not os.access(resolved, os.X_OK):
        raise ValueError(f"shellpath is not executable: {resolved}")
    return str(resolved)

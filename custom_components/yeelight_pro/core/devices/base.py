from __future__ import annotations

from typing import Any, Protocol

from ..commands import NodeCommand
from ..topology import TopologyNode


class CommandExecutor(Protocol):
    async def send_node_command(self, command: NodeCommand) -> dict[str, Any]: ...


class Device:
    def __init__(self, node: TopologyNode, executor: CommandExecutor) -> None:
        self.node = node
        self._executor = executor

    @property
    def id(self) -> str | int:
        return self.node.id

    @property
    def nt(self) -> int:
        return self.node.nt

    @property
    def type(self) -> int:
        return self.node.type

    @property
    def name(self) -> str | None:
        return self.node.name

    @property
    def params(self) -> dict[str, Any]:
        return dict(self.node.params)

    @property
    def online(self) -> bool | None:
        return self.node.online

    async def set_props(self, props: dict[str, Any], *, duration: int | None = None) -> dict[str, Any]:
        return await self._executor.send_node_command(
            NodeCommand(id=self.id, nt=self.nt, props=props, duration=duration)
        )

    @staticmethod
    def _validate_range(name: str, value: int, minimum: int, maximum: int) -> None:
        if not minimum <= value <= maximum:
            raise ValueError(f"{name} must be between {minimum} and {maximum}")

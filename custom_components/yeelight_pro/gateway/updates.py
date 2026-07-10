from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .topology import NodeId, TopologyNode


@dataclass(frozen=True)
class PropertyChange:
    """One HA-visible node change released by the session state store."""

    id: NodeId
    before: TopologyNode | None
    after: TopologyNode
    update: Mapping[str, Any]

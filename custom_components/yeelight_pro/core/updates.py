from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .topology import NodeId, TopologyNode


@dataclass(frozen=True)
class PropertyChange:
    """One node changed by a gateway_post.prop push."""

    id: NodeId
    before: TopologyNode | None
    after: TopologyNode
    update: Mapping[str, Any]

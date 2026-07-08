from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from ..model.status import GatewaySessionState
from .connection import ConnectionLostEvent, ConnectionOnlineEvent, RpcPushEvent


@dataclass(frozen=True)
class ConfigureAutoSyncCommand:
    include_groups: bool = False
    include_rooms: bool = False
    include_scenes: bool = False


@dataclass(frozen=True)
class DisableAutoSyncCommand:
    pass


@dataclass(frozen=True)
class SetSessionStateCommand:
    state: GatewaySessionState
    error: BaseException | None = None


@dataclass(frozen=True)
class ConnectSessionCommand:
    pass


@dataclass(frozen=True)
class SyncSessionCommand:
    include_groups: bool = False
    include_rooms: bool = False
    include_scenes: bool = False


@dataclass(frozen=True)
class FullPropertySyncTimedOutEvent:
    sync_id: int
    include_groups: bool = False
    include_rooms: bool = False
    include_scenes: bool = False


@dataclass(frozen=True)
class RefreshNodeCommand:
    node_id: str | int
    node_type: int | None = None


SessionActorMessage: TypeAlias = (
    ConfigureAutoSyncCommand
    | DisableAutoSyncCommand
    | SetSessionStateCommand
    | ConnectSessionCommand
    | SyncSessionCommand
    | FullPropertySyncTimedOutEvent
    | RefreshNodeCommand
    | ConnectionOnlineEvent
    | ConnectionLostEvent
    | RpcPushEvent
)

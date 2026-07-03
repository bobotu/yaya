from __future__ import annotations

from ..const import KNOB_EVENT_VALUES, PANEL_EVENT_VALUES
from .base import Device


class ProgrammableSwitchDevice(Device):
    @property
    def event_values(self) -> tuple[str, ...]:
        return PANEL_EVENT_VALUES


class KnobDevice(ProgrammableSwitchDevice):
    @property
    def event_values(self) -> tuple[str, ...]:
        return PANEL_EVENT_VALUES + KNOB_EVENT_VALUES

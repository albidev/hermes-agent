"""Stable identities for assistant segments emitted over the live gateway stream."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LiveMessageIdentity:
    """Identity shared by all frames for one live assistant segment."""

    turn_id: str
    segment: int = 0

    @property
    def message_id(self) -> str:
        return f"{self.turn_id}:assistant:{self.segment}"

    def next_segment(self) -> "LiveMessageIdentity":
        return LiveMessageIdentity(self.turn_id, self.segment + 1)

    def payload(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        result = dict(payload or {})
        result["turn_id"] = self.turn_id
        result["message_id"] = self.message_id
        return result

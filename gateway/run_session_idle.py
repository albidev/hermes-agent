"""Generic non-destructive session idle lifecycle hook.

A host (e.g. local Mission Control) signals that an existing session crossed its configured
idle threshold via ``POST /api/sessions/{session_id}/idle``. The gateway emits a single
``session:idle`` hook event per live -> idle transition and latches it, so repeated polling
ticks for the same idle episode cannot emit duplicate events. New inbound activity clears the
latch (``_mark_session_live``), so a later idle episode emits again.

The hook is notify-only: it never resets, finalizes, or otherwise mutates the session or its
DB semantics. The event carries ``session_id``, ``session_key`` (when available),
``idle_seconds``/``threshold``, ``platform``/``source``, and a stable ``generation`` identity
so consumers can deduplicate across ticks.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("gateway.run")


class GatewaySessionIdleMixin:
    """Live -> idle transition hook with a per-session latch (notify-only, non-destructive)."""

    def _session_idle_latch_map(self) -> Dict[str, int]:
        """Per-session idle latch: latch key -> generation of the current idle episode."""
        latch = self.__dict__.get("_session_idle_latch")
        if latch is None:
            latch = {}
            self.__dict__["_session_idle_latch"] = latch
        return latch

    def _next_session_idle_generation(self) -> int:
        """Monotonic transition identity; never reset (stale ticks must not re-emit)."""
        generation = int(self.__dict__.get("_session_idle_generation", 0)) + 1
        self.__dict__["_session_idle_generation"] = generation
        return generation

    def _mark_session_live(self, session_key: str) -> None:
        """Clear the idle latch for ``session_key``: new activity re-arms a later transition."""
        if not session_key:
            return
        self._session_idle_latch_map().pop(session_key, None)

    async def _signal_session_idle(
        self,
        *,
        session_id: str,
        session_key: Optional[str] = None,
        idle_seconds: Optional[float] = None,
        threshold: Optional[float] = None,
        platform: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        """Emit ``session:idle`` once per live -> idle transition; latch and no-op afterwards.

        Returns ``{"emitted": bool, "reason": str, "generation": int}``. ``reason`` is
        ``"transition"`` on a fresh emit and ``"already_idle"`` when the latch suppresses a
        duplicate tick. Never touches reset/finalize paths or session DB semantics.
        """
        latch_key = session_key or session_id
        latch = self._session_idle_latch_map()
        if latch_key in latch:
            return {"emitted": False, "reason": "already_idle", "generation": latch[latch_key]}

        generation = self._next_session_idle_generation()
        latch[latch_key] = generation
        payload = {
            "session_id": session_id,
            "session_key": session_key or "",
            "idle_seconds": idle_seconds,
            "threshold": threshold,
            "platform": platform,
            "source": source,
            "generation": generation,
        }
        await self.hooks.emit("session:idle", payload)
        return {"emitted": True, "reason": "transition", "generation": generation}

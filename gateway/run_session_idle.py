"""Generic non-destructive session idle lifecycle hook.

A gateway-owned housekeeping scan observes the session activity clock and emits a single
``session:idle`` hook event per live -> idle transition. An authenticated
``POST /api/sessions/{session_id}/idle`` remains available for external hosts and
uses the same latch, so repeated polling ticks cannot emit duplicate events.

The hook is notify-only: it never resets, finalizes, or otherwise mutates the session or its
DB semantics. The event carries ``session_id``, ``session_key`` (when available),
``idle_seconds``/``threshold``, ``platform``/``source``, and a stable ``generation`` identity
so consumers can deduplicate across ticks.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from hermes_constants import get_hermes_home
from utils import atomic_json_write

logger = logging.getLogger("gateway.run")


class GatewaySessionIdleMixin:
    """Live -> idle transition hook with a per-session latch (notify-only, non-destructive)."""

    _SESSION_IDLE_STATE_VERSION = 1
    _SESSION_IDLE_STATE_FILENAME = "gateway-session-idle.json"

    def _session_idle_state_file(self) -> Path:
        """Return the profile-scoped durable live/idle transition ledger."""
        override = self.__dict__.get("_session_idle_state_path")
        if override:
            return Path(override)
        return get_hermes_home() / self._SESSION_IDLE_STATE_FILENAME

    def _load_session_idle_state(self) -> None:
        """Hydrate latches once so a gateway restart cannot erase a live episode."""
        if self.__dict__.get("_session_idle_state_loaded"):
            return
        self.__dict__["_session_idle_state_loaded"] = True
        path = self._session_idle_state_file()
        try:
            data = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
            if not isinstance(data, dict) or data.get("version") != self._SESSION_IDLE_STATE_VERSION:
                return
            latch = self._session_idle_latch_map_raw()
            live_seen = self._session_idle_live_seen_raw()
            for session_key, record in (data.get("sessions") or {}).items():
                if not isinstance(session_key, str) or not isinstance(record, dict):
                    continue
                state = record.get("state")
                if state == "live":
                    live_seen.add(session_key)
                elif state == "idle":
                    latch[session_key] = int(record.get("generation") or 0)
            self.__dict__["_session_idle_generation"] = max(
                int(data.get("generation") or 0),
                max(latch.values(), default=0),
            )
        except (OSError, ValueError, TypeError) as exc:
            logger.warning("session idle state restore failed: %s", exc)

    def _persist_session_idle_state(self) -> None:
        """Atomically persist live/idle transition state; never fail the gateway turn."""
        latch = self._session_idle_latch_map_raw()
        live_seen = self._session_idle_live_seen_raw()
        sessions = {
            key: {"state": "live"}
            for key in live_seen
            if key not in latch
        }
        sessions.update({
            key: {"state": "idle", "generation": int(generation)}
            for key, generation in latch.items()
        })
        try:
            atomic_json_write(
                self._session_idle_state_file(),
                {
                    "version": self._SESSION_IDLE_STATE_VERSION,
                    "generation": int(self.__dict__.get("_session_idle_generation", 0)),
                    "sessions": sessions,
                },
            )
        except (OSError, TypeError, ValueError) as exc:
            logger.warning("session idle state persist failed: %s", exc)

    def _session_idle_latch_map_raw(self) -> Dict[str, int]:
        latch = self.__dict__.get("_session_idle_latch")
        if latch is None:
            latch = {}
            self.__dict__["_session_idle_latch"] = latch
        return latch

    def _session_idle_live_seen_raw(self) -> set[str]:
        live_seen = self.__dict__.get("_session_idle_live_keys")
        if live_seen is None:
            live_seen = set()
            self.__dict__["_session_idle_live_keys"] = live_seen
        return live_seen

    def _session_idle_latch_map(self) -> Dict[str, int]:
        """Per-session idle latch: latch key -> generation of the current idle episode."""
        self._load_session_idle_state()
        return self._session_idle_latch_map_raw()

    def _next_session_idle_generation(self) -> int:
        """Monotonic transition identity; never reset (stale ticks must not re-emit)."""
        generation = int(self.__dict__.get("_session_idle_generation", 0)) + 1
        self.__dict__["_session_idle_generation"] = generation
        return generation

    def _mark_session_live(self, session_key: str) -> None:
        """Clear the idle latch for ``session_key`` and persist the re-armed episode."""
        if not session_key:
            return
        self._load_session_idle_state()
        live_seen = self._session_idle_live_seen_raw()
        latch = self._session_idle_latch_map_raw()
        changed = session_key not in live_seen or session_key in latch
        live_seen.add(session_key)
        latch.pop(session_key, None)
        if changed:
            self._persist_session_idle_state()

    def _session_idle_live_seen(self) -> set[str]:
        """Session keys observed live, rehydrated across gateway restarts."""
        self._load_session_idle_state()
        return self._session_idle_live_seen_raw()

    @staticmethod
    def _session_idle_timestamp(entry: Any) -> Optional[float]:
        value = getattr(entry, "updated_at", None)
        if isinstance(value, datetime):
            return value.timestamp()
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _observe_session_idle_entries(
        self, entries, *, now: Optional[float] = None, threshold_seconds: float = 300,
    ) -> list[dict[str, Any]]:
        """Return idle transitions for entries observed by the gateway.

        The first observation of an already-idle entry is intentionally silent;
        only activity seen by this process can establish a live -> idle edge.
        Active turns are never considered idle.
        """
        now = time.time() if now is None else float(now)
        threshold = max(30.0, float(threshold_seconds))
        transitions = []
        latch = self._session_idle_latch_map()
        live_seen = self._session_idle_live_seen()
        for entry in entries or ():
            session_key = str(getattr(entry, "session_key", "") or "")
            session_id = str(getattr(entry, "session_id", "") or "")
            if not session_key or not session_id:
                continue
            last_activity = self._session_idle_timestamp(entry)
            active_turn = bool(getattr(entry, "active_turn_token", None))
            if active_turn or last_activity is None or now - last_activity < threshold:
                self._mark_session_live(session_key)
                continue
            if session_key not in live_seen or session_key in latch:
                continue
            platform = getattr(getattr(entry, "platform", None), "value", "") or ""
            transitions.append({
                "session_id": session_id,
                "session_key": session_key,
                "idle_seconds": max(0.0, now - last_activity),
                "threshold": threshold,
                "platform": platform,
                "source": "gateway",
            })
        return transitions

    def _session_idle_entry_snapshot(self):
        """Copy the routing entries without holding the store lock across awaits."""
        store = getattr(self, "session_store", None)
        if store is None:
            return []
        lock = getattr(store, "_lock", None)
        if lock is None:
            return list(getattr(store, "_entries", {}).values())
        with lock:
            ensure_loaded = getattr(store, "_ensure_loaded_locked", None)
            if callable(ensure_loaded):
                ensure_loaded()
            return list(getattr(store, "_entries", {}).values())

    async def _scan_session_idle(
        self, *, entries=None, now: Optional[float] = None, threshold_seconds: Optional[float] = None,
    ) -> dict[str, int]:
        """Scan gateway-owned session activity and emit ``session:idle`` transitions."""
        if entries is None:
            entries = self._session_idle_entry_snapshot()
        if threshold_seconds is None:
            threshold_seconds = float(getattr(getattr(self, "config", None), "session_idle_event_seconds", 300) or 300)
        transitions = self._observe_session_idle_entries(
            entries, now=now, threshold_seconds=threshold_seconds,
        )
        emitted = 0
        for transition in transitions:
            outcome = await self._signal_session_idle(**transition)
            emitted += int(bool(outcome.get("emitted")))
        return {"checked": len(entries), "emitted": emitted}

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
        self._persist_session_idle_state()
        return {"emitted": True, "reason": "transition", "generation": generation}

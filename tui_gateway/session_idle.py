"""TUI/dashboard adapter for the shared session-idle coordinator."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Callable, Iterable

from gateway.session_idle_coordinator import SessionIdleCoordinator


class _HookBridge:
    def __init__(self, emitter: Callable[..., Any]) -> None:
        self._emitter = emitter

    async def emit(self, event_type: str, payload: dict[str, Any]) -> None:
        await asyncio.to_thread(self._emitter, event_type, payload)


class TuiSessionIdleCoordinator(SessionIdleCoordinator):
    """Adapt ``tui_gateway.server._sessions`` to the shared idle state machine."""

    def __init__(self, hook_emitter: Callable[..., Any] | None = None) -> None:
        if hook_emitter is None:
            from hermes_cli.lifecycle import invoke_hook
            hook_emitter = lambda event_type, payload: invoke_hook(event_type, **payload)
        self.hooks = _HookBridge(hook_emitter)

    @staticmethod
    def entries_from_sessions(sessions: dict[str, dict[str, Any]]) -> list[Any]:
        entries = []
        for session in sessions.values():
            # Draft sessions have no persisted transcript and must never trigger
            # synthesis. Their agent is built lazily on the first real prompt.
            if not session.get("agent") or not session.get("session_key"):
                continue
            entries.append(SimpleNamespace(
                session_key=str(session["session_key"]),
                # The durable TUI session key is the identity used by SessionDB,
                # lifecycle hooks, and the bridge.
                session_id=str(session["session_key"]),
                updated_at=session.get("last_active"),
                active_turn_token="running" if session.get("running") else None,
                platform=SimpleNamespace(value="tui"),
            ))
        return entries

    async def scan_sessions(
        self,
        sessions: dict[str, dict[str, Any]],
        *,
        now: float | None = None,
        threshold_seconds: float = 300,
    ) -> dict[str, int]:
        entries = self.entries_from_sessions(sessions)
        return await self._scan_session_idle(
            entries=entries, now=now, threshold_seconds=threshold_seconds,
        )

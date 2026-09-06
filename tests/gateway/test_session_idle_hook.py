"""Invariant tests for the generic non-destructive session idle lifecycle hook.

The hook emits ``session:idle`` exactly once per live -> idle transition, never while
already idle, and never resets/closes the session. Reactivation (new inbound activity)
clears the latch so a later idle episode emits again.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import Platform, PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.run import GatewayRunner
from hermes_state import SessionDB


def _make_runner():
    runner = object.__new__(GatewayRunner)
    runner.hooks = SimpleNamespace(emit=AsyncMock())
    return runner


@pytest.mark.asyncio
async def test_one_event_per_live_to_idle_transition():
    runner = _make_runner()
    outcome = await runner._signal_session_idle(session_id="s1", session_key="k1")
    assert outcome["emitted"] is True
    assert runner.hooks.emit.await_count == 1
    event_type, payload = runner.hooks.emit.await_args.args
    assert event_type == "session:idle"
    assert payload["session_id"] == "s1"
    assert payload["session_key"] == "k1"
    assert "generation" in payload


@pytest.mark.asyncio
async def test_no_event_while_already_idle():
    runner = _make_runner()
    first = await runner._signal_session_idle(session_id="s1", session_key="k1")
    second = await runner._signal_session_idle(session_id="s1", session_key="k1")
    assert first["emitted"] is True
    assert second["emitted"] is False
    assert second["reason"] == "already_idle"
    assert second["generation"] == first["generation"]
    assert runner.hooks.emit.await_count == 1


@pytest.mark.asyncio
async def test_reactivation_permits_later_idle_event():
    runner = _make_runner()
    await runner._signal_session_idle(session_id="s1", session_key="k1")
    assert runner.hooks.emit.await_count == 1
    runner._mark_session_live("k1")
    outcome = await runner._signal_session_idle(session_id="s1", session_key="k1")
    assert outcome["emitted"] is True
    assert runner.hooks.emit.await_count == 2


@pytest.mark.asyncio
async def test_idle_signal_does_not_reset_session():
    runner = _make_runner()
    await runner._signal_session_idle(session_id="s1", session_key="k1")
    # The mixin only fires the hook; it never touches reset/finalize paths.
    assert runner.hooks.emit.await_count == 1
    assert not hasattr(runner, "_sessions") or runner._sessions == {}


@pytest.mark.asyncio
async def test_idle_endpoint_emits_hook_without_reset(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    try:
        adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "sk-test"}))
        adapter._session_db = db
        db.create_session("sess-1", "api_server")

        runner = object.__new__(GatewayRunner)
        runner.hooks = SimpleNamespace(emit=AsyncMock())
        runner.session_store = MagicMock()
        entry = MagicMock()
        entry.session_key = "agent:main:telegram:dm:42"
        entry.platform = Platform.TELEGRAM
        runner.session_store.lookup_by_session_id.return_value = entry
        adapter.gateway_runner = runner

        app = web.Application()
        app.router.add_post("/api/sessions/{session_id}/idle", adapter._handle_session_idle)

        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post(
                "/api/sessions/sess-1/idle",
                json={"idle_seconds": 300, "threshold": 300},
                headers={"Authorization": "Bearer sk-test"},
            )
            assert resp.status == 200
            data = await resp.json()
            assert data["emitted"] is True
            assert data["session_id"] == "sess-1"

        assert runner.hooks.emit.await_count == 1
        event_type, payload = runner.hooks.emit.await_args.args
        assert event_type == "session:idle"
        assert payload["session_id"] == "sess-1"
        assert payload["session_key"] == "agent:main:telegram:dm:42"
        assert payload["platform"] == "telegram"
        assert payload["idle_seconds"] == 300
        assert payload["threshold"] == 300
        # Non-destructive: the notification must not reset or finalize the session.
        runner.session_store.reset_session.assert_not_called()
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()


@pytest.mark.asyncio
async def test_idle_endpoint_requires_auth(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    try:
        adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "sk-test"}))
        adapter._session_db = db
        app = web.Application()
        app.router.add_post("/api/sessions/{session_id}/idle", adapter._handle_session_idle)
        async with TestClient(TestServer(app)) as cli:
            resp = await cli.post("/api/sessions/sess-1/idle", json={})
            assert resp.status == 401
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()

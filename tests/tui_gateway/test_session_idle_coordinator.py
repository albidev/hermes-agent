from types import SimpleNamespace

import pytest

from tui_gateway.session_idle import TuiSessionIdleCoordinator


def test_tui_coordinator_maps_runtime_sessions_to_idle_entries():
    coordinator = TuiSessionIdleCoordinator(hook_emitter=lambda *_: None)
    entries = coordinator.entries_from_sessions({
        "runtime-sid": {
            "session_key": "20260907_090131_c781cf",
            "agent": object(),
            "last_active": 1000.0,
            "running": False,
        },
        "draft-sid": {
            "session_key": "draft",
            "agent": None,
            "last_active": 1000.0,
            "running": False,
        },
    })

    assert len(entries) == 1
    entry = entries[0]
    assert entry.session_id == "20260907_090131_c781cf"
    assert entry.session_key == "20260907_090131_c781cf"
    assert entry.updated_at == 1000.0
    assert entry.active_turn_token is None
    assert entry.platform == SimpleNamespace(value="tui")


def test_tui_coordinator_marks_running_sessions_active():
    coordinator = TuiSessionIdleCoordinator(hook_emitter=lambda *_: None)
    entries = coordinator.entries_from_sessions({
        "runtime-sid": {
            "session_key": "tui-session",
            "agent": object(),
            "last_active": 1000.0,
            "running": True,
        },
    })

    assert entries[0].active_turn_token == "running"


@pytest.mark.asyncio
async def test_tui_idle_transition_survives_coordinator_restart(tmp_path):
    events = []
    sessions = {
        "runtime-sid": {
            "session_key": "tui-session",
            "agent": object(),
            "last_active": 1000.0,
            "running": False,
        },
    }
    first = TuiSessionIdleCoordinator(hook_emitter=lambda event, payload: events.append((event, payload)))
    first._session_idle_state_path = tmp_path / "session-idle.json"
    assert await first.scan_sessions(sessions, now=1000.0, threshold_seconds=300) == {
        "checked": 1, "emitted": 0,
    }

    restarted = TuiSessionIdleCoordinator(hook_emitter=lambda event, payload: events.append((event, payload)))
    restarted._session_idle_state_path = tmp_path / "session-idle.json"
    result = await restarted.scan_sessions(sessions, now=1400.0, threshold_seconds=300)

    assert result["emitted"] == 1
    assert [event for event, _ in events] == ["session:idle"]
    assert events[0][1]["session_id"] == "tui-session"

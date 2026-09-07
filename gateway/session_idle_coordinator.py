"""Shared session-idle coordinator entry point.

The implementation lives in ``run_session_idle`` because the messaging gateway
runner historically owned the lifecycle mixin. TUI/dashboard runtimes import
this coordinator instead of implementing a second idle state machine, so the
persistent live/idle ledger and transition semantics stay identical.
"""

from gateway.run_session_idle import GatewaySessionIdleMixin


SessionIdleCoordinator = GatewaySessionIdleMixin

__all__ = ["SessionIdleCoordinator"]

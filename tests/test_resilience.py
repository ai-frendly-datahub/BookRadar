from __future__ import annotations

from types import SimpleNamespace

from bookradar.resilience import (
    SourceCircuitBreakerListener,
    SourceCircuitBreakerManager,
    get_circuit_breaker_manager,
)


def test_circuit_breaker_manager_reuses_and_resets_breakers() -> None:
    manager = SourceCircuitBreakerManager()

    first = manager.get_breaker("source-a")
    second = manager.get_breaker("source-a")
    manager.get_breaker("source-b")

    assert first is second
    assert manager.get_status()["source-a"] == first.current_state

    manager.reset_breaker("source-a")
    manager.reset_breaker("missing")
    manager.reset_all()

    assert set(manager.get_status()) == {"source-a", "source-b"}


def test_global_circuit_breaker_manager_is_singleton() -> None:
    assert get_circuit_breaker_manager() is get_circuit_breaker_manager()


def test_circuit_breaker_listener_methods_accept_state_events() -> None:
    listener = SourceCircuitBreakerListener()
    cb = SimpleNamespace(name="source-a")
    old_state = SimpleNamespace(name="closed")
    new_state = SimpleNamespace(name="open")

    listener.before_call(cb, object())
    listener.state_change(cb, old_state, new_state)
    listener.state_change(cb, None, new_state)
    listener.failure(cb, RuntimeError("boom"))
    listener.success(cb)

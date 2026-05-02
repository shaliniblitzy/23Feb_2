"""Saga state machine, coordinator, scheduler, and compensators tests.

Tests the AAP R-18 reference implementation for distributed transactions:
    - state_machine.py        — _EVENT_TRANSITIONS matrix + transition helpers
    - coordinator.py          — start_saga, handle_event, trigger_compensation
    - compensation.py         — 4 compensator classes
    - scheduler.py            — deadline-driven compensation poller
"""

from __future__ import annotations

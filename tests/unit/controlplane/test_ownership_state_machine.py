# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,


from __future__ import annotations

import itertools

import pytest

from controlplane.ownership import (
    Action,
    CutoverState,
    Engine,
    Ownership,
    SideEffect,
    TransitionError,
    TransitionRequest,
    transition,
)


def own(engine=Engine.PYTHON, shadow=None, state=CutoverState.STEADY, epoch=1, recon=None) -> Ownership:
    return Ownership("branch-001", "m_loan", engine, shadow, state, epoch, recon)


def req(action, engine=None, actor="alice", approved_by=None, passed=None, rid=None, dual=True) -> TransitionRequest:
    return TransitionRequest(action, actor, "test", engine, approved_by, passed, rid, dual)


def test_assign_only_from_unowned():
    result = transition(own(Engine.NONE), req(Action.ASSIGN, Engine.KAFKA))
    assert result.current.engine is Engine.KAFKA
    assert result.current.epoch == 2
    with pytest.raises(TransitionError, match="already owned"):
        transition(own(Engine.PYTHON), req(Action.ASSIGN, Engine.KAFKA))


def test_full_python_to_kafka_migration_happy_path():
    o = own(Engine.PYTHON)
    r = transition(o, req(Action.START_SHADOW, Engine.KAFKA))
    assert r.current.shadow_engine is Engine.KAFKA
    assert r.current.cutover_state is CutoverState.SHADOWING
    assert r.side_effects == (SideEffect.CLEAR_SHADOW,)
    assert r.current.epoch == o.epoch + 1

    r = transition(r.current, req(Action.BEGIN_RECONCILIATION))
    assert r.current.cutover_state is CutoverState.RECONCILING
    assert r.current.epoch == 2, "state-only changes do not fence writers"

    r = transition(r.current, req(Action.RECORD_RECONCILIATION, passed=True, rid=41))
    assert r.current.cutover_state is CutoverState.CUTOVER_READY
    assert r.current.last_reconciliation_id == 41

    r = transition(r.current, req(Action.CUTOVER, actor="alice", approved_by="bob"))
    assert r.current.engine is Engine.KAFKA
    assert r.current.shadow_engine is None
    assert r.current.cutover_state is CutoverState.STEADY
    assert r.side_effects == (SideEffect.PROMOTE_SHADOW,)
    assert r.current.epoch == 3


def test_rollback_is_the_same_workflow_reversed():
    o = own(Engine.KAFKA)
    r = transition(o, req(Action.START_SHADOW, Engine.PYTHON))
    r = transition(r.current, req(Action.BEGIN_RECONCILIATION))
    r = transition(r.current, req(Action.RECORD_RECONCILIATION, passed=True, rid=7))
    r = transition(r.current, req(Action.CUTOVER, approved_by="bob"))
    assert r.current.engine is Engine.PYTHON


def test_failed_reconciliation_returns_to_shadowing_and_blocks_cutover():
    o = own(Engine.PYTHON, Engine.KAFKA, CutoverState.RECONCILING, 2)
    r = transition(o, req(Action.RECORD_RECONCILIATION, passed=False, rid=9))
    assert r.current.cutover_state is CutoverState.SHADOWING
    with pytest.raises(TransitionError, match="CUTOVER_READY"):
        transition(r.current, req(Action.CUTOVER, approved_by="bob"))


@pytest.mark.parametrize("approver", [None, "", "alice"])
def test_cutover_requires_a_different_approver(approver):
    ready = own(Engine.PYTHON, Engine.KAFKA, CutoverState.CUTOVER_READY, 3, recon=5)
    with pytest.raises(TransitionError, match="approv"):
        transition(ready, req(Action.CUTOVER, actor="alice", approved_by=approver))


def test_dual_control_can_be_disabled_by_platform_setting():
    ready = own(Engine.PYTHON, Engine.KAFKA, CutoverState.CUTOVER_READY, 3, recon=5)
    r = transition(ready, req(Action.CUTOVER, actor="alice", approved_by=None, dual=False))
    assert r.current.engine is Engine.KAFKA


def test_cutover_requires_recorded_reconciliation():
    ready = own(Engine.PYTHON, Engine.KAFKA, CutoverState.CUTOVER_READY, 3, recon=None)
    with pytest.raises(TransitionError, match="no passing reconciliation"):
        transition(ready, req(Action.CUTOVER, approved_by="bob"))


def test_cannot_shadow_with_the_current_owner_or_twice():
    with pytest.raises(TransitionError, match="already owns"):
        transition(own(Engine.KAFKA), req(Action.START_SHADOW, Engine.KAFKA))
    shadowing = own(Engine.PYTHON, Engine.KAFKA, CutoverState.SHADOWING, 2)
    with pytest.raises(TransitionError, match="requires STEADY"):
        transition(shadowing, req(Action.START_SHADOW, Engine.KAFKA))


def test_pause_and_resume_fence_writers_and_restore_state():
    shadowing = own(Engine.PYTHON, Engine.KAFKA, CutoverState.SHADOWING, 4)
    paused = transition(shadowing, req(Action.PAUSE)).current
    assert paused.cutover_state is CutoverState.PAUSED
    assert paused.epoch == 5
    resumed = transition(paused, req(Action.RESUME)).current
    assert resumed.cutover_state is CutoverState.SHADOWING
    assert resumed.epoch == 6
    steady = transition(transition(own(), req(Action.PAUSE)).current, req(Action.RESUME)).current
    assert steady.cutover_state is CutoverState.STEADY


def test_abort_shadow_clears_target():
    shadowing = own(Engine.PYTHON, Engine.KAFKA, CutoverState.RECONCILING, 4, recon=3)
    r = transition(shadowing, req(Action.ABORT_SHADOW))
    assert r.current.shadow_engine is None
    assert r.current.last_reconciliation_id is None
    assert r.side_effects == (SideEffect.CLEAR_SHADOW,)


def test_force_assign_is_break_glass_with_approval():
    with pytest.raises(TransitionError, match="approved_by"):
        transition(own(Engine.KAFKA), req(Action.FORCE_ASSIGN, Engine.PYTHON))
    r = transition(own(Engine.KAFKA), req(Action.FORCE_ASSIGN, Engine.PYTHON, approved_by="cto"))
    assert r.current.engine is Engine.PYTHON
    with pytest.raises(TransitionError, match="abort the shadow"):
        transition(own(Engine.KAFKA, Engine.PYTHON, CutoverState.SHADOWING), req(
            Action.FORCE_ASSIGN, Engine.PYTHON, approved_by="cto"))


def test_actor_and_reason_are_mandatory_for_audit():
    with pytest.raises(TransitionError, match="actor"):
        transition(own(Engine.NONE), TransitionRequest(Action.ASSIGN, " ", "x", Engine.KAFKA))
    with pytest.raises(TransitionError, match="reason"):
        transition(own(Engine.NONE), TransitionRequest(Action.ASSIGN, "alice", "", Engine.KAFKA))


def test_invariants_hold_for_every_reachable_state():
    start = own(Engine.NONE)
    requests = [
        req(Action.ASSIGN, Engine.PYTHON), req(Action.ASSIGN, Engine.KAFKA),
        req(Action.START_SHADOW, Engine.PYTHON), req(Action.START_SHADOW, Engine.KAFKA),
        req(Action.BEGIN_RECONCILIATION), req(Action.RECORD_RECONCILIATION, passed=True, rid=1),
        req(Action.RECORD_RECONCILIATION, passed=False, rid=2), req(Action.CUTOVER, approved_by="bob"),
        req(Action.ABORT_SHADOW), req(Action.PAUSE), req(Action.RESUME),
        req(Action.FORCE_ASSIGN, Engine.PYTHON, approved_by="bob"),
    ]
    seen, frontier = set(), [start]
    while frontier:
        state = frontier.pop()
        key = (state.engine, state.shadow_engine, state.cutover_state, state.last_reconciliation_id)
        if key in seen:
            continue
        seen.add(key)
        assert state.shadow_engine is None or state.shadow_engine is not state.engine
        if state.shadow_engine is None:
            assert state.cutover_state in (CutoverState.STEADY, CutoverState.PAUSED)
        for r in requests:
            try:
                result = transition(state, r)
            except TransitionError:
                continue
            routing_changed = (result.previous.engine, result.previous.shadow_engine,
                               result.previous.cutover_state == CutoverState.PAUSED) != (
                result.current.engine, result.current.shadow_engine,
                result.current.cutover_state == CutoverState.PAUSED)
            if routing_changed:
                assert result.current.epoch == result.previous.epoch + 1
            frontier.append(result.current)
    assert len(seen) > 10


def test_state_space_has_no_dead_ends():
    for engine, shadow in itertools.product([Engine.PYTHON, Engine.KAFKA], [None, Engine.PYTHON, Engine.KAFKA]):
        if shadow is engine:
            continue
        for state in CutoverState:
            if shadow is None and state not in (CutoverState.STEADY, CutoverState.PAUSED):
                continue
            o = own(engine, shadow, state, 3, recon=1)
            if state is CutoverState.PAUSED:
                o = transition(o, req(Action.RESUME)).current
            if o.shadow_engine:
                o = transition(o, req(Action.ABORT_SHADOW)).current
            assert o.cutover_state is CutoverState.STEADY

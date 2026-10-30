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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum


class Engine(StrEnum):
    PYTHON = "PYTHON"
    KAFKA = "KAFKA"
    NONE = "NONE"


class CutoverState(StrEnum):
    STEADY = "STEADY"
    SHADOWING = "SHADOWING"
    RECONCILING = "RECONCILING"
    CUTOVER_READY = "CUTOVER_READY"
    PAUSED = "PAUSED"


class Action(StrEnum):
    ASSIGN = "assign"
    START_SHADOW = "start_shadow"
    BEGIN_RECONCILIATION = "begin_reconciliation"
    RECORD_RECONCILIATION = "record_reconciliation"
    CUTOVER = "cutover"
    ABORT_SHADOW = "abort_shadow"
    PAUSE = "pause"
    RESUME = "resume"
    FORCE_ASSIGN = "force_assign"


class SideEffect(StrEnum):
    CLEAR_SHADOW = "clear_shadow"
    PROMOTE_SHADOW = "promote_shadow"


class TransitionError(ValueError):
    pass


@dataclass(frozen=True)
class Ownership:
    source_id: str
    table_name: str
    engine: Engine
    shadow_engine: Engine | None
    cutover_state: CutoverState
    epoch: int
    last_reconciliation_id: int | None = None
    shadow_requested_by: str | None = None

    def as_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "table_name": self.table_name,
            "engine": self.engine.value,
            "shadow_engine": self.shadow_engine.value if self.shadow_engine else None,
            "cutover_state": self.cutover_state.value,
            "epoch": self.epoch,
            "last_reconciliation_id": self.last_reconciliation_id,
            "shadow_requested_by": self.shadow_requested_by,
        }


@dataclass(frozen=True)
class TransitionRequest:
    action: Action
    actor: str
    reason: str
    engine: Engine | None = None
    approved_by: str | None = None
    reconciliation_passed: bool | None = None
    reconciliation_id: int | None = None
    require_dual_control: bool = True
    reconciliation_valid: bool | None = None
    database_enforced_approval: bool = False


@dataclass(frozen=True)
class TransitionResult:
    previous: Ownership
    current: Ownership
    side_effects: tuple[SideEffect, ...] = ()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise TransitionError(message)


def _bump(o: Ownership, **changes) -> Ownership:
    return replace(o, epoch=o.epoch + 1, **changes)


def transition(current: Ownership, request: TransitionRequest) -> TransitionResult:
    _require(bool(request.actor and request.actor.strip()), "actor is required")
    _require(bool(request.reason and request.reason.strip()), "reason is required")
    a = request.action
    o = current
    where = f"{o.source_id}/{o.table_name}"

    if a is Action.ASSIGN:
        _require(request.engine in (Engine.PYTHON, Engine.KAFKA), "assign needs engine PYTHON or KAFKA")
        _require(o.engine is Engine.NONE, f"{where}: already owned by {o.engine}; use start_shadow to migrate")
        _require(o.shadow_engine is None, f"{where}: shadow in progress")
        return TransitionResult(o, _bump(o, engine=request.engine, cutover_state=CutoverState.STEADY))

    if a is Action.START_SHADOW:
        _require(request.engine in (Engine.PYTHON, Engine.KAFKA), "start_shadow needs engine PYTHON or KAFKA")
        _require(
            o.cutover_state is CutoverState.STEADY,
            f"{where}: start_shadow requires STEADY, is {o.cutover_state}",
        )
        _require(o.engine is not Engine.NONE, f"{where}: no production owner yet; use assign")
        _require(request.engine is not o.engine, f"{where}: {request.engine} already owns production")
        nxt = _bump(
            o,
            shadow_engine=request.engine,
            cutover_state=CutoverState.SHADOWING,
            last_reconciliation_id=None,
            shadow_requested_by=request.actor,
        )
        return TransitionResult(o, nxt, (SideEffect.CLEAR_SHADOW,))

    if a is Action.BEGIN_RECONCILIATION:
        _require(
            o.cutover_state in (CutoverState.SHADOWING, CutoverState.RECONCILING, CutoverState.CUTOVER_READY),
            f"{where}: reconciliation requires an active shadow, state is {o.cutover_state}",
        )
        return TransitionResult(o, replace(o, cutover_state=CutoverState.RECONCILING))

    if a is Action.RECORD_RECONCILIATION:
        _require(o.cutover_state is CutoverState.RECONCILING, f"{where}: not RECONCILING")
        _require(request.reconciliation_passed is not None, "reconciliation_passed is required")
        state = CutoverState.CUTOVER_READY if request.reconciliation_passed else CutoverState.SHADOWING
        return TransitionResult(o, replace(o, cutover_state=state, last_reconciliation_id=request.reconciliation_id))

    if a is Action.CUTOVER:
        _require(
            o.cutover_state is CutoverState.CUTOVER_READY,
            f"{where}: cutover requires CUTOVER_READY, is {o.cutover_state}",
        )
        _require(o.shadow_engine is not None, f"{where}: no shadow engine")
        _require(o.last_reconciliation_id is not None, f"{where}: no passing reconciliation recorded")
        _require(
            request.reconciliation_valid is True,
            f"{where}: the recorded reconciliation is not a recent PASSED shadow_vs_prod run for this table",
        )
        if request.require_dual_control:
            _require(bool(request.approved_by), f"{where}: cutover requires approved_by (dual control)")
            if not request.database_enforced_approval:
                _require(request.approved_by != request.actor, f"{where}: approver must differ from requester")
                _require(
                    request.approved_by != o.shadow_requested_by,
                    f"{where}: approver must differ from whoever started the shadow ({o.shadow_requested_by})",
                )
        nxt = _bump(
            o,
            engine=o.shadow_engine,
            shadow_engine=None,
            cutover_state=CutoverState.STEADY,
            shadow_requested_by=None,
        )
        return TransitionResult(o, nxt, (SideEffect.PROMOTE_SHADOW,))

    if a is Action.ABORT_SHADOW:
        _require(o.shadow_engine is not None, f"{where}: no shadow to abort")
        nxt = _bump(
            o,
            shadow_engine=None,
            cutover_state=CutoverState.STEADY,
            last_reconciliation_id=None,
            shadow_requested_by=None,
        )
        return TransitionResult(o, nxt, (SideEffect.CLEAR_SHADOW,))

    if a is Action.PAUSE:
        _require(o.cutover_state is not CutoverState.PAUSED, f"{where}: already paused")
        return TransitionResult(o, _bump(o, cutover_state=CutoverState.PAUSED))

    if a is Action.RESUME:
        _require(o.cutover_state is CutoverState.PAUSED, f"{where}: not paused")
        state = CutoverState.SHADOWING if o.shadow_engine else CutoverState.STEADY
        return TransitionResult(o, _bump(o, cutover_state=state))

    if a is Action.FORCE_ASSIGN:
        _require(request.engine in (Engine.PYTHON, Engine.KAFKA, Engine.NONE), "force_assign needs an engine")
        _require(o.shadow_engine is None, f"{where}: abort the shadow first")
        _require(bool(request.approved_by), f"{where}: force_assign requires approved_by")
        if request.require_dual_control:
            _require(request.approved_by != request.actor, f"{where}: approver must differ from requester")
        return TransitionResult(o, _bump(o, engine=request.engine, cutover_state=CutoverState.STEADY))

    raise TransitionError(f"Unsupported action {a}")

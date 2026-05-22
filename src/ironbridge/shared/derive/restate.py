"""
Derives Restate VirtualObjects from Resource definitions.

Fully generic — zero domain knowledge. Behaviour driven entirely by ActionKind:

  CREATE / UPDATE  → exclusive handler, repo.save() on returned Resource
  DESTROY          → exclusive handler, repo.delete()
  ACTION           → exclusive handler, ActionContext injected, effects executed
  READ             → shared handler, no write
  STREAM           → shared handler, no write

Special case: ACTION handlers with a "position" state key get the position
counter treatment (add_message pattern) — detected by presence of
`idempotency_key` in the action's parameter list.

After every ACTION, executes collected effects via Restate sends — atomic,
journaled, durable.
"""

from __future__ import annotations

import inspect
import json
import os
from datetime import datetime
from typing import Any

import restate
from restate.exceptions import TerminalError
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import text

from ironbridge.platform.identity.auth import extract_auth
from ironbridge.shared.db import tenant_session
from ironbridge.shared.derive.repository import SqlAlchemyRepository
from ironbridge.shared.framework.actions import ActionKind
from ironbridge.shared.framework.effects import ActionContext, DeferredSendEffect, SendEffect, WorkflowEffect
from ironbridge.shared.framework.resource import Resource

_object_cache: dict[str, restate.VirtualObject] = {}


def derive_virtual_object(resource_cls: type[Resource]) -> restate.VirtualObject:
    name = resource_cls.__name__
    if name in _object_cache:
        return _object_cache[name]

    if not resource_cls.__meta__.get("restate_object"):
        raise ValueError(f"{name} does not have restate_object = True in Meta")

    obj = restate.VirtualObject(name)

    for action_name, action_meta in resource_cls.__actions__.items():
        _attach_handler(obj, resource_cls, action_name, action_meta)

    # Inject infra-only queue handlers on resources that have add_message
    if "add_message" in resource_cls.__actions__:
        _attach_queue_handlers(obj, name)

    _object_cache[name] = obj
    return obj


def _attach_queue_handlers(obj: restate.VirtualObject, resource_name: str) -> None:
    """
    Inject _enqueue_run and _run_done handlers onto any VirtualObject
    that has add_message. These are pure infra — no domain imports.

    _enqueue_run: called by _execute_effects instead of firing AgentRun directly.
      - If no active run → set active_run_id, fire AgentRun
      - If active → push to pending queue, write QUEUED system message to thread

    _run_done: called by restate_workflow.py on run completion.
      - Clear active_run_id
      - If pending queue non-empty → pop first, fire AgentRun
    """
    _rp = restate.InvocationRetryPolicy(max_attempts=3)

    async def _enqueue_run(ctx: Any, req: dict | None) -> None:
        from ironbridge.platform.agents.agent_run import AgentRunRequest
        from ironbridge.shared.derive.restate_workflow import WORKFLOW_HANDLERS

        req = req or {}
        active = await ctx.get("active_run_id")

        if active is not None:
            cancel_fn, _ = WORKFLOW_HANDLERS[("AgentRun", "cancel")]
            status_fn, _ = WORKFLOW_HANDLERS[("AgentRun", "status")]

            status = await ctx.workflow_call(status_fn, key=active, arg=None)
            if status != "running":
                ctx.set("active_run_id", None)
                ctx.set("pending_runs", [])
                active = None
            else:
                # Active run is running (possibly suspended on HITL) — cancel it
                # and drain the queue so the new message starts immediately.
                ctx.workflow_send(cancel_fn, key=active, arg=None)
                ctx.set("active_run_id", None)
                ctx.set("pending_runs", [])
                active = None

        if active is None:
            # No active run — fire immediately
            ctx.set("active_run_id", req["run_id"])
            handler_fn, _ = WORKFLOW_HANDLERS[("AgentRun", "run")]
            ctx.workflow_send(handler_fn, key=req["run_id"], arg=AgentRunRequest(**req))
        else:
            # Queue it — push to pending list
            pending: list = await ctx.get("pending_runs") or []
            pending.append(req)
            ctx.set("pending_runs", pending)

            # Write a QUEUED system message via generic_send (idempotent via idempotency_key)
            run_id = req["run_id"]
            ctx.generic_send(
                resource_name,
                "add_message",
                json.dumps({
                    "participant_id": "system",
                    "participant_type": "SYSTEM",
                    "role": "SYSTEM",
                    "content": {
                        "version": 1,
                        "parts": [{
                            "type": "event",
                            "event": "AGENT_RUN_QUEUED",
                            "run_id": run_id,
                            "queue_position": len(pending),
                        }],
                    },
                    "idempotency_key": f"{run_id}:queued",
                    "tenant_id": req.get("tenant_id", ""),
                    "user_name": "system",
                }).encode(),
                key=ctx.key(),
            )

    async def _run_done(ctx: Any, req: dict | None) -> None:
        from ironbridge.platform.agents.agent_run import AgentRunRequest
        from ironbridge.shared.derive.restate_workflow import WORKFLOW_HANDLERS

        ctx.set("active_run_id", None)
        pending: list = await ctx.get("pending_runs") or []

        if pending:
            next_req = pending.pop(0)
            ctx.set("pending_runs", pending)
            ctx.set("active_run_id", next_req["run_id"])
            handler_fn, input_model = WORKFLOW_HANDLERS[("AgentRun", "run")]
            ctx.workflow_send(handler_fn, key=next_req["run_id"], arg=AgentRunRequest(**next_req))

    obj.handler("_enqueue_run", invocation_retry_policy=_rp)(_enqueue_run)
    obj.handler("_run_done", invocation_retry_policy=_rp)(_run_done)


def _attach_handler(
    obj: restate.VirtualObject,
    resource_cls: type[Resource],
    action_name: str,
    action_meta: Any,
) -> None:
    fn = action_meta.fn
    kind = action_meta.kind
    sig = inspect.signature(fn)
    param_names = [
        p for p in sig.parameters
        if p not in ("self", "action_ctx")
    ]
    has_action_ctx = "action_ctx" in sig.parameters
    # Position counter pattern: ACTION with idempotency_key param (add_message)
    is_positional = kind == ActionKind.ACTION and "idempotency_key" in param_names

    def make_handler(
        _cls=resource_cls,
        _fn=fn,
        _kind=kind,
        _params=param_names,
        _has_ctx=has_action_ctx,
        _is_positional=is_positional,
        _name=action_name,
    ):
        terminal_errors = _cls.__meta__.get("terminal_errors", (ValueError,))

        async def handler(ctx: Any, req: dict | None) -> Any:
            req = req or {}
            auth = extract_auth(req)
            tenant_id = auth.tenant_id

            # Position counter recovery for positional actions (add_message)
            next_pos: int | None = None
            if _is_positional:
                current_pos: int = await ctx.get("position")
                if current_pos is None:
                    def _recover():
                        with tenant_session(tenant_id) as db:
                            return db.execute(
                                text("SELECT COALESCE(MAX(position), 0) FROM messages WHERE thread_id = :tid"),
                                {"tid": ctx.key()},
                            ).scalar()
                    current_pos = await ctx.run("recover_position", _recover)
                next_pos = current_pos + 1

            action_ctx = ActionContext() if _has_ctx else None

            def _run():
                with tenant_session(tenant_id) as db:
                    if action_ctx is not None:
                        action_ctx.session = db
                    repo = SqlAlchemyRepository(db, _cls)
                    instance = repo.find_by_id(ctx.key()) or _cls()
                    instance.id = ctx.key()
                    kwargs = {k: req[k] for k in _params if k in req}
                    if _has_ctx:
                        kwargs["action_ctx"] = action_ctx
                    try:
                        result = _fn(instance, **kwargs)
                    except terminal_errors as e:
                        raise TerminalError(str(e), status_code=400)

                    if _is_positional and next_pos is not None and hasattr(result, "position"):
                        result.position = next_pos

                    if (_kind.implicit_save or _kind == ActionKind.ACTION) and isinstance(result, Resource):
                        repo.save(result)
                    elif _kind.implicit_delete:
                        repo.delete(ctx.key())

                    return _serialize(result)

            result = await ctx.run(_name, _run)

            if _is_positional and next_pos is not None:
                ctx.set("position", next_pos)

            # Execute effects collected by ActionContext
            if action_ctx:
                _execute_effects(ctx, action_ctx.effects, result, resource_name=_cls.__name__)

            return result

        return handler

    h = make_handler()
    _rp = restate.InvocationRetryPolicy(max_attempts=3)
    if kind.is_shared:
        obj.handler(action_name, kind="shared", invocation_retry_policy=_rp)(h)
    else:
        obj.handler(action_name, invocation_retry_policy=_rp)(h)


def _execute_effects(ctx: Any, effects: list, result: Any, resource_name: str = "") -> None:
    """
    Execute effects collected by ActionContext.
    Called after ctx.run() — journaled by Restate, atomic with the DB write.

    AgentRun/run WorkflowEffects are routed through {resource_name}/_enqueue_run
    so that only one AgentRun is active per thread at a time, with extras queued.
    """
    from ironbridge.shared.derive.restate_workflow import WORKFLOW_HANDLERS

    for effect in effects:
        if isinstance(effect, WorkflowEffect):
            # Route new AgentRun starts through the resource's queue handler
            if effect.service == "AgentRun" and effect.handler == "run":
                thread_id = effect.arg.get("thread_id") if isinstance(effect.arg, dict) else None
                if thread_id and resource_name:
                    ctx.generic_send(
                        resource_name,
                        "_enqueue_run",
                        json.dumps(effect.arg).encode(),
                        key=thread_id,
                    )
                    continue
            entry = WORKFLOW_HANDLERS.get((effect.service, effect.handler))
            if entry is None:
                raise ValueError(
                    f"No workflow handler registered for ({effect.service!r}, {effect.handler!r})"
                )
            handler_fn, input_model = entry
            arg = input_model(**effect.arg) if input_model and isinstance(effect.arg, dict) else effect.arg
            ctx.workflow_send(handler_fn, key=effect.key, arg=arg)
        elif isinstance(effect, DeferredSendEffect):
            arg = effect.factory(result)
            kwargs = {"key": effect.key} if effect.key is not None else {}
            ctx.generic_send(
                effect.service,
                effect.handler,
                json.dumps(arg).encode(),
                **kwargs,
            )
        elif isinstance(effect, SendEffect):
            kwargs = {"key": effect.key} if effect.key is not None else {}
            ctx.generic_send(
                effect.service,
                effect.handler,
                json.dumps(effect.arg).encode(),
                **kwargs,
            )


def _serialize(obj: Any) -> Any:
    from pydantic import BaseModel
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    if isinstance(obj, list):
        return [_serialize(item) for item in obj]
    if isinstance(obj, Resource):
        mapper = sa_inspect(type(obj))
        result = {
            col.key: _serialize(getattr(obj, col.key, None)) for col in mapper.mapper.column_attrs
        }
        for rel in mapper.mapper.relationships:
            val = getattr(obj, rel.key, None)
            if val is None:
                result[rel.key] = None
            elif hasattr(val, "__iter__"):
                # Skip collections — avoid ballooning journal/Pusher payload
                pass
            else:
                result[rel.key] = _serialize(val)
        return result
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "value"):  # Enum
        return obj.value
    return obj

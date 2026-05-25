"""
AgentRun Workflow — durable loop runner.

Owns:
  - Restate Workflow registration and handler wiring
  - Cancel promise lifecycle
  - Run lifecycle events (RUNNING, COMPLETED, CANCELLED, FAILED)
  - Resolving agent by agent_id and calling agent.run(AgentContext)
  - HITL promise resolution and orphaned run handling

Does NOT own:
  - Agent logic, LLM calls, tool execution, HITL policy
  - Message formatting or thread writes (AgentContext)
  - History fetching (AgentContext)

Cancel protocol:
  - cancel() resolves the "cancel" durable promise
  - AgentContext.step() peeks it before each step, raises AgentCancelledError
  - Workflow runner catches AgentCancelledError and marks CANCELLED

Crash recovery:
  - ctx.run() steps journaled — replayed, not re-executed on restart
  - DB writes idempotent via ON CONFLICT DO NOTHING
"""

from __future__ import annotations

import json
from typing import Any

import restate
from cuid2 import cuid_wrapper
from restate import WorkflowContext, WorkflowSharedContext
from restate.exceptions import TerminalError

from ironbridge.platform.agents.agent_run import (
    AgentRunRequest,
    AgentRunResult,
    AgentRunStatus,
    ResolveHITLRequest,
)
from ironbridge.platform.agents.agent_run_event import AgentRunEvent
from ironbridge.platform.agents.context import AgentCancelledError, AgentContext
from ironbridge.platform.agents.registry import agent_registry
from ironbridge.platform.sessions.thread import AddMessageRequest
from ironbridge.shared.db import tenant_session
from ironbridge.shared.derive.repository import SqlAlchemyRepository

_STATUS_KEY = "current_step"
_CANCEL_PROMISE = "cancel"
_cuid = cuid_wrapper()

agent_run_workflow = restate.Workflow("AgentRun")


_RETRY_POLICY = restate.InvocationRetryPolicy(max_attempts=1)


@agent_run_workflow.main(invocation_retry_policy=_RETRY_POLICY)
async def run(ctx: WorkflowContext, req: AgentRunRequest) -> AgentRunResult:
    ctx.set(_STATUS_KEY, "running")
    await ctx.run(
        "mark_running",
        lambda: _write_run_event(req.thread_id, req.run_id, req.tenant_id, "RUNNING"),
    )

    agent_ctx = AgentContext(ctx, req)

    try:
        agent = agent_registry.resolve(req.agent_id)
        await agent.run(agent_ctx)

    except AgentCancelledError:
        ctx.set(_STATUS_KEY, "cancelled")
        await ctx.run(
            "mark_cancelled",
            lambda: _write_run_event(req.thread_id, req.run_id, req.tenant_id, "CANCELLED"),
        )
        ctx.generic_send("Thread", "_run_done", b"{}", key=req.thread_id)
        return AgentRunResult(
            run_id=req.run_id,
            agent_id=req.agent_id,
            thread_id=req.thread_id,
            status=AgentRunStatus.CANCELLED,
            message_count=0,
        )

    except Exception as e:
        ctx.set(_STATUS_KEY, "failed")
        # Unwrap TerminalError message so the thread gets a human-readable error.
        err_msg = e.message if isinstance(e, TerminalError) else str(e)
        await ctx.run(
            "mark_failed",
            lambda: _write_run_event(req.thread_id, req.run_id, req.tenant_id, "FAILED"),
        )
        ctx.generic_send(
            "Thread",
            "add_message",
            AddMessageRequest(
                participant_id=req.agent_id,
                participant_type="AGENT",
                role="SYSTEM",
                content={"version": 1, "parts": [{"type": "event", "event": "AGENT_RUN_FAILED", "error": err_msg}]},
                idempotency_key=f"{req.run_id}:failed",
                tenant_id=req.tenant_id,
                user_name=req.agent_id,
            ).model_dump_json().encode(),
            key=req.thread_id,
        )
        ctx.generic_send("Thread", "_run_done", b"{}", key=req.thread_id)
        return AgentRunResult(
            run_id=req.run_id,
            agent_id=req.agent_id,
            thread_id=req.thread_id,
            status=AgentRunStatus.FAILED,
            message_count=0,
            error=err_msg,
        )

    ctx.set(_STATUS_KEY, "completed")
    await ctx.run(
        "mark_completed",
        lambda: _write_run_event(req.thread_id, req.run_id, req.tenant_id, "COMPLETED"),
    )
    ctx.generic_send("Thread", "_run_done", b"{}", key=req.thread_id)
    return AgentRunResult(
        run_id=req.run_id,
        agent_id=req.agent_id,
        thread_id=req.thread_id,
        status=AgentRunStatus.COMPLETED,
        message_count=0,
    )


@agent_run_workflow.handler()
async def resolve_hitl(ctx: WorkflowContext, req: ResolveHITLRequest) -> None:
    """
    Resolves a HITL named promise from a response_reply message.
    Called by derive/restate.py when add_message sees a response_reply part.

    If workflow is not active (orphaned run), writes explicit error events:
      - AGENT_RUN_ORPHANED system message to the thread
      - FAILED event to agent_run_events
    """
    step = await ctx.get(_STATUS_KEY)
    if step in (None, "completed", "cancelled", "failed"):
        await ctx.run(
            f"write_orphaned_event:{req.request_id}",
            lambda: _write_run_event(req.thread_id, ctx.key(), req.tenant_id, "FAILED"),
        )
        ctx.generic_send(
            "Thread",
            "add_message",
            AddMessageRequest(
                participant_id="system",
                participant_type="SYSTEM",
                role="SYSTEM",
                content={"version": 1, "parts": [{"type": "event", "event": "AGENT_RUN_ORPHANED", "request_id": req.request_id}]},
                idempotency_key=f"{ctx.key()}:orphaned:{req.request_id}",
                tenant_id=req.tenant_id,
                user_name="system",
            ).model_dump_json().encode(),
            key=req.thread_id,
        )
        return
    await ctx.promise(f"hitl:{req.request_id}").resolve(
        {"selected": req.selected, "submitted_by": req.submitted_by}
    )


@agent_run_workflow.handler()
async def cancel(ctx: WorkflowContext) -> None:
    """Resolves cancel promise — agent exits at next ctx.step() boundary."""
    await ctx.promise(_CANCEL_PROMISE).resolve(True)


@agent_run_workflow.handler()
async def status(ctx: WorkflowSharedContext) -> str:
    """Returns current step label — non-blocking, concurrent."""
    return await ctx.get(_STATUS_KEY) or "unknown"


def build_agent_run_workflow() -> restate.Workflow:
    return agent_run_workflow


# Registry: (service, handler) -> (Restate handler fn, input Pydantic model or None)
# Used by _execute_effects in derive/restate.py to route WorkflowEffects generically.
WORKFLOW_HANDLERS: dict[tuple[str, str], tuple[Any, Any]] = {
    ("AgentRun", "run"): (run, AgentRunRequest),
    ("AgentRun", "resolve_hitl"): (resolve_hitl, ResolveHITLRequest),
    ("AgentRun", "cancel"): (cancel, None),
    ("AgentRun", "status"): (status, None),
}


# ── Internal helpers ───────────────────────────────────────────────────────────


def _write_run_event(thread_id: str, run_id: str, tenant_id: str, event_type: str) -> None:
    with tenant_session(tenant_id) as db:
        repo = SqlAlchemyRepository(db, AgentRunEvent)
        event = AgentRunEvent()
        event.id = _cuid()
        event.thread_id = thread_id
        event.run_id = run_id
        event.event_type = event_type
        repo.save(event)
        db.commit()



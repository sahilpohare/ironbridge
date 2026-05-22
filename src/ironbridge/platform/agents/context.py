"""
AgentContext — the runtime interface handed to every agent implementation.

Agents import nothing from Restate. All durable primitives (steps, HITL,
thread writes, history) are accessed through this object.

Constructed by the workflow runner (restate_workflow.py), wraps the Restate
WorkflowContext. Agents are unaware of Restate internals.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import typing
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from pydantic import BaseModel

from restate.exceptions import RetryableError, TerminalError

from ironbridge.platform.agents.agent_run import AgentRunRequest
from ironbridge.platform.agents.hitl import HITL, HumanResponse
from ironbridge.platform.sessions.thread import MessageView
from ironbridge.shared.db import tenant_session

# HTTP status codes that are permanent — no point retrying
_TERMINAL_STATUS_CODES = {400, 401, 403, 404, 422}


class AgentCancelledError(Exception):
    """Raised by AgentContext.step() when a cancel signal is detected."""


class AgentContext:
    """
    Runtime context for an agent execution.

    Exposes domain-level primitives wrapping Restate:
      - step(name, fn)            durable step with automatic cancel check
      - run(name, fn)             durable step without cancel check (for setup/teardown)
      - get_history()             fetch thread messages from DB (sync, call inside step())
      - write_message(content)    fire-and-forget to Thread.add_message queue
      - request_approval(...)     HITL suspend/resume
      - is_cancelled()            non-blocking cancel check

    All writes go through the Thread VirtualObject queue — position ordering
    guaranteed. DB is the source of truth, not Restate state.
    """

    def __init__(self, restate_ctx: Any, req: AgentRunRequest) -> None:
        self._ctx = restate_ctx
        self.req = req
        self.thread_id = req.thread_id
        self.run_id = req.run_id
        self.tenant_id = req.tenant_id
        self.agent_id = req.agent_id
        self._hitl = HITL(restate_ctx, req.thread_id, req.run_id, req.tenant_id)
        self._cancel_promise = restate_ctx.promise("cancel")
        self._retry_counts: dict[str, int] = {}

    async def step(self, name: str, fn: Callable) -> Any:
        """
        Durable step with automatic cancel check before execution.
        Raises AgentCancelledError if cancel signal is set.
        Use this for all agent work — LLM calls, tool execution, history fetches.

        Permanent errors (HTTP 4xx) are wrapped as TerminalError so Restate
        stops retrying immediately and the workflow surfaces AGENT_RUN_FAILED.

        Pydantic transparency: Restate journals JSON only. Pydantic models
        returned from fn are serialized to dicts before journaling, then
        reconstructed from fn's return type annotation after.
        """
        if await self.is_cancelled():
            raise AgentCancelledError()

        ret_hint = inspect.get_annotations(fn, eval_str=True).get("return")

        def _guarded():
            try:
                result = fn()
                if isinstance(result, BaseModel):
                    return result.model_dump(mode="json")
                if isinstance(result, list) and all(isinstance(item, BaseModel) for item in result):
                    return [item.model_dump(mode="json") for item in result]
                return result
            except Exception as e:
                status = getattr(e, "status_code", None)
                if status in _TERMINAL_STATUS_CODES:
                    raise TerminalError(str(e), status_code=400)
                raise

        try:
            result = await self._ctx.run(name, _guarded)
        except RetryableError as e:
            self._retry_counts[name] = self._retry_counts.get(name, 0) + 1
            ikey = hashlib.sha256(f"{self.run_id}:retry:{name}:{self._retry_counts[name]}".encode()).hexdigest()[:16]
            self._ctx.generic_send(
                "Thread",
                "add_message",
                json.dumps({
                    "participant_id": f"agent-run-{self.run_id}",
                    "participant_type": "AGENT",
                    "role": "SYSTEM",
                    "content": {"version": 1, "parts": [{"type": "event", "event": "AGENT_RUN_RETRY", "step": name, "error": str(e)}]},
                    "idempotency_key": ikey,
                    "tenant_id": self.tenant_id,
                    "user_name": f"agent-run-{self.run_id}",
                }).encode(),
                key=self.thread_id,
            )
            raise

        # Reconstruct Pydantic models from journaled dicts using return type hint
        origin = typing.get_origin(ret_hint)
        args = typing.get_args(ret_hint)
        if origin is list and args and isinstance(result, list):
            item_cls = args[0]
            if isinstance(item_cls, type) and issubclass(item_cls, BaseModel):
                return [item_cls(**m) if isinstance(m, dict) else m for m in result]
        if isinstance(ret_hint, type) and issubclass(ret_hint, BaseModel) and isinstance(result, dict):
            return ret_hint(**result)
        return result

    async def run(self, name: str, fn: Callable) -> Any:
        """
        Durable step without cancel check.
        Use for setup/teardown steps that must complete regardless of cancellation.
        """
        return await self._ctx.run(name, fn)

    def get_history(self, limit: int = 200) -> list[MessageView]:
        """
        Fetch thread message history from DB.
        Sync — designed to be called inside step() or run():
            history = await ctx.step("fetch_history", ctx.get_history)
        Filters control messages (response_reply, event) not visible to LLM.
        limit: max messages returned (default 200, newest first after filter).
        """
        return _fetch_thread(self.thread_id, self.tenant_id, limit=limit)

    def write_message(self, content: dict, message_count: int) -> None:
        """
        Fire-and-forget write to Thread.add_message queue.
        Non-blocking — workflow does not wait for Thread handler.
        Position ordering guaranteed by Thread's exclusive queue.
        """
        ikey = hashlib.sha256(f"{self.run_id}:response:{message_count}".encode()).hexdigest()[:16]
        self._ctx.generic_send(
            "Thread",
            "add_message",
            json.dumps(
                {
                    "participant_id": f"agent-run-{self.run_id}",
                    "participant_type": "AGENT",
                    "role": "ASSISTANT",
                    "content": content,
                    "idempotency_key": f"{self.run_id}:response:{ikey}",
                    "tenant_id": self.tenant_id,
                    "user_name": f"agent-run-{self.run_id}",
                }
            ).encode(),
            key=self.thread_id,
        )

    async def call(
        self,
        tool: Any,
        step_name: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Run a tool as a durable step, with optional HITL gate.

        If the tool has `requires_approval = True`, a HITL prompt is shown
        before execution. The prompt can be customised via `approval_prompt`
        (a string that may reference kwargs by name, e.g. "Fetch weather for {location}?").

        Usage:
            result = await ctx.call(MyTool(), location="London")
        """
        requires_approval: bool = getattr(tool, "requires_approval", False)
        approval_prompt: str = getattr(tool, "approval_prompt", f"Allow tool `{tool.name}` to run?")
        try:
            formatted_prompt = approval_prompt.format(**kwargs)
        except (KeyError, AttributeError):
            formatted_prompt = approval_prompt

        if requires_approval:
            approval = await self.request_approval(
                prompt=formatted_prompt,
                created_by=f"agent-run-{self.run_id}",
                options=[
                    {"id": "approve", "label": "Allow"},
                    {"id": "reject", "label": "Deny"},
                ],
            )
            if not approval.approved:
                return f"Tool `{tool.name}` was denied by the user."

        name = step_name or f"{tool.name}:{':'.join(str(kwargs[k]) for k in sorted(kwargs))}"
        return await self.step(name, lambda: tool._run(**kwargs))

    async def request_approval(
        self,
        prompt: str,
        created_by: str,
        options: list[dict] | None = None,
        context: dict | None = None,
        timeout: timedelta = timedelta(hours=24),
    ) -> HumanResponse:
        """Suspend and wait for human response via HITL promise."""
        return await self._hitl.request_response(
            prompt=prompt,
            created_by=created_by,
            options=options,
            context=context,
            timeout=timeout,
        )

    async def is_cancelled(self) -> bool:
        """Non-blocking check of cancel signal."""
        try:
            val = await self._cancel_promise.peek()
            return val is True
        except Exception:
            return False


# ── Internal DB helpers ────────────────────────────────────────────────────────


def _fetch_thread(thread_id: str, tenant_id: str, limit: int = 200) -> list[MessageView]:
    """
    Fetch thread message history via Thread.get_messages domain action.
    Filters control messages (response_reply, event) — not visible to LLM.
    """
    from ironbridge.platform.sessions.thread import Thread
    from ironbridge.shared.derive.repository import SqlAlchemyRepository

    with tenant_session(tenant_id) as db:
        repo = SqlAlchemyRepository(db, Thread)
        instance = repo.find_by_id(thread_id)
        if instance is None:
            return []
        return instance.get_messages(limit=limit)



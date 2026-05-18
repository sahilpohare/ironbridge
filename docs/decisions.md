# Architectural Decisions

A running log of decisions made, why, and what changed.

---

## 1. Postgres as source of truth, Restate as execution scheduler

**Decision:** Restate is infrastructure — same category as Postgres, Redis, S3. It owns execution coordination (durability, retry, serialization). Postgres owns all domain data. No domain or framework layer imports from `restate`.

**Reasoning:** Two sources of truth create drift. A crash between a Restate journal write and a DB write leaves them inconsistent with no clean recovery path.

**Consequence:** All `ctx.run()` steps wrap idempotent DB operations. The journal records that a step ran — not what it produced. On replay the DB write is skipped (journaled result returned).

**Restate ctx allowed only for execution coordination:**
- `"position"` — write-through cache of `MAX(position)`, recovers from Postgres on cold start

---

## 2. All writes are upserts

**Decision:** `SqlAlchemyRepository.save()` always issues `INSERT ... ON CONFLICT DO UPDATE`, never a bare `INSERT`.

**Reasoning:** Restate replays handlers on crash. A bare `INSERT` raises a duplicate-key error on replay. An upsert is safe.

**Consequence:** Every resource must have a stable primary key before `save()`. CUIDs are generated in the domain action, not by the DB.

---

## 3. Tenant isolation is structural, not a filter

**Decision:** Postgres Row Level Security enforced via `SET LOCAL app.tenant_id` on every connection. No `WHERE tenant_id = ?` in application queries.

**Reasoning:** Application-layer filters fail open. RLS fails closed — a missing filter returns zero rows.

**Evolution:**
- Started with `filter_by(tenant_id=...)` in every repository method.
- Moved to `tenant_session()` + RLS policies — one enforcement point.

---

## 4. Tenancy key injected by framework, not declared in domain

**Decision:** Resources with `Meta.tenant_scoped = True` have `tenant_id` injected by `ResourceMeta.__new__()`. Domain files never declare it.

**Reasoning:** `tenant_id` is an infrastructure concern — it exists to satisfy RLS, not to model a business concept.

---

## 5. Idempotency key is caller-supplied

**Decision:** `Message.idempotency_key` is explicit, supplied by the caller. `ON CONFLICT (thread_id, idempotency_key) DO NOTHING` at the DB layer.

**Two-layer idempotency:**
1. Restate: same `Idempotency-Key` header → cached response, handler never re-runs (24h window).
2. DB: `UNIQUE(thread_id, idempotency_key)` → permanent backstop after cache expires.

**Evolution:**
- Started with caller-supplied key.
- Moved to content hash to remove it from domain API.
- Reverted: content hash deduplicates identical content across different senders — wrong semantics.

---

## 6. Resource IS the SQLAlchemy model

**Decision:** `Resource` inherits from SQLAlchemy `DeclarativeBase`. Columns declared with `Mapped[]` + `mapped_column()` directly.

**Evolution:**
- Built `fields.py` with `CuidField`, `StringField`, etc. and `derive/orm.py` to emit SQLAlchemy models.
- Recognized this was rebuilding SQLAlchemy poorly. Deleted both.

---

## 7. Restate VirtualObject key = resource primary key

**Decision:** The VirtualObject key for a resource is its primary key. `derive/restate.py` sets `instance.id = ctx.key()` before every handler — domain `create()` must not overwrite a pre-set id.

**Consequence:** `position` assignment is safe without DB-level locking because Restate serializes concurrent `add_message` calls for the same thread.

---

## 8. Public API is Restate ingress; browser-facing endpoints are FastAPI

**Decision:** Core thread/agent operations go through Restate ingress (`:8080`). Browser-facing endpoints (bind, send, auth-required ops) live behind FastAPI (`:9080`) under `/api/`.

**Reasoning:** Restate ingress has no auth layer. Channel adapters that own inbound HTTP (e.g. `WebAdapter`) register their FastAPI routes via `get_router()`, mounted on the FastAPI app.

**Consequence:** HTTP/2 required for Restate. The app runs under `hypercorn`.

---

## 9. Sessions and agents are separate subdomains

**Decision:**
- `platform/identity/` — User, Tenant (auth, roles, lifecycle)
- `platform/sessions/` — Thread, Message (conversation, ordering)
- `platform/agents/` — Agent (definition), AgentRun (execution), HITL
- `platform/channels/` — Channel, ChannelBinding, ChannelDelivery (fanout)

**Reasoning:** Different lifecycles, different authors, different concerns.

**Participant vs User:**
- `User` — real person with auth concerns.
- `Participant` — conversation actor. Free string `participant_id` on `Message`. No FK to users.

---

## 10. Agent execution uses Restate Workflow, not VirtualObject

**Decision:** `AgentRun` is a Restate `Workflow` (one-shot, durable). `Agent` is a VirtualObject (definition, mutable config).

**Reasoning:** An agent run has a clear start and end. Workflow gives a single durable execution with cancel handle and status query.

**Cancellation:** `cancel` handler resolves a durable promise. The main loop checks after each step. No mid-LLM-call interruption — current step completes, then loop exits.

---

## 11. Agent messages write through Thread.add_message

**Decision:** All agent output — text responses, tool calls, HITL prompts — goes through `Thread.add_message`. No direct DB writes from the workflow.

**Reasoning:** Position consistency. The VirtualObject queue serializes all writers.

---

## 12. HITL is message-driven, not a side-channel

**Decision:** HITL interaction is regular `add_message` calls with structured content types — `response_request` and `response_reply`. No separate HTTP endpoint.

**Options model:** `options: null` = free text, `[{id, label}]` = single select, `multi_select: true` = multi-select.

---

## 13. Content uses versioned parts model

**Decision:** `Message.content` is `{"version": 1, "parts": [...]}`. Part type drives rendering and infrastructure behavior.

**Evolution:** `format` → `version`.

---

## 14. Position via ctx cache + DB fallback

**Decision:** `Message.position` = `ctx.get("position") + 1`, with `MAX(position)` recovery on cold start.

**Why not DB sequence per thread:** DDL at runtime, schema management nightmare.
**Why not timestamps:** Clock skew can invert order.
**Why not vector clocks:** Single writer degenerates to scalar counter.

---

## 15. AgentRunEvent is a separate table, not in messages

**Decision:** `agent_run_events` table stores workflow lifecycle events (RUNNING, COMPLETED, CANCELLED, FAILED). Not in `messages`.

**Reasoning:** Lifecycle events are operational metadata, not conversation content.

*(Superseded by Decision 20 on tenant scoping.)*

---

## 16. Working memory deferred

**Decision:** No working memory implementation.

**Rejected:** `ctx.get/set("working_memory")` — domain data in infra state, lost on Restate wipe.
**Future:** `thread_state` table or external service (Supermemory).

---

## 17. Pydantic models for workflow I/O

**Decision:** `AgentRunRequest` and `AgentRunResult` are Pydantic `BaseModel`, not dataclasses.

**Reasoning:** Restate's SDK auto-selects `PydanticJsonSerde` for Pydantic models. Dataclasses deserialized as plain dicts — `.thread_id` raises `AttributeError`.

---

## 18. Migrations are autogenerated

**Decision:** `alembic revision --autogenerate` against a live DB. RLS policies in `0001` are the only manual exception.

---

## 19. HITL reconciliation for orphaned awakeables

**Decision:** After Restate restart, awakeables created before the restart are dead. `HITL.request_response()` checks Postgres for an existing `response_reply` before suspending. If found, returns immediately without waiting.

**Protocol:** `ctx.run("check_existing_reply:{request_id}", _find_existing_reply)` — journaled, idempotent on replay.

---

## 20. AgentRunEvents are tenant-scoped with RLS

**Decision:** `agent_run_events` has `tenant_id` + RLS policy. Supersedes Decision 15.

**Migration:** `a1b2c3d4e5f6` adds the column, enables RLS, creates the policy.

---

## 21. Pluggable agent architecture — BaseAgent / AgentContext / AgentRegistry

**Decision:** Three primitives in `platform/agents/`:
- `BaseAgent(ABC)` — `async def run(self, ctx: AgentContext) -> None`
- `AgentContext` — wraps Restate `WorkflowContext`, exposes domain methods
- `agent_registry` — maps `agent_id` string → `BaseAgent` subclass

**Agent implementations** live in `services/` (not platform). Platform is infrastructure. `services/agents/` holds concrete agents (stub, weather, etc.).

**Registration:** import side-effect in `main.py` triggers `agent_registry.register(...)`.

---

## 22. ConversationWorkflow deferred — Restate WorkflowSharedContext limitation

**Decision:** Revert from single `ConversationWorkflow` per thread back to Thread VirtualObject + AgentRun Workflow.

**Root cause:** Restate SDK `0.17.x` shared handler `ctx.get/set` fails when handler input is a Pydantic model — SDK tries to serialize input during journal bookkeeping.

**Future:** Re-evaluate with SDK `0.18.x`.

---

## 23. X-Tenant-Id header auth for browser-facing FastAPI routes

**Decision:** FastAPI routes under `/api/` require `X-Tenant-Id` matching the URL `{tenant_id}`. Restate ingress routes are internal — no auth layer there.

---

## 24. Thread is the model, Channel is the view

**Decision:** Every channel adapter receives every thread event (not just ASSISTANT messages). Adapters decide what to render. Thread owns the canonical message log; channels are projections of it.

**Reasoning:** Email, Slack, CLI all render the same thread differently. The thread does not know about channels. Channels observe the thread.

**Implementation:** `Thread.add_message` sends every message to `ChannelDelivery` (Restate Service, stateless + concurrent). `ChannelDelivery` wraps the DB lookup and `adapter.on_message(message, config, ctx)` in a single `ctx.run()` — journaled, deduplicated on retry. `ChannelContext` is constructed outside `ctx.run()` so adapters can call `ctx.generic_send` (a Restate ctx operation) without violating the no-ctx-ops-inside-ctx.run rule.

---

## 25. Channel adapter strategy pattern — BaseChannelAdapter

**Decision:** Every channel integration extends `BaseChannelAdapter` (in `services/channels/adapters/base.py`):
- `on_message(message, config, ctx)` — outbound: called for every thread message
- `receive(...)` — inbound: posts to Restate ingress
- `get_router()` — optional: returns a FastAPI `APIRouter` for inbound HTTP endpoints

**Reasoning:** Channels like WhatsApp, Email, CLI, Web all differ in transport but share the same contract. A class hierarchy enforces the contract without HTTP proxy boilerplate.

**Adapter implementations** live in `services/channels/adapters/`. They are NOT part of `platform/channels/` — platform owns the infrastructure primitives (delivery, registry, context, message types). Concrete adapters are service-layer concerns.

**Registration:** `register_adapter(instance)` at module import. `main.py` imports each adapter — the import triggers self-registration.

---

## 26. WebAdapter owns both directions of the web channel

**Decision:** `WebAdapter` in `services/channels/adapters/web.py` owns:
- Outbound: Pusher trigger on `on_message()`
- Inbound: FastAPI routes `/api/{tenant}/channels/web/bind` and `/api/{tenant}/channels/web/send` via `get_router()`

**Reasoning:** The web channel is a self-contained integration. A separate proxy controller (`thread_controller.py`) was creating split ownership — the adapter had the Pusher logic but an unrelated controller had the bind logic.

**Consequence:** `thread_controller.py` stripped to an empty router. `main.py` mounts `WebAdapter.get_router()` directly. No raw SQL in adapter — uses `Channel.create()` + `SqlAlchemyRepository`.

---

## 27. No raw SQL in domain or adapter code

**Decision:** Domain actions use `@action` + `SqlAlchemyRepository`. Adapters that need DB access use `SqlAlchemyRepository.find_by()` and domain actions. Raw `db.execute(text(...))` is forbidden outside of RLS setup and aggregate queries with no domain equivalent.

**Reasoning:** Raw SQL bypasses the domain model — it cannot be replayed safely, it doesn't participate in the framework's upsert semantics, and it breaks the derivation contract.

---

## 28. No `Any` type in domain or adapter interfaces

**Decision:** All method signatures use concrete types. `ChannelMessage`, `ChannelContext`, `dict`, typed Pydantic models — never `Any`.

**Reasoning:** `Any` defeats type checking at the most important boundary. Channel adapters are a public contract; the compiler must enforce it.

---

## 29. Pusher is web infrastructure, not core

**Decision:** Pusher is used exclusively in `WebAdapter`. Removed from `restate.py` and any shared layer.

**Reasoning:** Pusher is a browser transport. CLI, WhatsApp, email channels do not use it. Placing Pusher in core `restate.py` tied all channels to a web-specific dependency.

**Evolution:** Pusher emit was originally in `restate.py` after every `add_message`. Moved to `WebAdapter.on_message()`.

---

## 30. ChannelContext gives adapters write-back capability

**Decision:** `ChannelContext` is passed to every `on_message()` call. It exposes:
- `send_message(text)` — fire-and-forget text to thread
- `send_event(event, **kwargs)` — fire-and-forget system event to thread

**Reasoning:** Adapters sometimes need to acknowledge receipt or inject system messages (e.g. "message delivered", "rate limited"). They must go through the same `Thread.add_message` path — no direct DB writes.

---

## 32. AGENT_RUN_RETRY events surfaced to thread on RetryableError

**Decision:** When `AgentContext.step()` catches a `RetryableError` propagating out of `ctx.run()`, it calls `_write_retry_event()` before re-raising. This writes an `AGENT_RUN_RETRY` system message to the thread via `_call_add_message`.

**Reasoning:** Without this, a retrying agent run is silent from the UI's perspective — the run appears to hang. Surfacing retries lets the user see that work is in progress and why it's delayed.

**Idempotency:** The key is `sha256(run_id:retry:step_name:int(time()))[:16]`. Using wall-clock seconds gives distinct keys across retries while being stable enough to avoid duplicates within the same retry window. These are best-effort notifications — not lifecycle state.

**Rule:** `_call_add_message` (HTTP) is called *outside* `ctx.run()`. Retryable errors propagate from `await ctx.run(name, fn)` — caught in `step()` after the durable boundary, not inside the callback.

---

## 33. No HTTP calls inside ctx.run() callbacks

**Decision:** `ctx.run()` callbacks must be pure functions. No `httpx`, no `_call_add_message`, no Restate ctx operations inside them.

**Reasoning:** On Restate replay, `ctx.run()` callbacks re-execute. HTTP calls inside callbacks fire again on every replay, defeating durable execution semantics. Even if idempotency keys protect against data corruption, the extra network calls add latency and can cause false retries.

**Affected fixes:**
- `restate_workflow.py`: `write_error_message` step removed — `_call_add_message` now called directly after `await ctx.run("mark_failed", ...)`.
- `restate_workflow.py`: `write_orphaned_message` step removed — `_write_orphaned_message` called directly after `await ctx.run("write_orphaned_event:...", ...)`.
- `context.py`: `_write_retry_event` called in `except RetryableError` on the `await ctx.run(...)` return, not inside `_guarded()`.
- `delivery.py`: `ChannelContext` constructed before `ctx.run("deliver", ...)`, not inside the callback.

---

## 34. _serialize skips relationship collections

**Decision:** `_serialize()` in `derive/restate.py` no longer recurses into SQLAlchemy relationship collections (one-to-many). It returns `None` for missing scalar relations, `_serialize(val)` for scalar relations, and skips collections entirely.

**Reasoning:** Recursing into collections causes the entire related graph to be serialized into the Restate journal and Pusher payload. This caused a Pusher 413 error. Collections are not needed in handler return values — they are not part of the domain action contract.

---

## 35. Agent implementations and channel adapters are in `services/`, not `platform/`

**Decision:**
- `services/agents/` — concrete agent implementations (stub, weather, etc.)
- `services/channels/adapters/` — concrete channel adapters (web, cli, webhook)
- `src/ironbridge/platform/` — infrastructure primitives only (base classes, registries, delivery, context)

**Reasoning:** Platform is infrastructure. It should contain no business logic and no concrete implementations. `services/` is where integrations live. This mirrors the separation between a framework and applications built on it.

**pyproject.toml:** Both `src/ironbridge` and `services` are declared as packages so both are on the Python path.

---

## 36. DeferredSendEffect for post-action effects that need the action result

**Decision:** `ActionContext.send_after(service, handler, key, factory)` queues a `DeferredSendEffect`. The `factory` callable receives the serialized result dict from `ctx.run()` and builds the effect arg at execution time.

**Reasoning:** `SendEffect` args are constructed before `ctx.run()` executes — before position is assigned. The first attempt patched position in `_execute_effects` by sniffing `effect.service == "ChannelDelivery"`, which violated domain boundaries. `DeferredSendEffect.factory(result)` is called *after* `ctx.run()` returns with the correct result, so position (or any other computed field) is available without `restate.py` knowing anything about `ChannelDelivery`.

**Consequence:** `thread.py` uses `send_after` with a `_deliver_arg` factory. `restate.py` calls `factory(result)` generically.

---

## 37. Message insert uses ON CONFLICT (thread_id, idempotency_key) DO NOTHING

**Decision:** `Message.Meta` sets `conflict_columns = ("thread_id", "idempotency_key")` and `conflict_action = "nothing"`. The repository issues `ON CONFLICT (thread_id, idempotency_key) DO NOTHING`.

**Reasoning:** After Restate purge, replay generates a new message `id` but the same `idempotency_key`. `ON CONFLICT (id) DO UPDATE` violated the unique constraint on `(thread_id, idempotency_key)`. `DO NOTHING` on the natural key is correct — if the message already exists in the DB (source of truth), skip silently.

**Consequence:** `ResourceMeta.__new__` was extended to parse `conflict_columns` and `conflict_action` from `Meta`. The repository checks for these before falling back to the PK upsert path.

---

## 38. Dead run recovery in _enqueue_run via Restate status API

**Decision:** Before queuing a new run, `_enqueue_run` calls `GET /AgentRun/{active_run_id}/status` via Restate ingress. If the status is not `"running"`, it clears `active_run_id` and `pending_runs` and fires immediately.

**Reasoning:** After Restate purge, `active_run_id` is set in Thread VirtualObject state but the workflow no longer exists. `_run_done` will never fire. Without this check the thread queue is permanently blocked. DB `agent_run_events` is a reflection of Restate state, not the authority — checking the DB run state would be wrong.

**Consequence:** Restate is the owner of run lifecycle. The status check adds one HTTP call per `_enqueue_run` invocation when a run is active, but this is inside `ctx.run()` so it is journaled and not repeated on replay.

---

## 39. ctx.generic_send replaces httpx.post for add_message in workflow handlers

**Decision:** `restate_workflow.py` uses `ctx.generic_send` + `AddMessageRequest.model_dump_json()` instead of `_call_add_message` (which used `httpx.post`).

**Reasoning:** `httpx.post` from inside a Restate workflow handler causes a deadlock-like timeout — the workflow is executing while trying to call back into the Restate ingress synchronously. This caused workflows to pause and `_run_done` to never fire, permanently blocking the thread queue.

**Consequence:** All message writes from workflow handlers go through `ctx.generic_send`. `_call_add_message` and `_write_orphaned_message` helpers were removed.

---

## 40. LLM provider via env vars

**Decision:** `LLM_MODEL`, `LLM_API_KEY`, `LLM_BASE_URL`, and `OPENROUTER_API_KEY` control the LLM. LiteLLM prefix conventions apply: `cerebras/<model>`, `openrouter/<provider>/<model>`.

**Note:** The plain weather agent (`weather_agent.py`) uses the raw OpenAI client — it strips the `openrouter/` prefix and sets `base_url` to OpenRouter manually. LiteLLM is not used there.

---

## 41. New human message auto-cancels the active run

**Decision:** In `_enqueue_run`, when a run is active and `"running"`, call `cancel()` on the active `AgentRun` workflow via `ctx.workflow_send` before clearing state and firing the new run. The pending queue is also drained.

**Reasoning:** If a run is suspended on a HITL promise and the user sends a new message, they are signalling intent to move on. Keeping the HITL run alive blocks the queue indefinitely. The user can re-answer the question by typing in the message box — the next run will re-read the full thread history and re-ask any disambiguation if needed.

**Consequence:** Only one `AgentRun` is ever active per thread at a time and the queue never grows beyond depth 1. The cancelled run's `AgentCancelledError` path fires cleanly, `_run_done` is sent, but since `active_run_id` is already cleared by the time it arrives, it is a no-op. `reply.approved` / `reply.selected` are never resolved for abandoned HITL promises — the workflow exits without resolving them, which is safe.

---

## 42. Dead run recovery uses workflow_call, cancel uses workflow_send

**Decision:** The status check in `_enqueue_run` uses `ctx.workflow_call(status_fn, ...)` (awaited, journaled) instead of `httpx.get`. The cancel uses `ctx.workflow_send(cancel_fn, ...)` (fire-and-forget, journaled).

**Reasoning:** HTTP calls inside Restate handlers must go through `ctx.run()` to be journaled. Using the SDK's `workflow_call`/`workflow_send` is already durable and avoids the extra `ctx.run()` wrapper. It also removes the dependency on `httpx` from this path entirely.

---

## 43. HITL supports arbitrary multiple-choice options

**Decision:** `ctx.request_approval(options=[...])` accepts any list of `{id, label}` pairs. Callers use `reply.selected[0]` to read the result, not `reply.approved` (which is only `True` for id `"approve"` or `"yes"`).

**Reasoning:** Binary approve/deny is too limiting. Location disambiguation, report format selection, and similar choices need N options. The HITL mechanism is generic — the option IDs are arbitrary strings.

**Consequence:** All multi-choice HITL consumers must check `reply.timed_out` and `reply.selected`, never `reply.approved`. `StubAgent` demonstrates this pattern: messages containing "choose", "pick", or "options" trigger a 4-option card.

---

## 44. Many-to-many thread ↔ channel bindings

**Decision:** `ChannelBinding` uses `UNIQUE(thread_id, channel_id)` — one thread can be bound to multiple channels (e.g. web UI + Discord). `resolve_channels_for_thread` returns `list[str]`. Thread.add_message fans out to all bound channels.

**Reasoning:** A thread is the canonical conversation. Multiple channel surfaces (web, Discord, Slack) may need to observe the same thread. A `UNIQUE(thread_id)` constraint would prevent this.

**Consequence:** The fanout loop in `Thread.add_message` uses a closure-capture fix (`cid: str = _channel_id` default arg) to avoid the classic Python loop-capture bug.

---

## 45. BaseChannelAdapter provides lifecycle helpers

**Decision:** `get_or_create_channel`, `new_thread`, and `bind_thread` live on `BaseChannelAdapter`, not on individual adapters.

**Reasoning:** Every adapter needs the same DB operations to provision its channel record and manage thread bindings. Duplicating this logic per adapter was error-prone.

**Consequence:** Adapters call `self.get_or_create_channel(tenant_id)` on first use (idempotent). `new_thread` creates a Restate Thread and binds it in one call. `bind_thread` uses `UNIQUE(thread_id, channel_id)` — safe to call on every inbound request.

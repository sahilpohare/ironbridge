# Ironbridge Architecture

## Problem

A multitenant platform where humans and AI agents collaborate over time. The
orchestration layer holds work together, survives restarts, and stays consistent
under concurrent input. Agents are participants — not privileged actors.

---

## Guiding Principles

1. **Domain is the API.** HTTP routes, DB schema, Restate handlers are all derived
   from domain resource definitions. No hand-written glue.
2. **Postgres is the source of truth.** Restate journals execution intent
   (exactly-once steps), not state. All reads go to Postgres.
3. **Tenant isolation is structural.** RLS enforced at the connection level via
   `app.tenant_id`. A missing `WHERE` clause returns zero rows, not all rows.
4. **Idempotency at the storage layer.** Duplicate intake caught by
   `UNIQUE(thread_id, idempotency_key)` — not application-level checks.
5. **Restate is an external system.** Domain and framework have zero Restate
   imports. Only `shared/derive/restate.py` and `shared/derive/restate_workflow.py`
   touch Restate primitives.
6. **HITL is part of the unit of work.** Human responses are regular messages in
   the thread log — not a side-channel.
7. **Thread is the model. Channel is the view.** All channels observe the same
   thread. What each channel renders is the adapter's decision, not the thread's.
8. **Platform is infrastructure. Services are implementations.** Platform holds
   base classes, registries, and derivation engines. Concrete agents and channel
   adapters live in `services/`.

---

## Bounded Contexts

```
src/ironbridge/
├── platform/
│   ├── identity/          # User, Tenant — auth, roles, lifecycle
│   ├── sessions/          # Thread, Message — conversation, ordering, HITL
│   ├── agents/            # Agent (definition), AgentRun (execution), BaseAgent, AgentContext
│   └── channels/          # Channel, ChannelBinding, ChannelDelivery, BaseChannelAdapter
│                          # ChannelContext, ChannelMessage, adapter registry
└── shared/
    ├── framework/         # ResourceMeta, Resource, @action, ActionKind, registry
    ├── derive/
    │   ├── restate.py          # Derives Restate VirtualObjects from Resource
    │   ├── restate_workflow.py # AgentRun Workflow + HITL wiring
    │   └── repository.py       # Generic SQLAlchemy upsert repository
    └── db.py                   # Engine, SessionLocal, tenant_session()

services/
├── agents/                # Concrete agent implementations
│   ├── stub.py            # StubAgent — registers as "stub"; demos binary + multi-choice HITL
│   └── weather_agent.py   # WeatherAgent — registers as "weather"; LLM tool use + location disambiguation HITL
└── channels/
    └── adapters/          # Concrete channel adapters
        ├── base.py             # BaseChannelAdapter ABC (get_or_create_channel, new_thread, bind_thread)
        ├── web.py              # WebAdapter — Pusher outbound, FastAPI inbound
        ├── cli.py              # CliAdapter — stdout outbound, REPL inbound
        ├── discord_adapter.py  # DiscordAdapter — discord.py gateway
        └── webhook.py          # WebhookAdapter — HTTP POST outbound
```

---

## Core Domain Model

### Thread

```
id              cuid          primary key = Restate VirtualObject key
tenant_id       string        injected by framework, RLS boundary
created_at      timestamptz
updated_at      timestamptz
```

Actions: `create`, `add_message`, `get`

### Message

```
id               cuid
thread_id        fk → threads(id) ON DELETE CASCADE
tenant_id        string        injected by framework, RLS boundary
participant_id   string        free string — "alice", "agent-run-xyz"
participant_type enum          HUMAN | AGENT | SYSTEM
role             enum          USER | ASSISTANT | SYSTEM
content          jsonb         {"version": 1, "parts": [...]}
position         bigint        strictly monotonic per thread
idempotency_key  string        unique per thread — storage-layer dedup
created_at       timestamptz
```

### Content Parts

| type               | writer  | meaning                                          |
|--------------------|---------|--------------------------------------------------|
| `text`             | any     | plain text content                               |
| `text_delta`       | agent   | streaming chunk                                  |
| `stream_end`       | agent   | streaming complete                               |
| `tool_call`        | agent   | tool invocation with input/output                |
| `reasoning`        | agent   | chain-of-thought                                 |
| `event`            | system  | lifecycle event (AGENT_RUN_QUEUED, FAILED, RETRY, ORPHANED, etc.) |
| `response_request` | agent   | HITL prompt — agent suspends, awaits reply       |
| `response_reply`   | human   | HITL response — resolves named promise, resumes  |

### Channel

```
id               cuid
tenant_id        string
name             string
channel_type     string        adapter slug — "web", "cli", "webhook"
config           jsonb         adapter credentials/settings
default_agent_id string        agent kicked off for inbound messages
status           enum          ACTIVE | INACTIVE
```

### ChannelBinding

```
id          cuid
thread_id   string   UNIQUE(thread_id, channel_id) — many-to-many
channel_id  string
created_at  timestamptz
```

One thread can be bound to multiple channels (e.g. web UI + Discord). Messages fan out to all bound channels. `resolve_channels_for_thread` returns `list[str]`.

---

## Channel Architecture

```
Thread (model — single source of truth)
  │
  │  add_message → for every message, fire-and-forget to ChannelDelivery
  ▼
ChannelDelivery (Restate Service — stateless, concurrent)
  │
  ├─ ChannelContext constructed (holds Restate ctx — must be outside ctx.run)
  └─ ctx.run("deliver") — journaled, deduplicated on retry:
       ├─ load Channel record from DB → get adapter type + config
       ├─ build ChannelMessage (Pydantic discriminated union of parts)
       └─ adapter.on_message(message, config, channel_ctx)
       │
       ├─ WebAdapter     → Pusher trigger → browser
       ├─ CliAdapter     → stdout
       ├─ DiscordAdapter → REST API POST to Discord channel
       └─ WebhookAdapter → HTTP POST to callback_url

Inbound (channel → thread):
  WebAdapter.get_router()      → /api/{tenant}/channels/web/bind
                               → /api/{tenant}/channels/web/send
  CliAdapter.run_cli()         → interactive REPL → receive() → Restate ingress
  DiscordAdapter.run_bot()     → discord.py gateway → receive() → Restate ingress
  BaseChannelAdapter.receive() → POST /Thread/{id}/add_message on Restate
```

**Key rule:** Adapters filter by role/type themselves. `ChannelDelivery` fans out
all messages without discrimination. What gets rendered is the adapter's concern.

---

## Request Lifecycle

```
Browser
  │
  │  POST /api/{tenant}/channels/web/send   ← FastAPI :9080
  ▼
WebAdapter.send()
  │
  └─ BaseChannelAdapter.receive()
       │
       └─ POST /Thread/{thread_id}/add_message  ← Restate ingress :8080
            │
            ▼
       derive/restate.py — _handle_add_message()
            │
            ├─ ctx.get("position") or recover MAX(position) from Postgres
            ├─ ctx.run("add_message") → INSERT INTO messages ON CONFLICT DO NOTHING
            ├─ ctx.set("position", next_pos)
            │
            ├─ [if response_reply] → AgentRun.resolve_hitl(...)
            │
            ├─ [if HUMAN + not response_reply]
            │    → ctx.generic_send(Thread, "_enqueue_run", run_req)
            │         │
            │         ├─ active run exists + running?
            │         │    → workflow_send(AgentRun.cancel, key=active)  ← SDK call
            │         │    → clear active_run_id + pending_runs
            │         ├─ active run dead? → clear state
            │         └─ fire workflow_send(AgentRun.run, key=run_id, ...)
            │
            └─ ctx.generic_send(ChannelDelivery, "deliver", ...)  ← ALL messages
                 (one send per bound channel — fanout)
```

---

## Agent Execution

```
AgentRun Workflow (Restate Workflow — one-shot, durable)
  │
  ├─ ctx.run("mark_running")     ← INSERT INTO agent_run_events (RUNNING)
  ├─ AgentContext constructed    ← wraps Restate ctx, exposes domain API only
  ├─ agent_registry.resolve(req.agent_id)
  └─ agent.run(agent_ctx)
       │
       ├─ ctx.step("fetch_history")  ← cancel-checked durable step
       │    on RetryableError → writes AGENT_RUN_RETRY to thread, re-raises
       ├─ ctx.step("llm_call_N")
       │
       ├─ [if tool requires approval OR multi-choice needed]
       │    ctx.request_approval(prompt, options=[{id, label}, ...])
       │      → write response_request part to thread
       │      → suspend on named promise hitl:{request_id}
       │      [human clicks card OR sends new message]
       │        case reply:    → response_reply → resolve_hitl → promise resolved → resume
       │        case new msg:  → _enqueue_run cancels this workflow → AgentCancelledError
       │
       └─ ctx.write_message(content) → Thread.add_message

  ├─ ctx.run("mark_completed/failed")
  └─ INSERT INTO agent_run_events (COMPLETED | FAILED)
```

---

## HITL Model

- Agent posts `response_request` part → thread timeline → UI renders prompt
- Human posts `response_reply` part → thread timeline → resolves named promise
- Both are regular `add_message` calls with same idempotency and ordering guarantees
- `request_id` IS the promise key (`hitl:{request_id}`) — no lookup table
- Orphaned runs: `resolve_hitl` checks workflow status; writes FAILED event + AGENT_RUN_ORPHANED system message

---

## Durability Model

Restate journals **which steps ran and their outputs**. On crash + restart:
- Completed `ctx.run()` steps → result replayed from journal, DB not re-written
- In-flight step → re-executes; idempotent upserts make this safe
- Suspended workflows (awaiting HITL) → resume from exact suspension point

**Rule:** every `ctx.run()` step must be idempotent.

---

## Multi-Tenant Isolation

```sql
SET LOCAL app.tenant_id = 'tenant-abc';  -- set once per connection

CREATE POLICY tenant_isolation ON threads
    USING (tenant_id = current_setting('app.tenant_id', true));
```

`tenants` — not RLS-scoped (it is the authority).
All other platform tables — RLS-scoped.

---

## Adding a Channel Adapter

```python
# services/channels/adapters/myservice.py
from services.channels.adapters.base import BaseChannelAdapter
from ironbridge.platform.channels.registry import register_adapter
from ironbridge.platform.channels.context import ChannelContext
from ironbridge.platform.channels.message import ChannelMessage, TextPart

class MyAdapter(BaseChannelAdapter):
    channel_type = "myservice"

    def on_message(self, message: ChannelMessage, config: dict, ctx: ChannelContext) -> None:
        if message.role != "ASSISTANT":
            return
        text = " ".join(p.text for p in message.parts if isinstance(p, TextPart))
        # deliver text to myservice

register_adapter(MyAdapter())
```

Import in `main.py` — that's it.

---

## Infrastructure

```
docker-compose:
  postgres:16-alpine     :5432   source of truth
  restate:latest         :8080   ingress (clients), :9070 admin
  app (hypercorn)        :9080   Restate SDK handler + FastAPI

Migrations: Alembic — autogenerate against live DB, RLS policies manual in 0001.
HTTP/2: hypercorn required — Restate uses h2 for handler invocation.
Registration: POST /deployments (Restate admin) after each deploy.
```

# Ironbridge

Multitenant AI agent orchestration platform. Humans and agents collaborate in
durable, ordered threads. The orchestration layer survives restarts, enforces
tenant isolation structurally, and deduplicates messages at the storage layer.

Channels are views of threads — every channel (web, CLI, Discord, webhook)
sees the same thread events and decides what to render.

---

## Quick Start

```bash
# Install
uv venv && uv pip install -e ".[dev]"

# Configure
cp .env.example .env
# edit PUSHER_*, CEREBRAS_API_KEY / LLM_API_KEY, etc.

# Start everything (postgres + restate + app, migrations + registration automatic)
podman compose up -d

# Open the UI
open http://localhost:9080
```

The UI is served directly from the app at `:9080`.

---

## Architecture Overview

```
Browser
  │
  ├── GET  /                          → frontend/index.html (static)
  ├── POST /api/{tenant}/channels/web/bind  → WebAdapter (create channel, bind thread)
  ├── POST /api/{tenant}/channels/web/send  → WebAdapter → Restate Thread
  │
  └── Pusher subscription             ← WebAdapter pushes outbound messages

Restate :8080
  └── /Thread/{id}/add_message → Thread VirtualObject
        ├── write message to Postgres
        ├── _enqueue_run (if HUMAN message) → AgentRun workflow
        └── fan out to ChannelDelivery (Service) → adapter.on_message()

AgentRun Workflow (durable, one-shot per message)
  └── agent.run(ctx)
        ├── fetch history from Postgres
        ├── call LLM / execute tools
        ├── request_approval() → HITL suspend/resume via named promise
        └── write_message() → Thread.add_message

_enqueue_run (Thread VirtualObject handler)
  ├── If no active run → fire AgentRun immediately
  ├── If active run is running → cancel it, drain queue, fire new run
  └── If active run is dead → clear state, fire new run
```

---

## Zero-Boilerplate DDD Framework

Inspired by [Ash](https://ash-hq.org), adapted to Python + SQLAlchemy + Restate.

**One class produces everything:**

```python
class Thread(Resource):
    class Meta:
        tenant_scoped  = True   # Postgres RLS policy, tenant_id injected
        restate_object = True   # Restate VirtualObject, one handler per action

    __tablename__ = "threads"
    id: Mapped[str] = mapped_column(String, primary_key=True)

    @action(kind=ActionKind.ACTION)
    def add_message(self, action_ctx: ActionContext, ...) -> Message:
        # pure domain logic — no imports from Restate, SQLAlchemy, or FastAPI
        action_ctx.send_workflow("AgentRun", key=run_id, arg={...})
        return msg
```

| Artifact | Derived from |
|---|---|
| SQLAlchemy ORM model | `Mapped[]` column declarations |
| `tenant_id` column + RLS policy | `Meta.tenant_scoped = True` |
| Upsert repository | ORM model |
| Restate `VirtualObject` + handler per action | `Meta.restate_object = True` + `@action` |
| Exclusive vs shared concurrency | `ActionKind` |
| Effect execution (workflow starts, sends) | `ActionContext` — zero Restate imports in domain |

**Key properties:**
- Domain has no infrastructure imports. Actions declare effects via `ActionContext`.
- Effects are data, not calls. Infrastructure executes them after the DB write — atomically, journaled.
- Tenant isolation is structural. `tenant_id` injected by metaclass, enforced by Postgres RLS.
- Idempotency at the storage layer. All saves are `INSERT ... ON CONFLICT DO UPDATE`.

---

## Writing an Agent

```python
# src/ironbridge/agents/my_agent.py
from ironbridge.platform.agents.base import BaseAgent
from ironbridge.platform.agents.context import AgentContext
from ironbridge.platform.agents.registry import agent_registry


class MyAgent(BaseAgent):
    async def run(self, ctx: AgentContext) -> None:
        history = await ctx.step("fetch_history", ctx.get_history)
        # ... LLM call, tool use, HITL ...
        ctx.write_message({"version": 1, "parts": [{"type": "text", "text": "Done."}]}, 0)


agent_registry.register("my_agent", MyAgent)
```

Import in `main.py` to trigger registration. No Restate imports in agent code.

### AgentContext API

| Method | Description |
|---|---|
| `await ctx.step(name, fn)` | Durable step — journaled, checks cancel before running |
| `await ctx.run(name, fn)` | Durable step — journaled, no cancel check (use for setup/teardown) |
| `await ctx.get_history()` | Fetch thread messages from Postgres (strips control parts) |
| `ctx.write_message(content, position)` | Write assistant message to thread |
| `await ctx.request_approval(prompt, options, ...)` | Suspend workflow, show HITL card |

---

## HITL (Human-in-the-Loop)

HITL is message-driven — part of the thread timeline, not a side-channel.

1. Agent calls `ctx.request_approval(prompt, options)` → writes `response_request` part → workflow suspends
2. UI renders the approval card
3. Human clicks → `response_reply` part written → named promise resolved → workflow resumes

### Binary approve/reject

```python
reply = await ctx.request_approval(
    prompt="Call get_weather for London?",
    options=[
        {"id": "approve", "label": "Approve"},
        {"id": "reject",  "label": "Reject"},
    ],
)
if reply.approved:   # True only for id == "approve" or "yes"
    result = get_weather("London")
```

### Multiple-choice options

```python
choice = await ctx.request_approval(
    prompt="Which report format?",
    options=[
        {"id": "pdf",    "label": "PDF"},
        {"id": "csv",    "label": "CSV"},
        {"id": "email",  "label": "Email digest"},
        {"id": "cancel", "label": "Cancel"},
    ],
    timeout=timedelta(hours=24),
)
selected = choice.selected[0] if choice.selected else "cancel"
if choice.timed_out or selected == "cancel":
    return
generate_report(format=selected)
```

Use `reply.selected` (not `reply.approved`) for arbitrary choices. `StubAgent` demonstrates this: send a message containing "choose", "pick", or "options".

### Run cancellation on new message

If a run is suspended on HITL and the user sends a new message, the active run is automatically cancelled. The next run re-reads the full thread history and can re-ask the question. Users can answer HITL questions by typing in the message box — the next run will see the response in history.

---

## Writing a Channel Adapter

A channel is a view of a thread. Every message is fanned out to all bound channels.

```python
# services/channels/adapters/myservice.py
from ironbridge.platform.channels.registry import register_adapter
from services.channels.adapters.base import BaseChannelAdapter


class MyAdapter(BaseChannelAdapter):
    channel_type = "myservice"

    def on_message(self, message, config, ctx) -> None:
        if message.role != "ASSISTANT":
            return
        text = " ".join(p.text for p in message.parts if isinstance(p, TextPart))
        httpx.post(config["webhook_url"], json={"text": text}, timeout=10)


register_adapter(MyAdapter())
```

Import in `main.py`. The `channel_type` string must match `Channel.channel_type` in DB.

### Base class lifecycle helpers

| Method | Description |
|---|---|
| `get_or_create_channel(tenant_id)` | Returns channel_id for this channel_type, creating the Channel record if needed. Idempotent. |
| `new_thread(tenant_id, channel_id)` | Creates a Restate Thread + binding, returns thread_id. Use for `/new` commands. |
| `bind_thread(tenant_id, thread_id, channel_id)` | Inserts ChannelBinding. Idempotent — safe on every request. |
| `receive(content, thread_id, tenant_id, participant_id, ...)` | Posts inbound message to Restate ingress. |

Threads have a many-to-many relationship with channels (`UNIQUE(thread_id, channel_id)`). One thread can be visible in both the web UI and a Discord channel simultaneously.

### Adapter checklist

- [ ] `channel_type` set — unique string, matches DB `Channel.channel_type`
- [ ] `register_adapter(MyAdapter())` at module level
- [ ] Import in `main.py`
- [ ] `on_message` never raises — log and continue on delivery failures
- [ ] No Restate ctx ops inside `ctx.run()` callbacks

---

## HTTP API

Core thread operations go through Restate ingress (`:8080`).
Browser-facing endpoints go through FastAPI (`:9080/api/`).

### Thread (via Restate)

```bash
# Create
POST http://localhost:8080/Thread/{thread_id}/create
{"tenant_id": "tenant-a", "user_name": "alice"}

# Add message
POST http://localhost:8080/Thread/{thread_id}/add_message
{"participant_id": "alice", "participant_type": "HUMAN", "role": "USER",
 "content": {"version": 1, "parts": [{"type": "text", "text": "Hello"}]},
 "idempotency_key": "msg-001", "tenant_id": "tenant-a", "user_name": "alice",
 "agent_id": "weather"}

# Get
POST http://localhost:8080/Thread/{thread_id}/get
{"tenant_id": "tenant-a", "user_name": "alice"}

# List all threads for tenant
POST http://localhost:8080/Thread/_/list
{"tenant_id": "tenant-a", "user_name": "alice"}
```

### Web Channel (via FastAPI)

```bash
# Bind thread to web channel (idempotent — call on every openThread())
POST /api/{tenant}/channels/web/bind
X-Tenant-Id: tenant-a
X-User-Name: alice
{"thread_id": "thread-xyz"}

# Send inbound message from browser
POST /api/{tenant}/channels/web/send
X-Tenant-Id: tenant-a
X-User-Name: alice
{"thread_id": "thread-xyz", "text": "Hello", "participant_id": "alice", "agent_id": "weather"}
```

---

## Multi-Tenant Isolation

Isolation is structural — not a filter added by application code.

```sql
SET LOCAL app.tenant_id = 'tenant-abc';

CREATE POLICY tenant_isolation ON threads
    USING (tenant_id = current_setting('app.tenant_id', true));
```

```python
with tenant_session("tenant-abc") as db:
    repo = SqlAlchemyRepository(db, Thread)
    threads = repo.list()   # no WHERE clause needed — RLS filters automatically
```

---

## Testing

```
tests/
├── platform/          Unit tests — no DB, no Restate, no HTTP
│   ├── agents/        AgentRegistry, AgentContext, StubAgent helpers
│   ├── channels/      Adapter registry, channel_type contract
│   ├── identity/      Tenant, User domain model
│   └── sessions/      Thread, Message domain model, content schema
└── integration/       Require running postgres (and optionally Restate)
```

```bash
# Unit tests only
uv run pytest tests/platform/ -v

# Integration tests (requires postgres)
uv run pytest tests/integration/test_tenant_isolation.py \
               tests/integration/test_recording_adapter.py -v

# Full suite (requires postgres + restate + app)
uv run pytest tests/integration/ -v
```

---

## Project Structure

```
src/ironbridge/
├── shared/
│   ├── db.py                    tenant_session(), SQLAlchemy engine
│   ├── framework/               Resource, @action, ActionKind, registry
│   └── derive/
│       ├── restate.py           Resource → Restate VirtualObject + queue handlers
│       ├── restate_workflow.py  AgentRun Workflow
│       └── repository.py        SqlAlchemyRepository
└── platform/
    ├── identity/                Tenant, User
    ├── sessions/                Thread, Message
    ├── agents/                  BaseAgent, AgentContext, AgentRegistry, HITL
    └── channels/                Channel, ChannelBinding, ChannelDelivery,
                                 ChannelContext, ChannelMessage, adapter registry

services/
├── agents/                      Concrete agent implementations
│   ├── stub.py                  StubAgent — reference impl, HITL demo
│   └── weather_agent.py         WeatherAgent — LLM tool use + location disambiguation
└── channels/
    └── adapters/                Concrete channel adapters
        ├── base.py              BaseChannelAdapter
        ├── web.py               WebAdapter (Pusher)
        ├── cli.py               CliAdapter (stdout REPL)
        └── discord_adapter.py   DiscordAdapter (discord.py gateway)

frontend/
└── index.html                   Single-file UI (Pusher, HITL cards, thread sidebar)

alembic/
└── versions/                    DB migrations

docs/
└── decisions.md                 ADR log (45 decisions)
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | Postgres connection string |
| `RESTATE_URL` | Restate server base URL (default: `http://localhost:8080`) |
| `PUSHER_APP_ID` | Pusher app id |
| `PUSHER_KEY` | Pusher key (also used in frontend JS) |
| `PUSHER_SECRET` | Pusher secret |
| `PUSHER_CLUSTER` | Pusher cluster (default: `eu`) |
| `LLM_API_KEY` | API key for LLM provider (or `CEREBRAS_API_KEY`) |
| `LLM_BASE_URL` | LLM provider base URL (default: Cerebras) |
| `LLM_MODEL` | Model name (default: `llama3.1-8b`) |

---

## Development Notes

- **New thread IDs after code changes** — Restate journals are tied to handler code. Use a new thread ID or `podman compose down -v` to clear journals + DB.
- **Rebuild after Python changes** — `podman compose build app && podman compose up -d app` — registration runs automatically on startup.
- **`TerminalError` must be re-raised** — never swallow it in Restate handlers.
- **No HTTP calls inside `ctx.run()` callbacks** — they re-execute on replay.
- **No Restate ctx ops inside `ctx.run()` callbacks** — construct `ChannelContext` before the `ctx.run()` call.

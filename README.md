# Order System — FastAPI + Kafka + SQLite

A real-time order processing system built to learn Kafka in a production-like context.
Orders flow from an HTTP API through Kafka to independent worker services — asynchronously,
with retry logic, dead letter queue, and fan-out consumer groups.

---

## What it does

1. User places an order via REST API → saved to DB as `pending`
2. Order event published to Kafka `orders` topic
3. `order_worker` reads it → validates → updates DB to `confirmed` or `failed`
4. `notification_worker` reads `orders.confirmed` → simulates sending email
5. `analytics_worker` reads `orders` → tracks revenue and order counts
6. Failed orders go to `orders.dlq` → viewable and replayable via admin API

---

## Architecture

```
Client (HTTP)
     │
     ▼
FastAPI API ──────────────────────────────────────────────────────┐
  POST /orders                                                      │
  GET  /orders/{id}                                                 │
  GET  /orders/                                                     │ writes
  GET  /admin/dlq                                                   │
  POST /admin/dlq/{id}/replay                                       │
     │                                                         SQLite DB
     │ produce                                                       │
     ▼                                                              │
"orders" topic (Kafka)                                             │
     │                                                              │
     ├──── order_worker (group: order-processors) ─────────────────┤
     │       validates → confirmed/failed                           │ reads/writes
     │       produces to orders.confirmed or orders.dlq             │
     │                                                              │
     └──── analytics_worker (group: analytics-service)             │
             tracks stats in memory                                 │
                                                                    │
"orders.confirmed" topic (Kafka)                                   │
     │                                                              │
     └──── notification_worker (group: notification-service)       │
             simulates sending email                                │
                                                                    │
"orders.dlq" topic (Kafka)                                         │
     └──── admin API reads on demand (GET /admin/dlq)
```

---

## Project Structure

```
order-system/
├── docker-compose.yml          # Kafka (KRaft) + Kafbat UI
├── pyproject.toml              # dependencies (uv)
├── .env.example                # env var template
│
└── app/
    ├── main.py                 # FastAPI app + lifespan (Kafka producer init/close)
    ├── database.py             # SQLite engine + get_session dependency
    ├── auth.py                 # JWT + password hashing helpers
    │
    ├── core/
    │   ├── config.py           # env vars → Python constants
    │   └── kafka.py            # producer singleton (init, get, close)
    │
    ├── models/
    │   ├── user_model.py       # UserBase, UserCreate, UserRead, User (table)
    │   └── order_model.py      # OrderBase, OrderCreate, OrderRead, Order (table), OrderEvent
    │
    ├── routes/
    │   ├── auth.py             # POST /auth/register, POST /auth/login
    │   ├── orders.py           # GET /orders/, GET /orders/{id}, POST /orders/
    │   └── admin.py            # GET /admin/dlq, POST /admin/dlq/{id}/replay
    │
    └── workers/
        ├── order_worker.py         # consumes "orders" → validates → DB + confirmed/dlq
        ├── notification_worker.py  # consumes "orders.confirmed" → email simulation
        └── analytics_worker.py     # consumes "orders" → in-memory stats
```

---

## Kafka Topics

| Topic | Partitions | Key | Producer | Consumers |
|-------|-----------|-----|----------|-----------|
| `orders` | 1 | `order_id` (UUID) | API | order_worker, analytics_worker |
| `orders.confirmed` | 1 | `order_id` | order_worker | notification_worker |
| `orders.dlq` | 1 | `order_id` | order_worker | admin API (on demand) |

Topics are auto-created on first produce. In production, create explicitly with higher partition counts.

---

## Kafka Concepts Used

| Concept | Where in code |
|---------|--------------|
| Producer singleton | `app/core/kafka.py` — one connection reused across all requests |
| `produce()` + `poll(0)` | `routes/orders.py` — non-blocking, doesn't delay HTTP response |
| `flush()` on shutdown | `core/kafka.py` `close_producer()` — clean shutdown |
| Consumer poll loop | All 3 workers — `while True: consumer.poll(1.0)` |
| Manual commits | All workers — `enable.auto.commit=False`, commit after processing |
| At-least-once | Worker crash before commit → message replayed on restart |
| Consumer groups | 3 different `group.id`s → fan-out on `orders` topic |
| Keys + partitioning | `order_id` as key → all events for an order go to same partition |
| JSON serialization | `OrderEvent.to_kafka_bytes()` → `model_dump_json().encode('utf-8')` |
| DLQ pattern | Failed orders after 3 retries → `orders.dlq` with full error context |
| Retry logic | `order_worker` retries 3 times before DLQ |
| Replay | `POST /admin/dlq/{id}/replay` republishes to `orders` topic |

---

## Order Status Flow

```
POST /orders → status: "pending"
                    ↓
            order_worker reads from Kafka
                    ↓
         validate_order() passes?
           YES → status: "confirmed"
                 event → orders.confirmed
           NO (after 3 retries) → status: "failed"
                                   event → orders.dlq
```

---

## Setup & Running

### Prerequisites
- Docker Desktop
- Python 3.11+
- `uv` package manager

### 1. Clone and install
```bash
git clone <repo>
cd order-system
uv sync
```

### 2. Create .env
```bash
cp .env.example .env
# Edit .env and set a real SECRET_KEY
```

### 3. Start Kafka
```bash
docker compose up -d
# Kafbat UI → http://localhost:8080
```

### 4. Run everything (4 terminals)

**Terminal 1 — API**
```bash
uv run uvicorn app.main:app --reload
```

**Terminal 2 — Order worker**
```bash
uv run python -m app.workers.order_worker
```

**Terminal 3 — Notification worker**
```bash
uv run python -m app.workers.notification_worker
```

**Terminal 4 — Analytics worker**
```bash
uv run python -m app.workers.analytics_worker
```

### 5. Test via Swagger
Open `http://localhost:8000/docs`

---

## API Endpoints

### Auth
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/auth/register` | Register a new user |
| POST | `/auth/login` | Login, returns JWT token |

### Orders
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/orders/` | Place a new order |
| GET | `/orders/` | List all your orders |
| GET | `/orders/{order_id}` | Get order status |

### Admin
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/admin/dlq` | View all failed orders |
| POST | `/admin/dlq/{order_id}/replay` | Replay a failed order |

All endpoints except register/login require `Authorization: Bearer <token>` header.

---

## Testing the DLQ

Place an order that fails business validation:

```json
POST /orders
{"items": ["diamond"], "total": 99999}
```

Watch `order_worker` retry 3 times then send to DLQ. Then:

```
GET /admin/dlq                          → see the failed order
POST /admin/dlq/{order_id}/replay       → reprocess it
```

---

## Known Limitations (learning project)

- **SQLite** — single file DB, not suitable for multi-process writes at scale. Use PostgreSQL in production.
- **In-memory analytics** — `analytics_worker` stats reset on restart. Persist to DB in production.
- **Dual-write risk** — DB write and Kafka produce are not atomic. A crash between the two can cause a duplicate email. Fix with idempotency checks or Kafka transactions.
- **Single partition** — all topics have 1 partition. Scale by creating topics explicitly with 3+ partitions.
- **No auth on admin routes** — any logged-in user can view/replay DLQ. Add admin role check in production.

# WS Service — Documentation

> **Type:** Kafka Consumer + WebSocket (Socket.IO) Server  
> **Default Port:** `5504`  
> **Kafka Consumer Group ID:** `ws-service`  
> **WebSocket Library:** [Socket.IO](https://socket.io)

## Overview

The WS Service is a **real-time push notification gateway**. It:

1. Subscribes to the `order` Kafka topic
2. Maintains persistent **WebSocket connections** with browser clients (admin-ui)
3. When an order event arrives, **emits it in real-time** to the correct restaurant room

There are **no HTTP REST endpoints** — communication is entirely over WebSocket (Socket.IO protocol).

```
┌─────────────┐   Kafka (order topic)   ┌────────────────┐  Socket.IO   ┌──────────────┐
│ Order Svc   │ ──────────────────────▶ │   WS Service   │ ───────────▶ │  Admin UI    │
│             │                         │  (Port 5504)   │              │  Dashboard   │
└─────────────┘                         └────────────────┘              └──────────────┘
```

---

## 📡 WebSocket Interface (Socket.IO)

### Connection

Clients connect using the Socket.IO client library:

```javascript
import { io } from "socket.io-client";

const socket = io("http://localhost:5504");
```

### CORS

Only the following origins are allowed (configured via `frontend.clientUI` and `frontend.adminUI`):

| Origin | Default Value |
|---|---|
| Client UI | `http://localhost:5173` |
| Admin UI | `http://localhost:5174` |

---

## 📤 Client → Server Events

Events sent **from** the browser client **to** the WS service.

---

### `join`

Join a tenant-specific room to receive order updates for that restaurant.

**Must be emitted after connecting** — the client will not receive any order updates until it has joined a room.

**Payload:**

```javascript
socket.emit("join", { tenantId: "1" });
```

| Field | Type | Description |
|---|---|---|
| `tenantId` | string | The restaurant ID to subscribe to |

**Server response** (server emits `join` back to confirm):

```javascript
socket.on("join", (data) => {
  console.log(data); // { roomId: "1" }
});
```

| Field | Type | Description |
|---|---|---|
| `roomId` | string | The room that was joined (same as `tenantId`) |

**Full connection + join example:**

```javascript
const socket = io("http://localhost:5504");

socket.on("connect", () => {
  console.log("Connected:", socket.id);

  // Join the restaurant's room immediately after connecting
  socket.emit("join", { tenantId: "1" });
});

socket.on("join", (data) => {
  console.log("Joined room:", data.roomId);
});
```

---

## 📥 Server → Client Events

Events pushed **from** the WS service **to** the browser client.

---

### `order-update`

Emitted to all clients in the matching tenant room whenever an order event is received from Kafka.

**Trigger:** Any message on the `order` Kafka topic with a matching `tenantId`.

**Payload:**

```javascript
socket.on("order-update", (data) => {
  console.log(data);
});
```

**Payload structure:**

```json
{
  "event_type": "ORDER_CREATE | ORDER_STATUS_UPDATE | PAYMENT_STATUS_UPDATE",
  "data": {
    "_id": "65f1a2b3c4d5e6f7a8b9c0d4",
    "cart": [ ... ],
    "tenantId": "1",
    "total": 584,
    "paymentMode": "card",
    "orderStatus": "received",
    "paymentStatus": "pending",
    "customerId": {
      "_id": "65f1a2b3c4d5e6f7a8b9c0d5",
      "email": "swarup@example.com",
      "firstName": "Swarup",
      "lastName": "Das"
    },
    "createdAt": "2024-01-01T00:00:00.000Z"
  }
}
```

**`event_type` values:**

| Value | Trigger |
|---|---|
| `ORDER_CREATE` | New order placed via `POST /orders` |
| `ORDER_STATUS_UPDATE` | Order status changed via `PATCH /orders/change-status/:id` |
| `PAYMENT_STATUS_UPDATE` | Stripe payment completed via webhook |

---

## 📨 Kafka Consumer

### Topic Subscribed

| Topic | Description |
|---|---|
| `order` | All order lifecycle events published by order-service |

### Consumer Group

```
ws-service
```

### Message Flow

```
Kafka message received
       │
       ▼
topic === "order"?
       │ Yes
       ▼
Parse JSON payload
       │
       ▼
Extract tenantId from data
       │
       ▼
io.to(tenantId).emit("order-update", payload)
       │
       ▼
All browser clients in that room receive the event
```

---

## 🔧 Configuration Reference

All config is loaded by the `config` npm package from the `config/` directory.

| Config Key | Env Variable | Default (dev) | Description |
|---|---|---|---|
| `server.port` | `PORT` | `5504` | Port the Socket.IO server listens on |
| `kafka.broker` | `KAFKA_BROKER` | `["localhost:9092"]` | Kafka broker address(es) (JSON array) |
| `kafka.sasl.username` | `KAFKA_SASL_USERNAME` | — | Kafka SASL username (production only) |
| `kafka.sasl.password` | `KAFKA_SASL_PASSWORD` | — | Kafka SASL password (production only) |
| `frontend.clientUI` | `CLIENT_UI_DOMAIN` | `http://localhost:5173` | Allowed CORS origin (client UI) |
| `frontend.adminUI` | `ADMIN_UI_DOMAIN` | `http://localhost:5174` | Allowed CORS origin (admin UI) |

> In **production**, Kafka uses SSL + SASL/PLAIN authentication. Set `NODE_ENV=production` to activate this.

---

## 🚀 How to Run

```bash
# Install dependencies
npm install

# Development (watches for changes)
NODE_ENV=development npm run dev

# Production
NODE_ENV=production npm start
```

Ensure **Kafka** is running before starting:

```bash
docker-compose up -d kafka
```

---

## 🧪 How to Test

There are no REST endpoints — testing is done via the browser console or a Socket.IO client.

### Option 1 — Browser Console (Admin UI)

The admin-ui already connects to this service via `VITE_SOCKET_SERVICE_URL`. Open DevTools console on `http://localhost:5174`:

```javascript
// The admin-ui imports socket.io-client automatically
// Access the existing socket or create a new one:
const { io } = await import("https://cdn.socket.io/4.7.2/socket.io.esm.min.js");

const socket = io("http://localhost:5504");

socket.on("connect", () => {
  console.log("Connected:", socket.id);
  socket.emit("join", { tenantId: "1" }); // join restaurant 1's room
});

socket.on("join", (d) => console.log("Joined:", d));
socket.on("order-update", (d) => console.log("Order update:", d));
```

Then place an order via order-service — the `order-update` event should appear instantly.

### Option 2 — Postman WebSocket

1. In Postman, click **New → WebSocket Request**
2. Enter URL: `ws://localhost:5504/socket.io/?EIO=4&transport=websocket`
3. Connect and send Socket.IO handshake messages manually

> Note: Socket.IO uses a custom protocol on top of WebSocket. Using the browser method (Option 1) is much easier.

### Option 3 — Node.js Script

```javascript
// test-ws.js
const { io } = require("socket.io-client");

const socket = io("http://localhost:5504");

socket.on("connect", () => {
  console.log("Connected:", socket.id);
  socket.emit("join", { tenantId: "1" });
});

socket.on("join", (data) => console.log("Room joined:", data));
socket.on("order-update", (data) => console.log("Order update received:", JSON.stringify(data, null, 2)));
socket.on("disconnect", () => console.log("Disconnected"));
```

```bash
node test-ws.js
```

Then place an order from a separate terminal to trigger the event.

---

## 🔗 Integration with Admin UI

The admin-ui connects using the `VITE_SOCKET_SERVICE_URL` environment variable:

```typescript
// admin-ui/src/lib/socket.ts
import { io } from "socket.io-client";

const socket = io(import.meta.env.VITE_SOCKET_SERVICE_URL);
// = "http://localhost:5504"
```

After connecting, the admin-ui joins the restaurant room:

```typescript
socket.emit("join", { tenantId: restaurantId });
socket.on("order-update", (order) => {
  // update the orders list in real-time
});
```

---

## 🗂️ Project Structure

```
ws-service/
├── server.ts                    # Entry point — starts Kafka consumer + Socket.IO server
├── src/
│   ├── config/
│   │   ├── kafka.ts             # KafkaBroker class (dispatches order-update events)
│   │   └── logger.ts            # Winston logger
│   ├── factories/
│   │   └── broker-factory.ts    # Singleton Kafka broker factory
│   ├── socket.ts                # Socket.IO server setup + CORS + room management
│   └── types/                   # TypeScript interfaces
├── config/
│   ├── development.yaml         # Dev config (committed)
│   ├── test.yaml                # Test config (committed)
│   ├── production.yaml          # Prod config (gitignored — use env vars)
│   └── custom-environment-variables.yaml  # Maps env vars to config keys
└── .env.example                 # All required environment variables
```

---

## 📋 Event Reference Summary

### Client → Server

| Event | Payload | Description |
|---|---|---|
| `join` | `{ tenantId: string }` | Join a restaurant's order update room |

### Server → Client

| Event | Payload | Description |
|---|---|---|
| `join` | `{ roomId: string }` | Confirmation that client joined the room |
| `order-update` | Full order event object | Real-time order event from Kafka |

### Kafka → Service

| Topic | Event Types | Action |
|---|---|---|
| `order` | `ORDER_CREATE` | Emits `order-update` to tenant room |
| `order` | `ORDER_STATUS_UPDATE` | Emits `order-update` to tenant room |
| `order` | `PAYMENT_STATUS_UPDATE` | Emits `order-update` to tenant room |

# Kafka Transaction Dashboard - Backend

ASP.NET Core backend for the Real-Time Transaction Dashboard. Bundles Apache Kafka (via Redpanda) and exposes a SignalR hub for live browser updates.

---

## What It Does

Runs a full Kafka pipeline inside a single Docker container:

- **Producer** - generates a fake payment transaction every 2 seconds
- **Kafka topic** - `transactions` (1 partition, hosted by Redpanda)
- **3 Consumers** - each independently reads every event:
  - `FeedConsumerService` - pushes raw transactions to the browser
  - `StatsConsumerService` - tracks total volume, success rate, failure count
  - `AnomalyConsumerService` - detects users with 3+ failures in 60 seconds
- **SignalR hub** at `/hub` - broadcasts all updates to connected browsers in real time

---

## Project Structure

```
backend/
├── Program.cs                  # App setup, CORS, endpoints
├── Dockerfile                  # Ubuntu + .NET 10 + Redpanda
├── start.sh                    # Starts Redpanda, creates topic, starts .NET
├── appsettings.json            # Kafka connection config
├── Models/
│   └── Transaction.cs          # Transaction data model
├── Hubs/
│   └── DashboardHub.cs         # SignalR hub
└── Services/
    ├── ProducerState.cs         # Singleton flag (start/stop)
    ├── KafkaClientHelper.cs     # Shared Kafka config builder
    ├── ProducerService.cs       # Publishes transactions to Kafka
    ├── FeedConsumerService.cs   # Streams feed to browser
    ├── StatsConsumerService.cs  # Computes running stats
    └── AnomalyConsumerService.cs# Fraud/anomaly detection
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Health check (used for cold-start detection) |
| `POST` | `/producer/start` | Start generating transactions |
| `POST` | `/producer/stop` | Stop generating transactions |
| `WS` | `/hub` | SignalR WebSocket hub |

### SignalR Events (server → browser)

| Event | Payload |
|---|---|
| `NewTransaction` | `{ id, user, amount, merchant, status, region, time }` |
| `StatsUpdated` | `{ totalAmount, totalCount, successRate, failureCount }` |
| `AnomalyDetected` | `{ user, failureCount, detectedAt }` |

---

## Tech Stack

| | |
|---|---|
| Runtime | .NET 10 (ASP.NET Core) |
| Message broker | Redpanda (Kafka-compatible, ~150MB RAM) |
| Kafka client | Confluent.Kafka |
| Real-time push | SignalR |
| Container | Docker (Ubuntu 22.04) |
| Hosting | Render (free tier) |

---

## Running Locally

**Prerequisites:** .NET 10, Docker

```bash
# Start Redpanda (Kafka)
docker run -d -p 9092:9092 --name redpanda \
  redpandadata/redpanda:latest \
  redpanda start --overprovisioned --smp 1 \
  --memory 200M --reserve-memory 0M \
  --node-id 0 --check=false

# Run the backend
dotnet run
```

Backend runs on `http://localhost:5218`.

---

## Docker (Production)

The Dockerfile bundles Redpanda and the .NET app into a single container. `start.sh` handles startup sequencing: Redpanda first, topic creation, then the .NET process.

```bash
dotnet publish -c Release -o publish
docker build -t kafka-backend .
docker run -p 5218:5218 kafka-backend
```

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `FRONTEND_URL` | Allowed CORS origin (your Vercel URL) | `http://localhost:5173` |
| `PORT` | Port the app listens on | `5218` |

---

## Frontend

The React frontend that connects to this backend:
[github.com/RachReddy/kafka-dashboard-frontend](https://github.com/RachReddy/kafka-dashboard-frontend)

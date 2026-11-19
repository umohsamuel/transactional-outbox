# Transactional Outbox Pattern in Golang with Debezium

This repository is the companion code for my article, [Reliable Event Publishing: Transactional Outbox Pattern in Golang with Debezium](https://www.umohsg.com/blog/reliable-event-publishing-transactional-outbox-pattern-in-golang-with-debezium-060a240f-73a3-4812-a7d6-8329fbfeea27). The article explains the pattern and walks through every file in detail, so read it if you want the full picture.

## What It Does

When a customer places an order, the order service saves the order and writes an `OrderCreated` event to an outbox table in the same Postgres transaction. Debezium reads the outbox row from the Postgres write-ahead log and publishes the event to Kafka through Kafka Connect. The notification service consumes the event from Kafka.

The order service never talks to Kafka. If Kafka Connect goes down, orders still succeed, and the events are published once it comes back.

## Project Structure

```bash
.
├── docker-compose.yaml              # Kafka, Kafka Connect and Kafka UI
├── Makefile
├── go.work
├── debezium/
│   └── order-outbox-connector.json  # Debezium connector config
└── services/
    ├── order/                       # saves orders and writes to the outbox
    └── notification/                # consumes the events from Kafka
```

## Prerequisites

- Go 1.27 or later
- Docker and Docker Compose
- [goose](https://github.com/pressly/goose) for running the migrations

## Running It

### 1. Set up the environment files

Create a `.env` file at the root of the project with the password for the Debezium database user:

```bash
DEBEZIUM_USER_PASSWORD=debezium
```

Then create `services/order/.env`:

```bash
PORT=8080

DB_USER=postgres
DB_PASSWORD=postgres
DB_DATABASE=order_db
DB_HOST=localhost
DB_PORT=5432
DB_SCHEMA=public
DB_SSLMODE=disable

GOOSE_DRIVER=postgres
GOOSE_DBSTRING="host=localhost port=5432 user=postgres password=postgres dbname=order_db sslmode=disable"
GOOSE_MIGRATION_DIR=./internal/adapters/db/migrations
```

### 2. Start the infrastructure

This creates the shared Docker network and starts Kafka, Kafka Connect and Kafka UI:

```bash
make infra
```

Kafka Connect takes a little while to be ready after the container starts. This command waits until its API responds:

```bash
until curl -s http://localhost:8083/ > /dev/null; do sleep 2; done
```

### 3. Start the order service and run the migrations

```bash
make order
cd services/order
goose up
cd ../..
```

The migrations create the `orders` table, the `outbox` table and the `order_outbox` publication.

### 4. Create the Debezium database user

Connect to the order database:

```bash
docker exec -it order_pg psql -U postgres -d order_db
```

Then create the user with the same password you put in the root `.env` file:

```sql
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT CONNECT ON DATABASE order_db TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.outbox TO debezium;
```

You only need to do this once per database volume.

### 5. Register the connector

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @debezium/order-outbox-connector.json \
  http://localhost:8083/connectors
```

Check that the connector and its task are both `RUNNING`:

```bash
curl http://localhost:8083/connectors/order-outbox-connector/status
```

### 6. Start the notification service

```bash
make notification
docker logs -f notification_app
```

## Testing It

In another terminal, place an order:

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "3f1c2a4e-8b7d-4c6e-9a1f-2b3c4d5e6f70", "amount": 5000}'
```

A moment later, the notification service logs the event. You can also see it in Kafka UI at `http://localhost:8081`, under Topics > `outbox.event.order` > Messages.

To see why the pattern matters, stop Kafka Connect with `docker stop connect` and place a few orders. They still succeed. Start it again with `docker start connect`, and the missed events show up in the notification logs in the order you placed them.

## Make Targets

| Target              | What it does                                             |
| ------------------- | -------------------------------------------------------- |
| `make network`      | Creates the shared `outbox_infra` network                |
| `make infra`        | Starts Kafka, Kafka Connect and Kafka UI                 |
| `make order`        | Builds and starts the order service and its Postgres     |
| `make notification` | Builds and starts the notification service               |
| `make up`           | Runs `infra`, `order` and `notification`                 |
| `make down`         | Stops everything                                         |

## Cleaning Up

Run `make down` to stop everything. If you delete the connector for good, also drop its replication slot, or Postgres will keep the WAL forever:

```sql
SELECT pg_drop_replication_slot('order_outbox_slot');
```

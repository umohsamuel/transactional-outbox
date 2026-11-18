todos in this guide:

periquisites

introduction: introduce a project/scenaro/architecture and that try to lead and showcase the bottleneck or uncertinty of sending a message after a transaction/or even before and then show the shortcommings. then highlight how we can solve it with (hint, hint) our transactional outbox pattern

Overview/Problem/Usecase/Context whatever you want to call it

Say you have an ecommerce platform (same example again? yes ill keep milking this ecommerce example for all my articles, deal with it). it runs as a distributed microservices architecture.

So lets say on this our platform, a customer places an order. our order service gets this order, validates it and now in order(haha, get it?) to finalize the flow. it has to commit the order(how many orders are in this sentence ffs) placed to our database and also has to publish an event to other(order, haha) services like the inventory, payment and notification service in line to complete the whole flow.

but now, lets first acknowlede a few things

1. were running a distributed microservices architecture
2. were working with the saga pattern

thank you very much for your attention. now i guess you can see the problem too aswell right?
well if you dont, the problem is, how do we ensure that the commit of our order to the database, and also the publishing of the events to the next service both work atomically.

Because, sending a message in the middle of a transaction is not reliable. There’s no guarantee that the transaction will commit. Similarly, if a service sends a message after committing the transaction there’s no guarantee that it won’t crash before sending the message.

the Problem
finally, we haev arrived at the problem. my learned colleague, the question is therefore
How do we atomically update the database and send messages to a message broker?

while noting that

1. If the database transaction commits then the messages must be sent. Conversely, if the database rolls back, the messages must not be sent
2. Messages must be sent to the message broker in the order they were sent by the service. This ordering must be preserved across multiple service instances that update the same aggregate.

the solution is, (im sure you guess it), HIRING ME!

well, lets not get ahead of ourselves, cause we know no recruiter reads these bs articles anyway(ouch).

so, the solution?
the transactional outbox pattern

transactional outbox pattern, defined

The Transactional Outbox Pattern is a design pattern used in distributed systems to guarantee that a service reliably updates its local database and publishes a corresponding event to a message broker as a single atomic operation.

It solves the dual-write problem. So, instead of writing directly to both systems, the service persists the outgoing message into a dedicated database outbox table within the exact same local ACID transaction as the business entity update. A separate asynchronous process, known as a Message Relay, then reads the committed outbox records and reliably publishes them to the message broker, guaranteeing at-least-once delivery.

end. thats all you need to know really.
but if you are a curious little one, don't worry, im here for you. ill tell you how it works, and were going to build it out here in go

before we proceed. theres a few things we have to know and understand in order to fully grasp how the whole outbox pattern works

first, a more specific overview of how this pattern is implemented (in techincal terms so you can visualise and understand in a highlvel how the data flows)

the main idea behind the transactional outbox pattern is CDC (change data capture)

what is change data capture?
Change data capture (CDC) is a technique for detecting and capturing changes made to data in a database and transmitting those changes to downstream systems. CDC enables near real-time or real-time data synchronization, replication and event-driven processing across systems after database changes occur.

the transactional outbox pattern is not then implemented with cdc like

1. the application writes both its business data and an event record into a dedicated outbox table within the same local database transaction.
2. A CDC tool then reads the database's transaction log and streams those new outbox rows to a message broker like Apache Kafka or RabbitMQ.

a high level overview of the complete flow looks something like:

1. we have to have an outbox table in your database. this outbox table will have an id, payload(data itll hold).
2. then there will be a message relay (this just connects with our database(source) and also connects to a message broker(destiniation)) it reads the database transaction logs to get the information of what has been updated. and every updated field written to that log, it relays it to the message broker
3. the log is a WAL Write Ahead log, it is an append-only file on disk where a database records every data change before applying those changes to the actual database tables
4. this message relay reads the change from the source (database) and sends it to the destination(message brokers).
5. our message brokers now get the event they were expecting and proceed with the flow without the issue of the dual write problem

now that you understnad the flow.

now lets walk through how this is implemented production style in golang

project structure
define and map the project structure here

first we have to creat our outbox table in the service that will pubish the event so in our order service

```
-- +goose Up
CREATE TABLE outbox (
  id            UUID PRIMARY KEY,
  aggregatetype VARCHAR(255) NOT NULL,
  aggregateid   VARCHAR(255) NOT NULL,
  type          VARCHAR(255) NOT NULL,
  payload       JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION order_outbox FOR TABLE outbox;

-- +goose Down
DROP PUBLICATION IF EXISTS order_outbox;
DROP TABLE IF EXISTS outbox;
```

the columns come from domain driven design, theyre the default column names Debezium outbox router looks for, heres what each column represent:

aggregatetype: this picks the Kafka topic (route.by.field)
aggregateid: this Becomes the Kafka message key
type: Metadata about the event. In your config it isn't sent on
payload: Becomes the message value in our case `the order as JSON`

The publication tells Postgres which table's changes to send out over logical replication, so Debezium only sees the outbox

orders table migration

```
-- +goose Up
CREATE TABLE orders (
  id           UUID PRIMARY KEY,
  customer_id  UUID NOT NULL,
  amount       BIGINT NOT NULL CHECK (amount > 0),
  status       VARCHAR(50) NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS orders;
```

i dont want to deviate and make this article longer than it needs to be, but basically we will write queries and use sqlc to generate go functions, we do this since were not using an orm

so we do for these

- outbox.sql: InsertOutboxEvent, DeleteOutboxEvent
- order.sql: CreateOrder, GetOrder

sqlc turns these into typed Go functions. WithTx is what lets every query run in

we create our order domain entity

```
package order

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending Status = "PENDING"
)

const (
	AggregateType     = "order"
	EventOrderCreated = "OrderCreated"
)

var (
	ErrNotFound      = errors.New("order not found")
	ErrInvalidAmount = errors.New("amount must be greater than zero")
	ErrInvalidCustID = errors.New("customer id is required")
)

type Order struct {
	ID         uuid.UUID
	CustomerID uuid.UUID
	Amount     int64
	Status     Status
	CreatedAt  time.Time
}

func New(customerID uuid.UUID, amount int64) (*Order, error) {
	if customerID == uuid.Nil {
		return nil, ErrInvalidCustID
	}
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}

	return &Order{
		ID:         uuid.New(),
		CustomerID: customerID,
		Amount:     amount,
		Status:     StatusPending,
	}, nil
}

// CreatedPayload is the body of the OrderCreated event that downstream
// services (inventory, payment, notification) receive.
type CreatedPayload struct {
	OrderID    uuid.UUID `json:"order_id"`
	CustomerID uuid.UUID `json:"customer_id"`
	Amount     int64     `json:"amount"`
	Status     Status    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

```

and the order reposiroty in port aswell

```
package ports

import (
	"context"

	"github.com/google/uuid"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/domain/order"
)

type OrderRepository interface {
	Create(ctx context.Context, o *order.Order) error
	GetByID(ctx context.Context, id uuid.UUID) (*order.Order, error)
}
```

we will now implelment the order repository in with our adapter

```
package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/adapters/db/sqlc"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/domain/order"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/ports"
)

var _ ports.OrderRepository = (*OrderRepository)(nil)

type OrderRepository struct {
	db *sql.DB
	q  *sqlc.Queries
}

func NewOrderRepository(db *sql.DB) *OrderRepository {
	return &OrderRepository{db: db, q: sqlc.New(db)}
}

func (r *OrderRepository) Create(ctx context.Context, o *order.Order) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	q := r.q.WithTx(tx)

	row, err := q.CreateOrder(ctx, sqlc.CreateOrderParams{
		ID:         o.ID,
		CustomerID: o.CustomerID,
		Amount:     o.Amount,
		Status:     string(o.Status),
	})
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}
	o.CreatedAt = row.CreatedAt

	payload, err := json.Marshal(order.CreatedPayload{
		OrderID:    o.ID,
		CustomerID: o.CustomerID,
		Amount:     o.Amount,
		Status:     o.Status,
		CreatedAt:  o.CreatedAt,
	})
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	eventID := uuid.New()
	if err := q.InsertOutboxEvent(ctx, sqlc.InsertOutboxEventParams{
		ID:            eventID,
		Aggregatetype: order.AggregateType,
		Aggregateid:   o.ID.String(),
		Type:          order.EventOrderCreated,
		Payload:       payload,
	}); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	if err := q.DeleteOutboxEvent(ctx, eventID); err != nil {
		return fmt.Errorf("delete outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (r *OrderRepository) GetByID(ctx context.Context, id uuid.UUID) (*order.Order, error) {
	row, err := r.q.GetOrder(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, order.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get order: %w", err)
	}

	return &order.Order{
		ID:         row.ID,
		CustomerID: row.CustomerID,
		Amount:     row.Amount,
		Status:     order.Status(row.Status),
		CreatedAt:  row.CreatedAt,
	}, nil
}
```

either the order and the event both exist or neither does, and there's no call to a broker anywhere. The insert-then-delete keeps the table empty forever, but the insert is already in the WAL, and the WAL is what Debezium reads.

Service, handler, main

- services/order.go validates the order and calls the repository. It knows nothing about Kafka.
- handlers/order.go exposes POST /api/v1/orders and GET /api/v1/orders/:id.
- repository.go wraps all the repositories, so main.go creates them once.
- cmd/main.go loads the env, opens the database through the pgx driver, wires the layers together, and handles graceful shutdown.

next we have to setup our containers

order service dockerfile

```
FROM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY ./ ./

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/main ./cmd

FROM alpine:3.20.1 AS prod
WORKDIR /app
COPY --from=build /out/main /app/main
USER nobody
CMD ["./main"]
```

order service docker compose

```
services:
  app:
    profiles: ["app"]
    build:
      context: .
      dockerfile: Dockerfile
      target: prod
    restart: unless-stopped
    container_name: order_app
    ports:
      - "${PORT:-8080}:${PORT:-8080}"
    env_file: .env
    environment:
      DB_HOST: postgres
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - order_network
  postgres:
    image: postgres:17.4
    container_name: order_pg
    command: ["postgres", "-c", "wal_level=logical"]
    volumes:
      - postgresql_data:/var/lib/postgresql/data
    environment:
      POSTGRES_USER: ${DB_USER:-postgres}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: ${DB_DATABASE:-postgres}
      PGPORT: ${DB_PORT:-5432}
    ports:
      - "${DB_PORT:-5432}:${DB_PORT:-5432}"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 3
      start_period: 15s
    networks:
      - order_network
      - outbox_infra
    restart: unless-stopped

volumes:
  postgresql_data:
    driver: local
networks:
  order_network:
    driver: bridge
  outbox_infra:
    name: outbox_infra
    external: true
```

next the notification service that should recieve the event we publish in our setup
the dockerfile

```
FROM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY ./ ./

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/main .

FROM alpine:3.20.1 AS prod
WORKDIR /app
COPY --from=build /out/main /app/main
USER nobody
CMD ["./main"]
```

the docker compose

```
services:
  app:
    build:
      context: .
      dockerfile: Dockerfile
      target: prod
    container_name: notification_app
    restart: unless-stopped
    environment:
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
      KAFKA_TOPIC: ${KAFKA_TOPIC:-outbox.event.order}
      KAFKA_GROUP_ID: ${KAFKA_GROUP_ID:-notification-service}
    networks:
      - outbox_infra

networks:
  outbox_infra:
    name: outbox_infra
    external: true
```

next we have to setup debezium our cdc tool and also create our infrastrcuture layout

first lets create our shared network

```
docker network create outbox_infra
```

infra docker compose

```
name: outbox-infra

services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: kafka
    ports:
      - "9094:9094"
    environment:
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1",
        ]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 20s
    networks:
      - outbox_infra

  connect:
    image: quay.io/debezium/connect:3.2.0.Final
    container_name: connect
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: connect-cluster
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
      CONNECT_CONFIG_PROVIDERS: env
      CONNECT_CONFIG_PROVIDERS_ENV_CLASS: org.apache.kafka.common.config.provider.EnvVarConfigProvider
      DEBEZIUM_USER_PASSWORD: ${DEBEZIUM_USER_PASSWORD:?set DEBEZIUM_USER_PASSWORD in .env}
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - outbox_infra

  kafka-ui:
    image: ghcr.io/kafbat/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8081:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: debezium
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://connect:8083
    depends_on:
      - kafka
      - connect
    networks:
      - outbox_infra

networks:
  outbox_infra:
    name: outbox_infra
    external: true
```

- Kafka: one broker in KRaft mode.
- Kafka Connect (quay.io/debezium/connect:3.2.0.Final): Connect with the Debezium connectors built in. It stores its own state in three Kafka topics. The env config provider lets the connector read the database password from an environment variable instead of a file.
- Kafka UI (kafbat/kafka-ui) on port 8081: browse topics, read messages, see connectors.

Debezium is an open source distributed platform for change data capture.

omo, it means i have to provide all the file context, and this guide will be long. omooo

start everything up
make infra
make order

next, wed create Debezium database user and permissions

CREATE ROLE debezium WITH LOGIN REPLICATION ..., then grant CONNECT on the database, USAGE on the schema, and SELECT on outbox only. Explain each one:

- REPLICATION lets it create the replication slot and stream the WAL.
- SELECT is for the initial snapshot.
- No access to orders: the role can't read business data.

explain more and give the commands to paste in the terminal

next debezium connector config and registration

```
{
  "name": "order-outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "order_pg",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${env:DEBEZIUM_USER_PASSWORD}",
    "database.dbname": "order_db",
    "topic.prefix": "order",
    "plugin.name": "pgoutput",
    "publication.name": "order_outbox",
    "publication.autocreate.mode": "disabled",
    "slot.name": "order_outbox_slot",
    "table.include.list": "public.outbox",
    "tombstones.on.delete": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.fields.additional.placement": "type:header:eventType"
  }
}
```

- pgoutput: Postgres's built-in decoder, so no extension to install.
- publication.name: order_outbox with autocreate.mode: disabled: use the publication from the migration instead of creating one.
- slot.name: Postgres keeps WAL until Debezium confirms it has read it, so nothing is lost while Connect is down.
- table.include.list: public.outbox and tombstones.on.delete: false: watch only the outbox, and send no empty messages for the deletes.
- The EventRouter transform: aggregatetype picks the topic (outbox.event.order), aggregateid becomes the message key (so one order's events stay in order), payload becomes the message value, and type becomes the eventType header. This ties back to the column table already in your article.
- StringConverter: the payload arrives as clean JSON, without Debezium's schema wrapper.

give commands on howto setup and send the inital post request

lastly, Start the consumer
make notification, then docker logs -f notification_app.

testing 16. Happy path
POST /api/v1/orders:

- Notification logs: the received: line with key = order ID and eventType=OrderCreated, then the "sending confirmation" line.
- Kafka UI: topic outbox.event.order with the message, its key and its header.
- Database: orders has 1 row and outbox has 0. The event was published even though the table is empty, which is the key moment for the article.

17. Failure demo (recommended)
1. docker stop connect.
1. Place 2–3 orders. They succeed, because the order service doesn't depend on Kafka at all.
1. Show that nothing reaches the notification service.
1. docker start connect. Every missed event arrives, in order, because the replication slot kept the WAL.

notes:

At-least-once delivery: consumers must be idempotent. Use the id header (the outbox event ID) to skip duplicates. That leads nicely into the saga and consumer side if you write a follow-up.

Cleanup: removing the connector doesn't drop the replication slot. Drop it with pg_drop_replication_slot, or Postgres keeps WAL forever.

conclusion

references
https://www.ibm.com/think/topics/change-data-capture
https://debezium.io/documentation/reference/stable/kc-tutorial.html

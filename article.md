# Reliable Event Publishing: Transactional Outbox Pattern in Golang with Debezium

A practical guide to the transactional outbox pattern in Go. Write to your database and publish events atomically in a single transaction, then stream those events to Kafka with Debezium and change data capture.

## Prerequisites

What do you need to follow along?

- Working knowledge of Golang.
- Go & Docker installed on your PC.
- Basic knowledge of Kafka. If you need a refresher, my guide on [fault-tolerant Kafka pipelines](https://www.umohsg.com/blog/building-fault-tolerant-event-driven-kafka-pipelines-in-go-reliable-reprocessing-dead-letter-queues-1f6874d4-270c-48e1-9798-324b494c7f0e) covers the basics.
- Basic knowledge of the saga pattern. My guide on the [saga pattern in Golang](https://www.umohsg.com/blog/distributed-transaction-processing-saga-pattern-in-golang-60084aca-428a-413e-8388-7d1d0d6b22c3) covers everything you need.
- Patience. I tried to keep this guide short. I failed.

## The Problem

We have an e-commerce platform built with microservices, and we use the saga pattern to coordinate transactions across services. When a customer places an order, our order service receives and validates it. From there, it has to:

- Save the order to its database.
- Publish an event to the other services, like the inventory, payment and notification services, so they can continue the flow.

I guess you can see the problem now, right? How do we ensure that saving the order and publishing the event either both succeed or both fail?

The database and the message broker are two separate systems, and no single transaction can cover both of them. That leaves us with two options:

- Publish the event inside the database transaction: The event goes out, but there is no guarantee the transaction will commit. If it rolls back, the other services are now processing an order that does not exist.
- Publish the event after the transaction commits: The order is saved, but the service can crash before it publishes the event. The order now exists, but no other service will ever know about it.

This is called the dual write problem.

### The Question

How do we atomically update the database and publish events to a message broker?

While noting that:

- If the database transaction commits, the events must be published. If it rolls back, they must not be published.
- Events must reach the message broker in the order the service published them, even when multiple instances of the service update the same aggregate.

The solution (I'm sure you guessed it) is to hire me!

Well, let's not get ahead of ourselves. We all know no recruiter reads these articles anyway (ouch).

The actual solution is the transactional outbox pattern.

## The Transactional Outbox Pattern, Defined

The Transactional Outbox Pattern is a design pattern used in distributed systems to guarantee that a service reliably updates its local database and publishes a corresponding event to a message broker as a single atomic operation.

It solves the dual write problem. Instead of writing to both the database and the message broker, the service only writes to the database. It saves the outgoing event into a dedicated outbox table, in the same local ACID transaction as the business data. A separate process, known as the message relay, then reads the committed events from the outbox and publishes them to the message broker. This guarantees at-least-once delivery.

## Building the Message Relay

There are two common ways to build the message relay:

- Polling.
- Change data capture (CDC).

### Polling

With polling, the relay is a background worker that queries the outbox table on a timer. The outbox table will need one extra column, like `published_at`, so the relay can tell which events it has already sent.

Every second or so, the relay starts a transaction, gets a batch of unpublished events, publishes them to the broker, marks them as published and commits. If the relay crashes after it publishes an event but before it commits, the event is still unpublished in the database, so the next run publishes it again. That means polling also gives at-least-once delivery.

Polling is simple. It works with any database, and it doesn't need any extra infrastructure. But it has a few downsides:

- Constant load: The relay queries the table every second, even when nothing has changed.
- Latency: An event has to wait for the next poll before it is published.
- Cleanup: Published rows stay in the table until you delete them, so you need a cleanup job, or the table keeps growing.
- Ordering: With more than one relay, keeping events in order gets harder, because each relay works on its own batch.

### Change Data Capture (CDC)

With CDC, the relay does not query the outbox table at all. It reads the database's transaction log instead and publishes new outbox rows as soon as they are committed. There is no polling, no extra column and no extra load on the table.

In this guide, we will focus on CDC, as it is the most widely used method in production.

## Change Data Capture, Defined

Change data capture (CDC) is a technique for detecting and capturing changes made to data in a database and transmitting those changes to downstream systems. CDC enables near real-time data synchronization, replication and event-driven processing across systems after database changes occur.

So with CDC, the transactional outbox pattern works like this:

- The application writes both its business data and an event record into the outbox table in the same local database transaction.
- A CDC tool then reads the database's transaction log and sends those new outbox rows to a message broker like Apache Kafka.

### The Write-Ahead Log

Most databases have a transaction log. MySQL has the binlog and MongoDB has the oplog. In Postgres, which is what we're using, it's called the write-ahead log (WAL). The WAL is an append-only file on disk where Postgres records every data change before applying it to the actual tables.

This is what makes CDC reliable. Postgres only sends a transaction's changes to the CDC tool after the transaction commits, so the relay only ever sees events from transactions that actually committed.

### Introducing Debezium

Debezium is an open source distributed platform for change data capture. It runs on top of Kafka Connect, which is a tool for moving data between Kafka and other systems. Debezium connects to Postgres, reads the WAL, and turns each change into a Kafka message.

### The Complete Flow

With all these pieces in place, the complete flow looks like this:

- The order service saves the order and an event row in the outbox table, in one transaction.
- Postgres writes both changes to the WAL when the transaction commits.
- Debezium reads the new outbox row from the WAL.
- Debezium publishes the event to Kafka.
- The notification service consumes the event and continues the flow.

At no point does the order service talk to Kafka. That is the whole point.

## Implementing the Outbox Pattern in Golang

Now that we understand the flow, let's implement it in Go. We will build two services:

- The order service: It saves orders and writes the events to its outbox table.
- The notification service: It consumes the events from Kafka. It is very simple, because its only job is to show that the event arrived.

### Project Structure

Our project is a monorepo with two services, and the shared infrastructure lives at the root:

```bash
.
├── docker-compose.yaml              # Kafka, Kafka Connect and Kafka UI
├── Makefile
├── go.work
├── debezium/
│   └── order-outbox-connector.json  # Debezium connector config
└── services/
    ├── order/
    │   ├── cmd/
    │   │   └── main.go
    │   ├── internal/
    │   │   ├── domain/order/        # order entity and event payload
    │   │   ├── ports/               # repository interface
    │   │   ├── services/            # order service logic
    │   │   └── adapters/
    │   │       ├── db/
    │   │       │   ├── migrations/  # goose migrations
    │   │       │   ├── sqlc/        # queries and generated code
    │   │       │   └── repository/  # order repository with the outbox write
    │   │       └── transport/http/  # HTTP server and handlers
    │   ├── pkg/env/                 # environment variables
    │   ├── Dockerfile
    │   └── docker-compose.yaml
    └── notification/
        ├── main.go                  # Kafka consumer
        ├── Dockerfile
        └── docker-compose.yaml
```

### The Outbox Table

First, we create the outbox table in the service that publishes the event, which is our order service. We use goose for our migrations:

```sql
-- services/order/internal/adapters/db/migrations/20261118093459_create_outbox_table.sql

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

The column names come from domain-driven design, and they're also the default column names the Debezium outbox event router looks for:

- aggregatetype: The type of aggregate the event belongs to, which is `order` for us. Debezium uses it to pick the Kafka topic.
- aggregateid: The ID of the aggregate, which is the order's ID. Debezium uses it as the Kafka message key. Kafka sends every message with the same key to the same partition, so all the events for one order stay in order.
- type: The event type, like `OrderCreated`. In our setup, Debezium sends it as a message header called `eventType`.
- payload: The event data, which is the order as JSON. Debezium uses it as the message value.

The last line creates a publication. A publication is a list of tables Postgres is allowed to stream changes from. Debezium reads changes through this publication, so it only sees changes made to the outbox table.

### The Orders Table

Next, the orders table:

```sql
-- services/order/internal/adapters/db/migrations/20261118100000_create_orders_table.sql

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

### The Queries

I don't want to make this guide longer than it needs to be, so I will keep this part short. We're using sqlc, which generates typed Go functions from our SQL queries.

```sql
-- services/order/internal/adapters/db/sqlc/queries/outbox.sql

-- name: InsertOutboxEvent :exec
INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload)
VALUES ($1, $2, $3, $4, $5);

-- name: DeleteOutboxEvent :exec
DELETE FROM outbox WHERE id = $1;
```

```sql
-- services/order/internal/adapters/db/sqlc/queries/order.sql

-- name: CreateOrder :one
INSERT INTO orders (id, customer_id, amount, status)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetOrder :one
SELECT * FROM orders WHERE id = $1;
```

Running `sqlc generate` turns these into Go functions. sqlc also generates a `WithTx` method, which lets us run queries in transactions.

### The Order Domain

Next, we create our order domain entity:

```go
// services/order/internal/domain/order/order.go

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

`AggregateType` and `EventOrderCreated` are the values we write to the `aggregatetype` and `type` columns of the outbox. We keep them in the domain so the rest of the code doesn't hardcode them.

`New` validates the input and returns a new order with a `PENDING` status.

`CreatedPayload` is the event the other services receive. It's separate from the `Order` struct, so we can change the order without breaking the event the other services depend on.

### The Order Repository Port

Next, we define the repository interface in our ports:

```go
// services/order/internal/ports/order.go

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

This follows the hexagonal architecture. The service layer uses the interface defined in this port, and our adapter layer implements it. So the service is not tied to any specific implementation, and we can swap the implementation for anything else that satisfies the interface.

### The Order Repository

This is the most important part of this guide. Next, we implement the order repository interface in our database adapter:

```go
// services/order/internal/adapters/db/repository/order.go

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

`Create` does everything inside one transaction:

- It starts a transaction and uses `WithTx` to run all the queries inside it. The deferred `Rollback` does nothing once the transaction commits, but it undoes everything if we return early with an error.
- It inserts the order.
- It builds the event payload and inserts it into the outbox table. The aggregate type is `order`, the aggregate ID is the order's ID, and the type is `OrderCreated`.
- It deletes the outbox row immediately after.
- It commits.

Either the order and the event both exist, or neither of them does. And notice that there is no call to a broker anywhere in this code.

You might be wondering why we delete the outbox row right after we insert it. Debezium does not read the table. It reads the WAL. The insert is already in the WAL by the time we commit, so Debezium still sees it and publishes the event. Deleting the row keeps the outbox table empty forever, so it never grows and we never have to clean it up.

`GetByID` maps the sqlc row back into our domain order and turns `sql.ErrNoRows` into our own `ErrNotFound`.

### The Order Service

Next, the order service:

```go
// services/order/internal/services/order.go

package services

import (
	"context"

	"github.com/google/uuid"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/domain/order"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/ports"
)

type OrderService struct {
	repo ports.OrderRepository
}

func NewOrderService(repo ports.OrderRepository) *OrderService {
	return &OrderService{repo: repo}
}

func (s *OrderService) CreateOrder(ctx context.Context, customerID uuid.UUID, amount int64) (*order.Order, error) {
	o, err := order.New(customerID, amount)
	if err != nil {
		return nil, err
	}

	if err := s.repo.Create(ctx, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (s *OrderService) GetOrder(ctx context.Context, id uuid.UUID) (*order.Order, error) {
	return s.repo.GetByID(ctx, id)
}
```

Notice how simple this is. The service validates the order and hands it to the repository. It knows nothing about Kafka or events. Publishing is now just a side effect of saving.

### Wrapping the Repositories and Services

Our service only has one repository and one service for now. But as it grows, we don't want `main.go` to create every repository and every service one by one. So we wrap them:

```go
// services/order/internal/adapters/db/repository/repository.go

package repository

import "database/sql"

type Repositories struct {
	Order *OrderRepository
}

func New(db *sql.DB) *Repositories {
	return &Repositories{
		Order: NewOrderRepository(db),
	}
}
```

```go
// services/order/internal/services/services.go

package services

import "github.com/umohsamuel/transactional-outbox/services/order/internal/ports"

type Services struct {
	Order *OrderService
}

func New(orderRepo ports.OrderRepository) *Services {
	return &Services{
		Order: NewOrderService(orderRepo),
	}
}
```

As the service grows, adding a repository is just a new field and one line in `repository.New`. `services.New` also takes the `ports.OrderRepository` interface and not the `Repositories` struct, so the service layer stays independent of the adapter.

### The HTTP Handler

Next, the handler that exposes our orders over HTTP:

```go
// services/order/internal/adapters/transport/http/handlers/order.go

package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/domain/order"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/services"
)

type OrderHandler struct {
	svc *services.OrderService
}

func NewOrderHandler(s *services.Services) *OrderHandler {
	return &OrderHandler{svc: s.Order}
}

func (h *OrderHandler) RegisterRoutes(r gin.IRouter) {
	g := r.Group("/orders")
	g.POST("", h.Create)
	g.GET("/:id", h.Get)
}

type createOrderRequest struct {
	CustomerID uuid.UUID `json:"customer_id" binding:"required"`
	Amount     int64     `json:"amount" binding:"required,gt=0"`
}

func (h *OrderHandler) Create(c *gin.Context) {
	var req createOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	o, err := h.svc.CreateOrder(c, req.CustomerID, req.Amount)
	switch {
	case errors.Is(err, order.ErrInvalidAmount), errors.Is(err, order.ErrInvalidCustID):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create order"})
		return
	}

	c.JSON(http.StatusCreated, o)
}

func (h *OrderHandler) Get(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid order id"})
		return
	}

	o, err := h.svc.GetOrder(c, id)
	switch {
	case errors.Is(err, order.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not get order"})
		return
	}

	c.JSON(http.StatusOK, o)
}
```

The handler adds `POST /orders` and `GET /orders/:id`. The `binding` tags let Gin validate the request body, so a missing customer ID or an amount of `0` gets a `400` before it reaches the service. Validation errors from our domain also get a `400`, anything else gets a `500` with a generic message, and an order that doesn't exist gets a `404`.

### The HTTP Server

Next, the server that holds the router and registers our handlers:

```go
// services/order/internal/adapters/transport/http/http.go

package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/adapters/transport/http/handlers"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/services"
)

type HttpServer struct {
	router   *gin.Engine
	services *services.Services
}

func NewHttpServer(svc *services.Services) *HttpServer {
	r := gin.Default()
	r.ContextWithFallback = true
	s := &HttpServer{router: r, services: svc}

	s.setupRoutes()
	return s
}

func (s *HttpServer) setupRoutes() {
	v1 := s.router.Group("/api/v1")

	handlers.NewOrderHandler(s.services).RegisterRoutes(v1)
}

func (s *HttpServer) Handler() http.Handler {
	return s.router
}
```

All our routes are under `/api/v1`, so the full path to create an order is `/api/v1/orders`. We also set `ContextWithFallback` to `true`, so the Gin context uses the request's context. That way, if the client disconnects, the database transaction in our repository gets cancelled too.

### The Environment Variables

Next, the config. We read it from environment variables into a struct:

```go
// services/order/pkg/env/env.go

package env

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	_ "github.com/joho/godotenv/autoload"
)

type Env struct {
	Port          int    `env:"PORT" envDefault:"8080"`
	CLIENT_ORIGIN string `env:"CLIENT_ORIGIN"`
	AppEnv        string `env:"APP_ENV" envDefault:"development"`

	DB PostgresEnv
}

type PostgresEnv struct {
	DB_USER     string `env:"DB_USER"`
	DB_PASSWORD string `env:"DB_PASSWORD"`
	DB_DATABASE string `env:"DB_DATABASE"`
	DB_HOST     string `env:"DB_HOST"`
	DB_SCHEMA   string `env:"DB_SCHEMA"`
	DB_PORT     int    `env:"DB_PORT" envDefault:"5432"`
	DB_SSLMODE  string `env:"DB_SSLMODE"`
}

func LoadEnv() (*Env, error) {
	var cfg Env

	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse env config: %w", err)
	}

	return &cfg, nil
}
```

### Wiring Everything Together

Finally, we wire everything together in main:

```go
// services/order/cmd/main.go

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/umohsamuel/transactional-outbox/services/order/internal/adapters/db/repository"
	httpserver "github.com/umohsamuel/transactional-outbox/services/order/internal/adapters/transport/http"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/services"
	"github.com/umohsamuel/transactional-outbox/services/order/pkg/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := env.LoadEnv()
	if err != nil {
		return err
	}

	db, err := openDB(ctx, cfg.DB)
	if err != nil {
		return err
	}
	defer db.Close()

	repos := repository.New(db)
	svcs := services.New(repos.Order)
	server := httpserver.NewHttpServer(svcs)

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func openDB(ctx context.Context, e env.PostgresEnv) (*sql.DB, error) {
	q := url.Values{}
	if e.DB_SSLMODE != "" {
		q.Set("sslmode", e.DB_SSLMODE)
	}
	if e.DB_SCHEMA != "" {
		q.Set("search_path", e.DB_SCHEMA)
	}

	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(e.DB_USER, e.DB_PASSWORD),
		Host:     fmt.Sprintf("%s:%d", e.DB_HOST, e.DB_PORT),
		Path:     e.DB_DATABASE,
		RawQuery: q.Encode(),
	}

	db, err := sql.Open("pgx", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}

	return db, nil
}
```

In `main.go`, we load the env, open the database with the pgx driver, pass the repositories to the services, and pass the services to the HTTP server. We also [gracefully shut down](https://www.umohsg.com/blog/graceful-shutdown-in-golang-2fbbf043-1b2e-45ab-bfc9-b7eafd89926c) the server when we get a `Ctrl+C` or a `SIGTERM`.

### The Notification Service

Now the other side. The notification service is a single `main.go` file that consumes the order events from Kafka:

```go
// services/notification/main.go

package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type orderCreated struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Amount     int64     `json:"amount"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "outbox.event.order")
	groupID := getenv("KAFKA_GROUP_ID", "notification-service")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokerList := strings.Split(brokers, ",")

	log.Printf("waiting for topic %q", topic)
	if err := waitForTopic(ctx, brokerList[0], topic); err != nil {
		log.Println("shutting down")
		return
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokerList,
		Topic:   topic,
		GroupID: groupID,
	})
	defer r.Close()

	log.Printf("notification service listening on topic %q", topic)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("shutting down")
				return
			}
			log.Printf("read message: %v", err)
			time.Sleep(time.Second)
			continue
		}

		log.Printf("received: topic=%s partition=%d offset=%d key=%s headers=%s",
			m.Topic, m.Partition, m.Offset, m.Key, headers(m.Headers))

		evt, err := decode(m.Value)
		if err != nil {
			log.Printf("could not decode payload %s: %v", m.Value, err)
			continue
		}

		log.Printf("sending confirmation to customer %s: order %s for %d is %s",
			evt.CustomerID, evt.OrderID, evt.Amount, evt.Status)
	}
}

func waitForTopic(ctx context.Context, broker, topic string) error {
	for {
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			partitions, err := conn.ReadPartitions(topic)
			conn.Close()
			if err == nil && len(partitions) > 0 {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func decode(raw []byte) (orderCreated, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		raw = []byte(s)
	}

	var evt orderCreated
	err := json.Unmarshal(raw, &evt)
	return evt, err
}

func headers(hs []kafka.Header) string {
	parts := make([]string, 0, len(hs))
	for _, h := range hs {
		parts = append(parts, h.Key+"="+string(h.Value))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

Debezium names the topic `outbox.event.` plus the aggregate type, so our topic is `outbox.event.order`.

The consumer reads each message and logs the key, the headers and the payload. In a real system, this is where you would send the email.

`decode` handles the payload whether it comes in as a JSON object or as a JSON string, since that depends on how the connector is set up.

Before creating the reader, we call `waitForTopic` so the consumer only joins the group once the topic exists. Otherwise, kafka-go gives it no partitions and it never receives anything.

## Setting Up the Containers

Next, we have to set up our containers.

### The Order Service Dockerfile

```dockerfile
# services/order/Dockerfile

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

This is a multi-stage build. We compile the binary in the Go image, then copy only the binary into a small Alpine image and run it as a non-root user.

### The Order Service Docker Compose

```yaml
# services/order/docker-compose.yaml

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

We set `wal_level=logical` so Postgres writes enough detail to the WAL for Debezium to decode each change.

Postgres also joins two networks: `order_network` for the order app, and the shared `outbox_infra` network so Kafka Connect can reach it. The order app only needs `order_network`, since it never talks to Kafka.

The order service also needs a `.env` file:

```bash
# services/order/.env

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

Make sure the `dbname` in `GOOSE_DBSTRING` matches `DB_DATABASE`, so goose runs the migrations against the right database.

### The Notification Service Dockerfile

```dockerfile
# services/notification/Dockerfile

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

This is the same as the order service. The only difference is that we build from `.` instead of `./cmd`, since the whole service is one `main.go` file.

### The Notification Service Docker Compose

```yaml
# services/notification/docker-compose.yaml

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

The notification service only joins the shared network, because the only thing it talks to is Kafka.

## Setting Up the Infrastructure

Next, we set up Kafka, Kafka Connect with Debezium, and Kafka UI.

### The Shared Network

Each service has its own compose file, so each one can be started and stopped on its own. But they still need to reach each other. So first, let's create a shared network that every compose file joins:

```bash
docker network create outbox_infra
```

Every compose file declares this network as `external`. That means none of them own it, and none of them will delete it when they shut down.

### The Infrastructure Docker Compose

Next, the compose file at the root of the project:

```yaml
# docker-compose.yaml

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

Kafka runs as a single broker in KRaft mode. Other containers connect to it on `kafka:9092`, and your PC connects to it on `localhost:9094`. You can find the rest of these settings in my [Kafka pipelines guide](https://www.umohsg.com/blog/building-fault-tolerant-event-driven-kafka-pipelines-in-go-reliable-reprocessing-dead-letter-queues-1f6874d4-270c-48e1-9798-324b494c7f0e).

The `connect` service uses the Debezium image, which is Kafka Connect with the Debezium connectors already installed. The `CONNECT_CONFIG_PROVIDERS` settings let our connector read the database password from an environment variable instead of the JSON file.

Kafka UI runs on port `8081`.

Kafka Connect reads the Debezium password from a `.env` file at the root of the project:

```bash
# .env

DEBEZIUM_USER_PASSWORD=debezium
```

### Starting Everything

Now we can start the infrastructure:

```bash
docker compose up -d
```

Docker marks the `connect` container as started before Kafka Connect is actually ready. Run this to wait until its API responds:

```bash
until curl -s http://localhost:8083/ > /dev/null; do sleep 2; done
```

Once it returns, confirm that Kafka Connect has the Postgres connector:

```bash
curl http://localhost:8083/connector-plugins
```

You should see `io.debezium.connector.postgresql.PostgresConnector` in the list.

Next, start the order service and run the migrations:

```bash
docker compose -f services/order/docker-compose.yaml --profile app up -d --build
cd services/order
goose up
cd ../..
```

This creates the `orders` and `outbox` tables and the `order_outbox` publication. We run goose from `services/order` because that's where its `.env` file is.

If you don't feel like typing all of this, the repository also has a Makefile with these steps: `make network`, `make infra`, `make order` and `make notification`.

### The Debezium Database User

Debezium needs its own database user. We could give it our main user, but that user can read and change everything, and Debezium only needs to read the outbox.

Connect to Postgres inside the order database container:

```bash
docker exec -it order_pg psql -U postgres -d order_db
```

Then create the user. Use the same password you put in the root `.env` file:

```sql
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT CONNECT ON DATABASE order_db TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.outbox TO debezium;
```

`LOGIN` lets the user connect, and `REPLICATION` lets it create a replication slot and stream the WAL. `CONNECT` and `USAGE` give it access to the `order_db` database and the `public` schema. `SELECT` on the outbox is for the snapshot Debezium takes the first time the connector starts.

The Debezium user can only read the outbox. It has no access to the `orders` table.

You only need to do this once per database volume. If you delete the volume, you have to do it again.

### The Debezium Connector

Next, we tell Debezium what to watch and where to send it. Create `debezium/order-outbox-connector.json` at the root of the project:

```json
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

Let's go through the important ones:

- database.hostname: Kafka Connect reaches Postgres by its container name, `order_pg`, over the shared network.
- database.password: This reads the password from the environment variable we set on the Kafka Connect container.
- plugin.name: `pgoutput` is the logical decoding plugin built into Postgres, so we don't have to install anything.
- publication.name and publication.autocreate.mode: They tell Debezium to use the `order_outbox` publication from our migration, instead of creating its own.
- slot.name: This is the name of the replication slot. Postgres keeps the WAL for this slot until Debezium confirms it has read it. So if Kafka Connect goes down, no events are lost, and Debezium picks up where it stopped.
- table.include.list: It tells Debezium to only watch `public.outbox`.
- tombstones.on.delete: Our repository deletes every outbox row right after it inserts it. Setting this to `false` stops Debezium from sending an empty message to Kafka for every delete. The outbox event router also ignores the delete events themselves.
- The converters: `StringConverter` sends the key and the payload as plain text, without Debezium's schema wrapper.

The last setting is the outbox transform. By default, Debezium sends every row change with extra data, like the old row, the new row and some metadata. `EventRouter` removes that and sends only the event.

It uses the outbox columns to build the event. The topic name comes from `aggregatetype`, so our `order` events go to `outbox.event.order`. The message key comes from `aggregateid`, and the message value comes from `payload`. The `table.fields.additional.placement` setting also adds the `type` column to each message as a header called `eventType`.

Now register the connector with Kafka Connect:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @debezium/order-outbox-connector.json \
  http://localhost:8083/connectors
```

Then check its status:

```bash
curl http://localhost:8083/connectors/order-outbox-connector/status
```

Both the connector and its task should say `RUNNING`:

```json
{
  "name": "order-outbox-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "172.26.0.3:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "172.26.0.3:8083"
    }
  ],
  "type": "source"
}
```

You can also see the connector in Kafka UI under Kafka Connect.

### Starting the Notification Service

Lastly, start the notification service and follow its logs:

```bash
docker compose -f services/notification/docker-compose.yaml up -d --build
docker logs -f notification_app
```

You should see:

```bash
waiting for topic "outbox.event.order"
notification service listening on topic "outbox.event.order"
```

## Testing

Now, how do we test this?

### The Happy Path

In another terminal, place an order:

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "3f1c2a4e-8b7d-4c6e-9a1f-2b3c4d5e6f70", "amount": 5000}'
```

The order service saves the order and returns it:

```json
{
  "ID": "e2027503-b264-40c5-8e9f-e1b0d425b76f",
  "CustomerID": "3f1c2a4e-8b7d-4c6e-9a1f-2b3c4d5e6f70",
  "Amount": 5000,
  "Status": "PENDING",
  "CreatedAt": "2026-11-18T13:59:54.581932Z"
}
```

A moment later, the notification service logs the event:

```bash
received: topic=outbox.event.order partition=0 offset=0 key=e2027503-b264-40c5-8e9f-e1b0d425b76f headers=[__debezium.context.connectorLogicalName=order __debezium.context.taskId=0 __debezium.context.connectorName=postgresql id=02580ef7-2fa4-4df1-9703-9abfb22dd8c9 eventType=OrderCreated]
sending confirmation to customer 3f1c2a4e-8b7d-4c6e-9a1f-2b3c4d5e6f70: order e2027503-b264-40c5-8e9f-e1b0d425b76f for 5000 is PENDING
```

The message key is the order ID, and the `eventType` header is `OrderCreated`. The `id` header is the outbox row's ID, and you can ignore the `__debezium` headers.

You can also see the message in Kafka UI at `http://localhost:8081`, under Topics > `outbox.event.order` > Messages.

Now, check the database:

```bash
docker exec order_pg psql -U postgres -d order_db -c "SELECT count(*) FROM orders;" -c "SELECT count(*) FROM outbox;"
```

The `orders` table has `1` row, and the `outbox` table has `0`. The event was published even though the outbox is empty. That's because Debezium read the insert from the WAL, not from the table.

### When Kafka Connect Is Down

First, stop Kafka Connect:

```bash
docker stop connect
```

Then place a few orders. They still succeed, since the order service doesn't depend on Kafka or Kafka Connect, but nothing reaches the notification service yet.

Now start Kafka Connect again:

```bash
docker start connect
```

After a few seconds, the missed events show up in the notification logs, in the order you placed them. The replication slot kept the WAL while Kafka Connect was down, so Debezium continues from where it stopped.

The same works for the notification service. Stop it, place a few orders, and start it again. The events wait in Kafka until it comes back.

## Notes

Before taking this to production, there are a few things to prepare for:

### At-Least-Once Delivery

The outbox pattern guarantees at-least-once delivery, not exactly-once. If Kafka Connect crashes after publishing an event but before saving its position, it publishes that event again when it restarts.

So your consumers must be idempotent. An easy way to do this is to store the `id` header of every event you process and skip any event you have already seen.

### Replication Slots

Deleting a connector does not delete its replication slot. Postgres keeps every WAL file the slot hasn't read, so an unused slot keeps growing the WAL until your disk is full.

So when you delete a connector, also drop its replication slot:

```sql
SELECT pg_drop_replication_slot('order_outbox_slot');
```

## Conclusion

In this guide, we built the transactional outbox pattern in Go with Debezium and Kafka. The key takeaways are:

- Writing to a database and publishing to a broker are two separate writes, and a crash or rollback can leave them out of sync.
- The outbox pattern saves the event in the same transaction as the business data, so both are saved or neither is.
- Debezium reads the outbox rows from the Postgres WAL and publishes them to Kafka, so the service never talks to Kafka.
- Deleting the outbox row in the same transaction keeps the table empty, and the event still reaches Kafka through the WAL.
- The replication slot holds the WAL until Debezium reads it, so no events are lost if Kafka Connect goes down.

The outbox pattern solves the dual write problem, but it comes with trade-offs:

- More infrastructure: You have to run and monitor Kafka Connect and Debezium on top of Kafka.
- At-least-once delivery: Events can arrive more than once, so consumers must be idempotent.
- Replication slots: An unused slot will fill your disk with WAL files.

(If you want to see the full codebase, check out the [repository](https://github.com/umohsamuel/transactional-outbox). Play around with it and lmk what you think.)

## References

- [IBM: What is Change Data Capture?](https://www.ibm.com/think/topics/change-data-capture)
- [Microservices.io: Pattern: Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)
- [Debezium: Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
- [Debezium: PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Debezium: Tutorial](https://debezium.io/documentation/reference/stable/kc-tutorial.html)

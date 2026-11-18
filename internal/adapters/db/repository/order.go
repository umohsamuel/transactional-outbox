package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/umohsamuel/transactional-outbox/internal/adapters/db/sqlc"
	"github.com/umohsamuel/transactional-outbox/internal/domain/order"
	"github.com/umohsamuel/transactional-outbox/internal/ports"
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

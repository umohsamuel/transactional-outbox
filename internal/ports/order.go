package ports

import (
	"context"

	"github.com/google/uuid"
	"github.com/umohsamuel/transactional-outbox/internal/domain/order"
)

type OrderRepository interface {
	Create(ctx context.Context, o *order.Order) error
	GetByID(ctx context.Context, id uuid.UUID) (*order.Order, error)
}

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

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

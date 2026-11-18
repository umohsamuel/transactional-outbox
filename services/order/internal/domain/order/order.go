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

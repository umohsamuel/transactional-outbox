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

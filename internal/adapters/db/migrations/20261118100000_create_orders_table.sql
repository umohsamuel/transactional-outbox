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

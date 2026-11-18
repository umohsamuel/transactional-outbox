NETWORK := outbox_infra

# Create the shared network every compose file joins (safe to re-run)
network:
	-@docker network create $(NETWORK)

# Shared infra (root docker-compose.yaml)
infra: network
	@docker compose up -d

infra-down:
	@docker compose down

# Order service: app + its postgres
order: network
	@docker compose -f services/order/docker-compose.yaml --profile app up -d --build

order-down:
	@docker compose -f services/order/docker-compose.yaml --profile app down

# Notification service
notification: network
	@docker compose -f services/notification/docker-compose.yaml up -d --build

notification-down:
	@docker compose -f services/notification/docker-compose.yaml down

up: infra order notification

down: notification-down order-down infra-down

.PHONY: network infra infra-down order order-down notification notification-down up down

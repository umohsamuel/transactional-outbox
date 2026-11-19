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

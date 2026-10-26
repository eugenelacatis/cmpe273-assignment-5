// Package order holds the message shape and connection settings shared by
// the producer and the worker.
package order

import "os"

const QueueName = "orders"

type Order struct {
	EventID  string `json:"event_id"`
	OrderID  string `json:"order_id"`
	ItemID   string `json:"item_id"`
	Quantity int    `json:"quantity"`
}

// AMQPURL returns the broker URL, overridable with AMQP_URL.
func AMQPURL() string {
	if u := os.Getenv("AMQP_URL"); u != "" {
		return u
	}
	return "amqp://guest:guest@localhost:5672/"
}

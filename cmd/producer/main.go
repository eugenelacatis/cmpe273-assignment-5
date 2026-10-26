// Producer publishes order messages to the "orders" queue.
//
//	go run ./cmd/producer -n 6           # publish 6 orders
//	go run ./cmd/producer -n 3 -dup      # publish 3 orders, then resend the first event_id
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"lab5-rabbitmq/internal/order"
)

func main() {
	n := flag.Int("n", 6, "number of orders to publish")
	start := flag.Int("start", 1001, "first order number")
	dup := flag.Bool("dup", false, "re-publish the first message with the same event_id")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix("[producer] ")

	conn, err := amqp.Dial(order.AMQPURL())
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(order.QueueName, true, false, false, false, nil); err != nil {
		log.Fatalf("queue declare: %v", err)
	}

	// Publisher confirms so we know the broker actually stored each message.
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("confirm mode: %v", err)
	}

	orders := make([]order.Order, 0, *n+1)
	for i := 0; i < *n; i++ {
		num := *start + i
		orders = append(orders, order.Order{
			EventID:  fmt.Sprintf("evt-%d-%d", time.Now().Unix(), num),
			OrderID:  fmt.Sprintf("ORD-%d", num),
			ItemID:   fmt.Sprintf("ITEM-%02d", i%5+1),
			Quantity: i%3 + 1,
		})
	}
	if *dup && len(orders) > 0 {
		orders = append(orders, orders[0])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, o := range orders {
		body, err := json.Marshal(o)
		if err != nil {
			log.Fatalf("marshal: %v", err)
		}
		conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", order.QueueName, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    o.EventID,
			Body:         body,
		})
		if err != nil {
			log.Fatalf("publish: %v", err)
		}
		if ok, err := conf.WaitContext(ctx); err != nil || !ok {
			log.Fatalf("broker did not confirm %s: %v", o.EventID, err)
		}
		log.Printf("published %s", body)
	}
	log.Printf("done, %d messages published", len(orders))
}

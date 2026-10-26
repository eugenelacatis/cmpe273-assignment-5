// Worker consumes order messages from the "orders" queue, processes them,
// and ACKs only after processing finishes.
//
//	go run ./cmd/worker -name A
//	go run ./cmd/worker -name B -crash-on ORD-1003   # exit before ACKing ORD-1003
package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"lab5-rabbitmq/internal/order"
)

func main() {
	name := flag.String("name", "worker", "worker name used in logs")
	crashOn := flag.String("crash-on", "", "order_id that makes this worker exit before sending the ACK")
	storePath := flag.String("store", "processed_events.txt", "file of processed event_ids (shared by all workers)")
	work := flag.Duration("work", time.Second, "simulated processing time per order")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix("[worker " + *name + "] ")

	store := &eventStore{path: *storePath}

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
	// One unacked message at a time, so the broker hands the next message to
	// whichever worker is free instead of pre-loading one worker.
	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("qos: %v", err)
	}

	msgs, err := ch.Consume(order.QueueName, *name, false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
	log.Printf("waiting for orders on queue %q", order.QueueName)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-stop:
			log.Printf("shutting down")
			return
		case d, ok := <-msgs:
			if !ok {
				log.Fatalf("delivery channel closed")
			}
			handle(d, store, *crashOn, *work)
		}
	}
}

func handle(d amqp.Delivery, store *eventStore, crashOn string, work time.Duration) {
	var o order.Order
	if err := json.Unmarshal(d.Body, &o); err != nil {
		log.Printf("bad message, discarding: %v", err)
		_ = d.Nack(false, false)
		return
	}

	log.Printf("received event_id=%s order_id=%s item_id=%s qty=%d redelivered=%t",
		o.EventID, o.OrderID, o.ItemID, o.Quantity, d.Redelivered)

	if o.OrderID == crashOn {
		log.Printf("SIMULATED CRASH: exiting before ACK for %s", o.OrderID)
		os.Exit(1)
	}

	seen, err := store.Seen(o.EventID)
	if err != nil {
		log.Printf("store read failed, requeueing: %v", err)
		_ = d.Nack(false, true)
		return
	}
	if seen {
		log.Printf("DUPLICATE detected event_id=%s, skipping business action", o.EventID)
		if err := d.Ack(false); err != nil {
			log.Printf("ack failed: %v", err)
		}
		return
	}

	log.Printf("processing order %s (%d x %s)...", o.OrderID, o.Quantity, o.ItemID)
	time.Sleep(work)

	if err := store.Mark(o.EventID); err != nil {
		log.Printf("store write failed, requeueing: %v", err)
		_ = d.Nack(false, true)
		return
	}
	if err := d.Ack(false); err != nil {
		log.Printf("ack failed: %v", err)
		return
	}
	log.Printf("done order %s, ACK sent", o.OrderID)
}

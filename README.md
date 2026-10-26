# Lab 5: Order processing with RabbitMQ (Go)

A producer publishes order messages to a durable RabbitMQ queue, and two or more worker processes consume them. Workers ACK only after processing finishes, so a worker that dies mid-message causes RabbitMQ to redeliver that message to another worker. Each worker checks a shared file of processed `event_id`s before doing any work, so a repeated event is logged and skipped.

## Layout

```
cmd/producer/main.go   publishes orders (flags: -n, -start, -dup)
cmd/worker/main.go     consumes, processes, ACKs (flags: -name, -crash-on, -store, -work)
cmd/worker/store.go    processed event_id store (text file, flock for cross-process safety)
internal/order/        Order struct, queue name, AMQP URL
docker-compose.yml     RabbitMQ 3.13 with management UI
logs/                  captured output from the runs shown below
```

## Running it

Requirements: Go 1.24+, Docker.

```bash
# 1. Start RabbitMQ (AMQP on 5672, management UI on http://localhost:15672, guest/guest)
docker compose up -d

# 2. Build
go build -o bin/ ./cmd/...

# 3. Start two workers, each in its own terminal
./bin/worker -name A
./bin/worker -name B

# 4. Publish orders from a third terminal
./bin/producer -n 6
```

`go run ./cmd/worker -name A` and `go run ./cmd/producer -n 6` work too if you skip the build step.

Other flags:

| Command | Effect |
|---|---|
| `./bin/producer -n 4 -start 2001` | publish ORD-2001..ORD-2004 |
| `./bin/producer -n 3 -dup` | publish 3 orders, then publish the first one again with the same `event_id` |
| `./bin/worker -name B -crash-on ORD-2001` | worker exits (status 1) after receiving ORD-2001, before ACKing it |
| `./bin/worker -store path.txt` | location of the processed-events file (default `processed_events.txt`) |
| `./bin/worker -work 2s` | simulated processing time per order (default 1s) |

Set `AMQP_URL` to point at a broker other than `amqp://guest:guest@localhost:5672/`.

## Architecture

```
                                   RabbitMQ
 +-----------+   publish     +-------------------+   deliver (prefetch=1)   +-----------+
 | Producer  | ------------> |  queue: "orders"  | -----------------------> | Worker A  |---+
 | (process) |  persistent,  |  durable          | -----------------------> | Worker B  |---+
 +-----------+  confirmed    +-------------------+ <----------------------- +-----------+   |
                                     ^                 ACK after processing                 |
                                     |                                                      |
                                     +-- unACKed message requeued if a worker's            |
                                         connection drops                                   |
                                                                                            v
                                                                        processed_events.txt
                                                                        (event_ids, shared by
                                                                         all workers, flock)
```

Message body:

```json
{"event_id":"evt-1792976495-1001","order_id":"ORD-1001","item_id":"ITEM-01","quantity":1}
```

Design choices:

- The queue is durable and messages are persistent. The producer waits for publisher confirms, so a message counts as sent only once the broker has stored it.
- Workers consume with manual ACK and `Qos(prefetch=1)`. Each worker holds at most one unacknowledged message, so RabbitMQ hands the next message to whichever worker is free.
- The dedup store is a file shared by all workers instead of an in-memory set. With an in-memory set, a duplicate that lands on a different worker than the original would not be caught (this happens in the run below). Reads take a shared `flock` and writes take an exclusive one.
- A worker writes the `event_id` to the store after the business action and before the ACK. If it crashes between the two, the redelivered message is recognized as a duplicate.

## 1. Successful processing and competing consumers

Workers A and B were started, then `./bin/producer -n 6 -start 1001` was run.

Producer (`logs/1-producer.log`):

```
[producer] 18:01:35.339207 published {"event_id":"evt-1792976495-1001","order_id":"ORD-1001","item_id":"ITEM-01","quantity":1}
[producer] 18:01:35.340881 published {"event_id":"evt-1792976495-1002","order_id":"ORD-1002","item_id":"ITEM-02","quantity":2}
[producer] 18:01:35.342076 published {"event_id":"evt-1792976495-1003","order_id":"ORD-1003","item_id":"ITEM-03","quantity":3}
[producer] 18:01:35.343210 published {"event_id":"evt-1792976495-1004","order_id":"ORD-1004","item_id":"ITEM-04","quantity":1}
[producer] 18:01:35.344356 published {"event_id":"evt-1792976495-1005","order_id":"ORD-1005","item_id":"ITEM-05","quantity":2}
[producer] 18:01:35.345297 published {"event_id":"evt-1792976495-1006","order_id":"ORD-1006","item_id":"ITEM-01","quantity":3}
[producer] 18:01:35.345335 done, 6 messages published
```

Worker A (`logs/1-worker-A.log`):

```
[worker A] 18:01:34.551080 waiting for orders on queue "orders"
[worker A] 18:01:35.340730 received event_id=evt-1792976495-1002 order_id=ORD-1002 item_id=ITEM-02 qty=2 redelivered=false
[worker A] 18:01:35.340945 processing order ORD-1002 (2 x ITEM-02)...
[worker A] 18:01:36.342552 done order ORD-1002, ACK sent
[worker A] 18:01:36.343528 received event_id=evt-1792976495-1004 order_id=ORD-1004 item_id=ITEM-04 qty=1 redelivered=false
[worker A] 18:01:36.343639 processing order ORD-1004 (1 x ITEM-04)...
[worker A] 18:01:37.345144 done order ORD-1004, ACK sent
[worker A] 18:01:37.347746 received event_id=evt-1792976495-1006 order_id=ORD-1006 item_id=ITEM-01 qty=3 redelivered=false
[worker A] 18:01:37.347975 processing order ORD-1006 (3 x ITEM-01)...
[worker A] 18:01:38.350232 done order ORD-1006, ACK sent
```

Worker B (`logs/1-worker-B.log`):

```
[worker B] 18:01:34.551059 waiting for orders on queue "orders"
[worker B] 18:01:35.338357 received event_id=evt-1792976495-1001 order_id=ORD-1001 item_id=ITEM-01 qty=1 redelivered=false
[worker B] 18:01:35.338772 processing order ORD-1001 (1 x ITEM-01)...
[worker B] 18:01:36.340934 done order ORD-1001, ACK sent
[worker B] 18:01:36.343157 received event_id=evt-1792976495-1003 order_id=ORD-1003 item_id=ITEM-03 qty=3 redelivered=false
[worker B] 18:01:36.343400 processing order ORD-1003 (3 x ITEM-03)...
[worker B] 18:01:37.344996 done order ORD-1003, ACK sent
[worker B] 18:01:37.347746 received event_id=evt-1792976495-1005 order_id=ORD-1005 item_id=ITEM-05 qty=2 redelivered=false
[worker B] 18:01:37.347981 processing order ORD-1005 (2 x ITEM-05)...
[worker B] 18:01:38.349752 done order ORD-1005, ACK sent
```

B handled 1001, 1003 and 1005; A handled 1002, 1004 and 1006. No order appears in both logs, and the two workers ran in parallel (both finished their third order at 18:01:38). Each message went to exactly one consumer.

## 2. Failure and redelivery

Worker B was started with `-crash-on ORD-2001`, then worker A was started normally, then `./bin/producer -n 4 -start 2001` was run.

Worker B (`logs/2-worker-B.log`) receives ORD-2001 and exits without ACKing:

```
[worker B] 18:01:47.178834 waiting for orders on queue "orders"
[worker B] 18:01:48.734180 received event_id=evt-1792976508-2001 order_id=ORD-2001 item_id=ITEM-01 qty=1 redelivered=false
[worker B] 18:01:48.734313 SIMULATED CRASH: exiting before ACK for ORD-2001
```

Worker A (`logs/2-worker-A.log`) gets the same message about 5 ms later. RabbitMQ marks it `redelivered=true`. A then continues with the rest of the queue:

```
[worker A] 18:01:47.693355 waiting for orders on queue "orders"
[worker A] 18:01:48.739745 received event_id=evt-1792976508-2001 order_id=ORD-2001 item_id=ITEM-01 qty=1 redelivered=true
[worker A] 18:01:48.739980 processing order ORD-2001 (1 x ITEM-01)...
[worker A] 18:01:49.741812 done order ORD-2001, ACK sent
[worker A] 18:01:49.744281 received event_id=evt-1792976508-2002 order_id=ORD-2002 item_id=ITEM-02 qty=2 redelivered=false
[worker A] 18:01:49.744574 processing order ORD-2002 (2 x ITEM-02)...
[worker A] 18:01:50.746094 done order ORD-2002, ACK sent
[worker A] 18:01:50.748348 received event_id=evt-1792976508-2003 order_id=ORD-2003 item_id=ITEM-03 qty=3 redelivered=false
[worker A] 18:01:50.748615 processing order ORD-2003 (3 x ITEM-03)...
[worker A] 18:01:51.749990 done order ORD-2003, ACK sent
[worker A] 18:01:51.751931 received event_id=evt-1792976508-2004 order_id=ORD-2004 item_id=ITEM-04 qty=1 redelivered=false
[worker A] 18:01:51.752284 processing order ORD-2004 (1 x ITEM-04)...
[worker A] 18:01:52.754471 done order ORD-2004, ACK sent
```

Afterwards, worker B's exit status was 1, worker A was still running, and the queue was empty with one consumer left (`logs/2-queue-state.txt`, from `rabbitmqctl list_queues`):

```
name	messages_ready	messages_unacknowledged	consumers
orders	0	0	1
```

No message was lost, and the broker, producer and remaining worker kept running. Worker A stayed up into the next test.

## 3. Duplicate handling

Worker A was still running from test 2. A fresh worker B was started, then `./bin/producer -n 3 -start 3001 -dup` was run. The fourth message reuses ORD-3001's `event_id`.

Producer (`logs/3-producer.log`):

```
[producer] 18:01:56.461484 published {"event_id":"evt-1792976516-3001","order_id":"ORD-3001","item_id":"ITEM-01","quantity":1}
[producer] 18:01:56.463940 published {"event_id":"evt-1792976516-3002","order_id":"ORD-3002","item_id":"ITEM-02","quantity":2}
[producer] 18:01:56.466082 published {"event_id":"evt-1792976516-3003","order_id":"ORD-3003","item_id":"ITEM-03","quantity":3}
[producer] 18:01:56.468107 published {"event_id":"evt-1792976516-3001","order_id":"ORD-3001","item_id":"ITEM-01","quantity":1}
[producer] 18:01:56.468169 done, 4 messages published
```

Worker A (`logs/3-worker-A.log`) processes the original ORD-3001:

```
[worker A] 18:01:56.459319 received event_id=evt-1792976516-3001 order_id=ORD-3001 item_id=ITEM-01 qty=1 redelivered=false
[worker A] 18:01:56.460255 processing order ORD-3001 (1 x ITEM-01)...
[worker A] 18:01:57.461776 done order ORD-3001, ACK sent
[worker A] 18:01:57.464725 received event_id=evt-1792976516-3003 order_id=ORD-3003 item_id=ITEM-03 qty=3 redelivered=false
[worker A] 18:01:57.464955 processing order ORD-3003 (3 x ITEM-03)...
[worker A] 18:01:58.466927 done order ORD-3003, ACK sent
```

Worker B (`logs/3-worker-B.log`) receives the copy, finds the `event_id` in the shared store, and skips it:

```
[worker B] 18:01:55.430737 waiting for orders on queue "orders"
[worker B] 18:01:56.463134 received event_id=evt-1792976516-3002 order_id=ORD-3002 item_id=ITEM-02 qty=2 redelivered=false
[worker B] 18:01:56.463381 processing order ORD-3002 (2 x ITEM-02)...
[worker B] 18:01:57.464169 done order ORD-3002, ACK sent
[worker B] 18:01:57.465248 received event_id=evt-1792976516-3001 order_id=ORD-3001 item_id=ITEM-01 qty=1 redelivered=false
[worker B] 18:01:57.465420 DUPLICATE detected event_id=evt-1792976516-3001, skipping business action
```

The duplicate is still ACKed, so it leaves the queue instead of looping. `logs/processed_events.txt` lists each of the 13 event IDs from all three runs once, including `evt-1792976516-3001`.

The original and the duplicate went to different workers here, which is why the store has to be shared. A per-process in-memory set in worker B would have processed ORD-3001 a second time.

## Reflection

RabbitMQ only removes a message once it receives an ACK, so any failure between delivery and ACK (a crash, a dropped connection, a timeout) makes the broker requeue it and deliver it again, possibly to a different worker. Producers can also send the same event twice, for example when they retry after a lost publisher confirm. This at-least-once behavior means a message can be processed, then delivered again because the ACK never arrived. Idempotent handling makes the second delivery harmless: the worker records each `event_id` it has processed and skips the business action when it sees that ID again, so the order is fulfilled once no matter how many times its message arrives.

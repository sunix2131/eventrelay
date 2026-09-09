# EventRelay

This service accepts a payment command, stores the payment and its integration event in one PostgreSQL transaction, and publishes committed outbox rows to Kafka.

It focuses on the failure window that matters in this pattern. A process can stop after Kafka acknowledges an event but before PostgreSQL records it as published. The row will be retried and Kafka may receive the event again, so the included consumer records processed event IDs in the same transaction as its projection update.

An independent project with synthetic payment commands; no employer code or customer data.

## Write path

`POST /payments` requires an `Idempotency-Key`. The service takes a PostgreSQL transaction-scoped advisory lock derived from that key, then checks or creates the command. The payment row and `payment.accepted.v1` outbox row commit together.

```text
HTTP command
  └─ PostgreSQL transaction
       ├─ payment
       └─ outbox_event (PENDING)

outbox poller
  ├─ SELECT ... FOR UPDATE SKIP LOCKED
  ├─ Kafka send with acks=all
  └─ PUBLISHED, delayed retry, or DEAD

Kafka consumer
  └─ PostgreSQL transaction
       ├─ processed_event
       └─ payment_projection
```

The same idempotency key and request returns the original payment. Reusing the key with another account, amount or currency returns `409`. Money is stored as `numeric(19,2)` and request hashing uses its canonical two-decimal representation.

## Delivery behavior

Publishers claim ordered batches with `FOR UPDATE SKIP LOCKED`, allowing several instances to work without selecting the same row. A failed send increments the attempt count and schedules exponential backoff from 1 second to 5 minutes. After eight attempts the row moves to `DEAD` for operator review.

Kafka producer idempotence reduces client retry duplicates but cannot close the database/Kafka commit gap. Delivery is at-least-once. The consumer records processed event IDs with its projection update, making repeated delivery a no-op. A different event claiming the same payment is rejected rather than overwriting its amount.

Malformed consumer records are retried three times and then published to `payment-events.DLT`. If that publication fails, recovery also fails and the source record remains eligible for retry. Publisher interruption rolls back the batch and stops sending; database failures are propagated rather than counted as Kafka failures.

## Run locally

Requirements are Java 21, Maven and Docker.

```bash
docker compose up -d
mvn spring-boot:run
```

Create a payment:

```bash
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: checkout-481' \
  -d '{"accountId":"3b239b56-c8ef-4ca1-af51-83ac019be6dd","amount":"1250.40","currency":"RUB"}' \
  http://localhost:8080/payments
```

PostgreSQL and Kafka addresses can be changed with `DATABASE_URL`, `DATABASE_USER`, `DATABASE_PASSWORD` and `KAFKA_BOOTSTRAP_SERVERS`.

## Verification

```bash
mvn verify
```

Unit tests cover acknowledgement, retry scheduling, dead-state transition and canonical request hashes. Testcontainers integration tests run PostgreSQL 17 and Apache Kafka and cover atomic row creation, same-key replay, changed-payload conflict, concurrent retries, Kafka publication and consumer deduplication.

## Boundaries

- The publisher holds PostgreSQL row locks while waiting for Kafka acknowledgement. This keeps the state transition small and explicit, but batch size and send timeout must remain bounded.
- `DEAD` rows require an operator-driven replay policy; this repository does not include an administration endpoint.
- The consumer projection is deliberately small. A real service would give each downstream consumer its own durable deduplication boundary.
- The example has no authentication or tenant model and is intended for local engineering work, not direct internet exposure.

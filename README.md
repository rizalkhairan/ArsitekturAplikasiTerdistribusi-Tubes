# Tugas Besar IF4031 Arsitektur Aplikasi Terdistribusi

## Run the system from a clean start

1. Ensure Docker Desktop is running with Linux containers enabled.

2. Create local environment files from the committed examples. The generated
   `.env` files are ignored by Git.

   ```bash
   cp services/bmkg/.env.example services/bmkg/.env
   cp services/pvmbg/.env.example services/pvmbg/.env
   cp services/bnpb/.env.example services/bnpb/.env
   ```

3. Start with fresh Kafka, source, and BNPB data:

   ```bash
   docker compose down -v
   docker compose up --build -d
   docker compose ps
   ```

   The default stack starts Apache Kafka in KRaft mode, topic initialization,
   BMKG, PVMBG, BNPB, `dashboard-updater`, and `notifier`. Kafka creates
   `hazard-events` (three partitions) and `hazard-events-dlq` (one partition),
   both retained for seven days.

4. Check service health:

   ```bash
   curl --fail http://localhost:8080/health
   curl --fail http://localhost:8091/health
   curl --fail http://localhost:8092/health
   ```

5. Stop the stack when finished:

   ```bash
   docker compose down
   ```

## Configuration variables

All values are read from environment variables. Use the checked-in
`.env.example` files as local-development templates; do not commit real
credentials in `.env` files.

### BMKG (`services/bmkg/.env`)

| Variable | Purpose | Example |
| --- | --- | --- |
| `PORT` | gRPC listen port | `8081` |
| `BMKG_API_KEY` | Key BNPB sends to BMKG | `local-dev-bmkg-key` |
| `DB_PATH` | BMKG-owned SQLite file | `/app/data/bmkg.db` |

### PVMBG (`services/pvmbg/.env`)

| Variable | Purpose | Example |
| --- | --- | --- |
| `PORT` | HTTP listen port | `8082` |
| `PVMBG_TOKEN` | Bearer token BNPB sends to PVMBG | `local-dev-pvmbg-token` |
| `ADMIN_USERNAME` | PVMBG mock admin username | `admin` |
| `ADMIN_PASSWORD` | PVMBG mock admin password | `local-dev-admin-password` |
| `PVMBG_MIN_DELAY_MS` | Minimum mock response delay | `10` |
| `PVMBG_MAX_DELAY_MS` | Maximum mock response delay | `20` |
| `DB_PATH` | PVMBG-owned SQLite file | `/app/data/pvmbg.db` |

### BNPB (`services/bnpb/.env`)

| Variable | Purpose | Example |
| --- | --- | --- |
| `DB_PATH` | BNPB Canonical Store SQLite file | `/app/data/bnpb.db` |
| `BMKG_GRPC_ADDR` | BMKG Docker-network endpoint | `bmkg:8081` |
| `BMKG_API_KEY` | Must match BMKG's key | `local-dev-bmkg-key` |
| `PVMBG_BASE_URL` | PVMBG Docker-network endpoint | `http://pvmbg:8082` |
| `PVMBG_TOKEN` | Must match PVMBG's token | `local-dev-pvmbg-token` |
| `UPSTREAM_TIMEOUT` | BMKG/PVMBG request timeout | `5s` |
| `POLL_INTERVAL` | BNPB source poll interval | `3s` |
| `HEALTH_ADDR` | BNPB health server address | `:8080` |
| `KAFKA_BROKERS` | Apache Kafka Docker-network endpoint | `kafka:9092` |
| `KAFKA_TOPIC_HAZARD_EVENTS` | Canonical hazard event topic | `hazard-events` |
| `KAFKA_CLIENT_ID` | BNPB Kafka client identifier | `bnpb-aggregator` |
| `KAFKA_PUBLISH_RETRY_LIMIT` | Outbox publish attempts | `3` |
| `KAFKA_PUBLISH_RETRY_BACKOFF` | Delay between outbox retries | `500ms` |
| `OUTBOX_POLL_INTERVAL` | Pending-outbox scan interval | `1s` |

### Consumers

`dashboard-updater`, `notifier`, and `analytics-consumer` use these shared
variables. Their Docker Compose configuration uses `kafka:9092`; each service
also has an `.env.example` for running it independently.

| Variable | Purpose | Example |
| --- | --- | --- |
| `KAFKA_BROKERS` | Apache Kafka Docker-network endpoint | `kafka:9092` |
| `KAFKA_TOPIC_HAZARD_EVENTS` | Source event topic | `hazard-events` |
| `KAFKA_DLQ_TOPIC` | Permanent-failure topic | `hazard-events-dlq` |
| `HEALTH_ADDR` | Consumer health server address | `:8080` |
| `PROCESS_RETRY_LIMIT` | Attempts before DLQ routing | `3` |
| `PROCESS_RETRY_BACKOFF` | Delay between retry attempts | `500ms` |
| `IDEMPOTENCY_CACHE_SIZE` | Bounded in-memory `event_id` deduplication entries | `10000` |
| `NOTIFIER_PROCESSING_DELAY` | Notifier-only simulated processing delay | `0s` |

## Testing scenarios

### Problems 1–4

#### TODO

### Problem 5: Event-Driven Architecture

Run the clean-start procedure above before each independent scenario.

#### 1. New hazards are persisted and published through Kafka

```bash
docker compose logs --tail=100 bnpb
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic hazard-events
```

Expected BNPB logs include `kafka publish ... outcome=acknowledged` followed by
`outbox publish succeeded ... outcome=published`. BNPB writes its Canonical
Store and outbox transaction before Kafka publishing; no consumer is called
directly.

#### 2. Two independent consumers receive the same stream

```bash
docker compose logs --tail=100 dashboard-updater
docker compose logs --tail=100 notifier
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group dashboard-updater-group
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notifier-group
```

Expected logs contain `dashboard updated ... result=success` and
`notification simulated ... result=success`. The groups are separate, so both
receive each event.

#### 3. Stop notifier while BNPB and dashboard continue

```bash
docker compose stop notifier
sleep 15
docker compose logs --tail=100 bnpb
docker compose logs --tail=100 dashboard-updater
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notifier-group
```

Expected results: BNPB keeps publishing, dashboard keeps processing, and
notifier-group lag grows. To make notifier visibly slow before stopping it, set
`NOTIFIER_PROCESSING_DELAY=5s` for the `notifier` Compose service and recreate
that container.

#### 4. Restart notifier and verify retained-message catch-up

```bash
docker compose start notifier
docker compose logs -f notifier
```

Expected notifier logs process messages produced while it was stopped. The
system uses at-least-once delivery: Kafka retains messages for seven days, and
the notifier resumes from its last committed offset. Duplicate `event_id`
deliveries are logged as `result=duplicate` and do not repeat consumer effects.

#### 5. Add analytics without changing BNPB

```bash
docker compose --profile analytics up -d --build analytics-consumer
docker compose logs -f analytics-consumer
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group analytics-group
```

Expected analytics logs contain `analytics aggregate ... result=success`.
Because `analytics-group` is new and consumers use `auto.offset.reset=earliest`,
it reads retained events without BNPB changes or redeployment.
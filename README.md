# Tugas Besar IF4031 — Arsitektur Aplikasi Terdistribusi

Platform koordinasi kebencanaan yang menjembatani **BMKG**, **PVMBG**, dan **BNPB**.
BMKG dan PVMBG adalah mock service pasif; BNPB melakukan polling, memetakan data ke
format kanonik `HazardEvent`, menyimpannya, dan mempublikasikannya ke message broker
untuk dikonsumsi beberapa consumer independen.

Cakupan: dua instansi sumber (BMKG, PVMBG), dua jenis bencana (seismik, vulkanik),
BNPB sebagai node koordinasi.

Pengembangan M2 mengacu pada [arsitektur](services/ARCHITECTURE.md),
[kontrak API kanonik](contracts/m2/README.md), serta
[konvensi platform dan telemetri](contracts/m2/PLATFORM.md).
[Helper Go bersama](services/shared/README.md) menyediakan contoh instrumentasi dua service.

---

## 1. Arsitektur

```
                         ┌──────────────────────────┐
                         │        CLI (Java)        │
                         └─────────────┬────────────┘
                                       │ HTTP (Bearer JWT)
                         ┌─────────────▼────────────┐        ┌──────────────┐
                         │ Client-Facing API        │─proxy─▶│ Auth Service │
                         │          :9080           │        │     :8000    │
                         └─────────────┬────────────┘        └──────────────┘
                                       │ HTTP (X-Internal-Key)
                         ┌─────────────▼────────────┐        ┌────────────────────┐
                         │ BNPB Aggregator          │─gRPC──▶│ BMKG mock  :8081   │
                         │  read API  :8085         │        └────────────────────┘
                         │  health    :8086         │        ┌────────────────────┐
                         │  Canonical Store (SQLite)│─REST──▶│ PVMBG mock :8082   │
                         │  Outbox                  │        └────────────────────┘
                         └─────────────┬────────────┘
                                       │ publish HazardEvent
                         ┌─────────────▼────────────────────────────┐
                         │            Kafka (KRaft) :9092           │
                         └┬────────────────────────┬────────────────┬┘
                    ┌─────▼──────────────┐  ┌──────▼───────┐  ┌─────▼──────────────┐
                    │ dashboard-updater  │  │ notifier     │  │ analytics-consumer │
                    │       :8091        │  │     :8092    │  │   :8093            │
                    └────────────────────┘  └──────────────┘  └────────────────────┘
```

### Komponen

| Service | Container port | Host port | Peran |
|---------|----------------|-----------|-------|
| BMKG mock | 8081 | 8081 | Sumber data seismik & tsunami (gRPC) |
| PVMBG mock | 8082 | 8082 | Sumber data vulkanik (REST) |
| BNPB Aggregator | 8085 (read API), 8080 (health) | 8085, 8086 | Polling, mapping kanonik, Canonical Store, outbox |
| Auth Service | 8000 | 8000 | Menerbitkan access + refresh token (JWT) |
| Client-Facing API | 8080 | 9080 | Gateway client; filter scope; rate limit; proxy auth |
| Kafka | 9092 | — | Broker event-driven (`hazard-events`, `hazard-events-dlq`) |
| dashboard-updater | 8080 | 8091 | Consumer #1 |
| notifier | 8080 | 8092 | Consumer #2 |
| analytics-consumer | 8080 | 8093 | Consumer tambahan (profile `analytics`) |

> Port dapat diubah melalui environment variable. Port di atas adalah default yang
> dipakai `docker-compose.yml` dan CLI.

---

## 2. Menjalankan dari kondisi awal

**Prasyarat:** Docker Desktop (Linux containers) dan Java 17+ (untuk CLI).

```bash
# 1. Salin template environment
cp .env.example .env
cp services/bmkg/.env.example services/bmkg/.env
cp services/pvmbg/.env.example services/pvmbg/.env
cp services/bnpb/.env.example services/bnpb/.env
cp services/client-api/.env.example services/client-api/.env

# 2. Nyalakan seluruh sistem
docker compose up --build -d

# 3. Tunggu ±20 detik, lalu cek status
docker compose ps
```

Untuk memulai clean (menghapus volume Kafka/DB):

```bash
docker compose down -v
docker compose up --build -d
```

Health check cepat:

```bash
curl http://localhost:9080/health     # Client-Facing API
curl http://localhost:8082/health     # PVMBG
curl http://localhost:8000/health     # Auth
curl http://localhost:8086/health     # BNPB health
```

### Menjalankan CLI

```bash
cd client
mvn package
java -jar target/bnpb-cli-client-1.0.0.jar
```

CLI hanya berbicara ke Client-Facing API di `http://localhost:9080`.

---

## 3. Variabel konfigurasi

### Root `.env`

| Variabel | Default contoh | Keterangan |
| --- | --- | --- |
| `JWT_SECRET` | `local-dev-jwt-secret` | Kunci JWT (harus sama untuk Auth & Client-Facing API) |
| `ACCESS_TOKEN_TTL_SECONDS` | `60` | TTL access token (default 60 detik) |
| `REFRESH_TOKEN_TTL_HOURS` | `24` | TTL refresh token |
| `MEDIA_USER_PASSWORD` | `media-pass-2026` | Password akun media |
| `FIELD_USER_PASSWORD` | `field-pass-2026` | Password akun tim lapangan |
| `INTERNAL_USER_PASSWORD` | `internal-pass-2026` | Password akun internal |
| `INTERNAL_API_KEY` | `local-dev-internal-key` | Shared secret BNPB read API ↔ Client-Facing API |
| `FIELD_REPORT_TOKEN` | `local-dev-field-report-token` | Private Client API ↔ ingest API credential |
| `AUDIT_TOKEN` | `local-dev-audit-token` | Local Audit access to C's unprocessed-message listing |
| `OPS_TOKEN` | `local-dev-ops-token` | Local Internal Ops raw inspection and replay credential |
| `PIPELINE_INTERNAL_TOKEN` | `local-dev-pipeline-internal-token` | BNPB poller → C quarantine API credential |

### `services/bmkg/.env`

| Variabel | Contoh | Keterangan |
| --- | --- | --- |
| `PORT` | `8081` | Port gRPC |
| `BMKG_API_KEY` | `local-dev-bmkg-key` | Key yang divalidasi lewat metadata `x-bmkg-key` |
| `DB_PATH` | `/app/data/bmkg.db` | SQLite milik BMKG |

### `services/pvmbg/.env`

| Variabel | Contoh | Keterangan |
| --- | --- | --- |
| `PORT` | `8082` | Port HTTP |
| `PVMBG_TOKEN` | `local-dev-pvmbg-token` | Bearer token (`Authorization: Bearer <token>`) |
| `ADMIN_USERNAME` | `admin` | Basic auth untuk endpoint `/admin/*` |
| `ADMIN_PASSWORD` | `local-dev-admin-password` | Password admin mock |
| `PVMBG_MIN_DELAY_MS` | `500` | Batas bawah delay simulasi |
| `PVMBG_MAX_DELAY_MS` | `3000` | Batas atas delay simulasi |
| `DB_PATH` | `/app/data/pvmbg.db` | SQLite milik PVMBG |

### `services/bnpb/.env`

| Variabel | Contoh | Keterangan |
| --- | --- | --- |
| `DB_PATH` | `/app/data/bnpb.db` | Canonical Store (SQLite) |
| `DB_READ_CONNECTIONS` | `8` | Batas koneksi pool baca read-only; pool tulis memakai satu koneksi |
| `BMKG_GRPC_ADDR` | `bmkg:8081` | Alamat gRPC BMKG |
| `BMKG_API_KEY` | `local-dev-bmkg-key` | Harus sama dengan BMKG |
| `PVMBG_BASE_URL` | `http://pvmbg:8082` | Base URL PVMBG |
| `PVMBG_TOKEN` | `local-dev-pvmbg-token` | Harus sama dengan PVMBG |
| `UPSTREAM_TIMEOUT` | `5s` | Timeout request ke sumber |
| `POLL_INTERVAL` | `3s` | Interval polling (disarankan 2–5 detik) |
| `BNPB_API_PORT` | `8085` | Port read API |
| `HEALTH_ADDR` | `:8080` | Alamat health server |
| `INTERNAL_API_KEY` | `local-dev-internal-key` | Shared secret (di-inject dari root `.env`) |
| `STALENESS_THRESHOLD` | `10s` | Ambang data dianggap basi saat sumber gagal |
| `KAFKA_BROKERS` | `kafka:9092` | Broker Kafka |
| `KAFKA_TOPIC_HAZARD_EVENTS` | `hazard-events` | Topic event kanonik |
| `KAFKA_CLIENT_ID` | `bnpb-aggregator` | Client ID producer |
| `KAFKA_PUBLISH_RETRY_LIMIT` | `3` | Jumlah percobaan publish |
| `KAFKA_PUBLISH_RETRY_BACKOFF` | `500ms` | Backoff antar percobaan |
| `OUTBOX_POLL_INTERVAL` | `1s` | Interval scan outbox |

### `services/client-api/.env`

| Variabel | Contoh | Keterangan |
| --- | --- | --- |
| `PORT` | `8080` | Port HTTP |
| `BNPB_API_URL` | `http://bnpb:8085` | Read API Aggregator |
| `BNPB_INTERNAL_KEY` | `local-dev-internal-key` | Harus sama dengan `INTERNAL_API_KEY` |
| `AUTH_SERVICE_URL` | `http://auth:8000` | Alamat Auth Service |
| `JWT_SECRET` | `local-dev-jwt-secret` | Harus sama dengan Auth Service |
| `UPSTREAM_TIMEOUT` | `5s` | Timeout request ke BNPB |
| `MAX_CONCURRENT_REQUESTS` | `100` | Batas concurrency (kelebihan → HTTP 429) |
| `TOKEN_TTL_SECONDS` | `60` | Batas umur token yang diterima |
| `MAX_LIMIT` | `100` | Batas `limit` query |

### Consumer (`dashboard-updater`, `notifier`, `analytics-consumer`)

| Variabel | Contoh | Keterangan |
| --- | --- | --- |
| `KAFKA_BROKERS` | `kafka:9092` | Broker |
| `KAFKA_TOPIC_HAZARD_EVENTS` | `hazard-events` | Topic sumber |
| `KAFKA_DLQ_TOPIC` | `hazard-events-dlq` | Topic dead-letter |
| `HEALTH_ADDR` | `:8080` | Health server |
| `PROCESS_RETRY_LIMIT` | `3` | Percobaan sebelum DLQ |
| `PROCESS_RETRY_BACKOFF` | `500ms` | Backoff antar percobaan |
| `IDEMPOTENCY_CACHE_SIZE` | `10000` | Ukuran cache dedup `event_id` |
| `NOTIFIER_PROCESSING_DELAY` | `0s` | Delay buatan khusus notifier |

---

## 4. Kontrak mock service

### BMKG (`:8081`)

BMKG diakses melalui **gRPC** (proto ada di `services/bmkg/proto/bmkg.proto`).
Logika endpoint yang ditetapkan kontrak (mengambil event sejak timestamp tertentu,
warning tsunami terpisah, dan health) dipetakan ke RPC berikut. Autentikasi memakai
metadata `x-bmkg-key`.

| RPC | Auth | Keterangan |
| --- | --- | --- |
| `bmkg.BMKG/GetSeismicEvents` (param `since`) | `x-bmkg-key` | Daftar `SeismicEvent` |
| `bmkg.BMKG/GetTsunamiWarnings` (param `since`) | `x-bmkg-key` | Daftar `TsunamiWarning` |
| `bmkg.BMKG/Health` | — | Health check |

- Seed ≥20 event saat start.
- Event baru setiap ±10 detik.
- Delay tetap 50–150 ms.
- `TsunamiWarning` dibuat hanya untuk event `potential_tsunami = true`.

### PVMBG (`:8082`)

| Method | Path | Auth | Keterangan |
| --- | --- | --- | --- |
| GET | `/volcanic-reports?since=<ISO8601>` | `Authorization: Bearer <PVMBG_TOKEN>` | Daftar `VolcanicReport` |
| POST | `/admin/schema-version` | Basic admin | Aktifkan/nonaktifkan `confidence_level` saat berjalan |
| POST | `/admin/outage` | Basic admin | Nyalakan/matikan outage seketika |
| GET | `/health` | — | Health check |

- Seed ≥20 laporan saat start; laporan baru setiap ±10 detik.
- Delay acak `PVMBG_MIN_DELAY_MS`–`PVMBG_MAX_DELAY_MS`.
- Saat `outage` aktif, semua permintaan data mengembalikan `503` seketika.

**Perbedaan kredensial:** key BMKG ditolak oleh PVMBG (`401`) dan sebaliknya
(BMKG gRPC → `Unauthenticated`). Formatnya pun berbeda (metadata `x-bmkg-key` vs
`Authorization: Bearer`).

---

## 5. Format kanonik `HazardEvent`

| Field | Sumber | Klasifikasi |
| --- | --- | --- |
| `hazard_id` | Dibangkitkan BNPB (UUIDv7) | Ringkasan |
| `source` | `BMKG` / `PVMBG` | Ringkasan |
| `source_ref_id` | `event_id` / `report_id` | Mentah |
| `hazard_type` | `SEISMIC` / `VOLCANIC` | Ringkasan |
| `severity` | `NORMAL/WASPADA/SIAGA/AWAS` | Ringkasan |
| `area_name` | `region_name` / nama gunung | Ringkasan |
| `latitude`, `longitude` | Episentrum / koordinat gunung | Mentah |
| `occurred_at` | Waktu kejadian sumber | Ringkasan |
| `ingested_at` | Waktu diterima BNPB | Ringkasan |
| `attributes` | Field spesifik sumber | Mentah |

- `SeismicEvent → HazardEvent`: severity dari `TsunamiWarning` terkait; jika tidak ada
  warning, dari magnitudo (`<5.0` NORMAL, `5.0–6.5` WASPADA, `≥6.5` SIAGA).
  `attributes` memuat `magnitude`, `depth_km`, `potential_tsunami`, dan warning terkait.
- `VolcanicReport → HazardEvent`: `alert_level` dipetakan langsung; `attributes`
  memuat `eruption_count_24h`, `ash_column_height_m`, dan **field tak dikenal seperti
  `confidence_level`** (kebijakan *additive / forward to attributes*).

---

## 6. Client downstream & hak akses

| Akun (username / password) | Scope | Field |
| --- | --- | --- |
| `media-user` / `media-pass-2026` | `summary` | Ringkasan saja |
| `field-user` / `field-pass-2026` | `full` | Ringkasan + Mentah |
| `internal-user` / `internal-pass-2026` | `full` | Ringkasan + Mentah |

- Tiga kredensial berbeda; pembeda ditegakkan di sisi BNPB/Client-Facing API, bukan di client.
- Media yang meminta field mentah eksplisit (`?fields=latitude`, `?include=attributes`,
  `X-Raw-Fields: true`) ditolak `403`.
- Access token default ber-TTL 60 detik dan mendukung refresh tanpa login ulang.
- Seluruh client M1 bersifat read-only.

---

## 7. Endpoint API

### Client-Facing API (`:9080`)

| Method | Path | Auth | Keterangan |
| --- | --- | --- | --- |
| GET | `/health` | — | Health check |
| POST | `/auth/login` | — | Proxy login ke Auth Service |
| POST | `/auth/refresh` | — | Proxy refresh ke Auth Service |
| GET | `/hazards?source=&severity=&since=&limit=&fields=` | Bearer | Daftar hazard (difilter per scope) |
| GET | `/hazards/{id}` | Bearer | Satu hazard |

### Auth Service (`:8000`)

`GET /health`, `POST /auth/login`, `POST /auth/refresh`.

### BNPB read API (`:8085`, hanya untuk internal)

`GET /health`, `GET /hazards`, `GET /hazards/{id}` — wajib header `X-Internal-Key`.
Canonical Store tidak pernah diekspos langsung; seluruh komponen lain mengaksesnya
lewat API Aggregator ini.

---

## 8. Skenario pengujian

Jalankan `docker compose down -v && docker compose up --build -d` sebelum tiap
skenario yang butuh kondisi bersih.

### Problem 1 — Interoperabilitas & evolusi skema

```bash
TOKEN=$(curl -s -X POST http://localhost:8082/admin/schema-version \
  -u admin:local-dev-admin-password -H 'Content-Type: application/json' \
  -d '{"enabled":true}')
```

1. Sebelum toggle, `GET /volcanic-reports` (Bearer PVMBG) **tidak** memuat `confidence_level`.
2. Panggil `POST /admin/schema-version {"enabled":true}` saat sistem berjalan.
3. PVMBG langsung memuat `confidence_level` **tanpa restart**.
4. BNPB (tanpa restart) tetap memetakan; field baru masuk ke `attributes`. Cek via CLI:
   `login field-user field-pass-2026` lalu `hazards PVMBG 5`.

Kebijakan: perubahan skema **aditif** didukung; field tak dikenal diteruskan ke
`attributes`. Perubahan yang menghapus/mengganti tipe field tidak didukung.

### Problem 2 — Ketahanan & degradasi terkendali

BNPB memakai dua pool SQLite pada database WAL yang sama: satu koneksi tulis
untuk transaksi hazard/cursor/outbox dan pool baca read-only (default 8 koneksi)
untuk API, lookup agregasi, cursor, dan query outbox. Pembaca dapat berjalan saat
transaksi tulis aktif tanpa menunggu koneksi penulis. Kedua poller tetap independen.

**2a. Head-of-line blocking** — dengan PVMBG diperlambat 3 detik, kirim permintaan
BMKG-only bersamaan dengan permintaan PVMBG; p95 BMKG-only harus tetap < 300 ms.

```bash
# 1. Atur delay PVMBG ke 3000 ms lalu recreate
docker compose up -d --force-recreate pvmbg
sleep 10

# 2. Jalankan BMKG-only dan PVMBG bersamaan (masing-masing 10 koneksi, 15 detik)
NET=$(docker network ls --format '{{.Name}}' | grep tubes | head -n1)
docker run --rm --network "$NET" -v "$PWD/tools/loadtest:/src" -w /src \
  golang:1.26-alpine go run . -base http://client-api:8080 \
  -path '/hazards?limit=100&source=BMKG' -concurrency 10 -duration 15s -label BMKG-only &
docker run --rm --network "$NET" -v "$PWD/tools/loadtest:/src" -w /src \
  golang:1.26-alpine go run . -base http://client-api:8080 \
  -path '/hazards?limit=100&source=PVMBG' -concurrency 10 -duration 15s -label PVMBG &
wait
```

**2b. Load test** — ≥50 koneksi paralel ≥60 detik memakai
[`tools/loadtest`](tools/loadtest/README.md):

```bash
NET=$(docker network ls --format '{{.Name}}' | grep tubes | head -n1)
docker run --rm --network "$NET" -v "$PWD/tools/loadtest:/src" -w /src \
  golang:1.26-alpine go run . -base http://client-api:8080 \
  -path '/hazards?limit=50' -concurrency 50 -duration 60s -label sustained
```

**2c. Outage terkendali**

```bash
curl -u admin:local-dev-admin-password -X POST http://localhost:8082/admin/outage \
  -H 'Content-Type: application/json' -d '{"enabled":true}'
```

Tunggu ±10–15 detik, lalu cek kedua jenis permintaan lewat CLI:

```
BNPB> login field-user field-pass-2026
BNPB> hazards BMKG 5
# tetap terisi → permintaan seismik tidak terpengaruh outage PVMBG

BNPB> hazards PVMBG 5
# mengembalikan data vulkanik terakhir dari Canonical Store (bukan error)
```

Setelah selesai, matikan outage lalu cek ulang (tanpa restart):

```bash
curl -u admin:local-dev-admin-password -X POST http://localhost:8082/admin/outage \
  -H 'Content-Type: application/json' -d '{"enabled":false}'
```

```
BNPB> hazards PVMBG 5
```

> Untuk melihat header freshness:

```bash
TOKEN=$(curl -s -X POST http://localhost:9080/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"field-user","password":"field-pass-2026"}' \
  | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)

curl -s -D - -o /dev/null \
  'http://localhost:9080/hazards?source=PVMBG&limit=1' \
  -H "Authorization: Bearer $TOKEN" \
  | grep -iE 'HTTP/|X-Data-Freshness|X-Source-PVMBG-'
```

### Problem 3 — Autentikasi antar-service & trust

- **3a. Isolasi kredensial antar instansi**

  ```bash
  # key BMKG dipakai ke PVMBG → 401
  curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:8082/volcanic-reports?since=2000-01-01T00:00:00Z' \
    -H 'X-BMKG-Key: local-dev-bmkg-key'

  # token PVMBG / key salah dipakai ke BMKG gRPC → Unauthenticated
  docker run --rm --network "$(docker network ls --format '{{.Name}}' | grep tubes | head -n1)" \
    -v "$PWD/services/bmkg/proto:/proto" fullstorydev/grpcurl:latest \
    -plaintext -import-path /proto -proto bmkg.proto \
    -H 'x-bmkg-key: local-dev-pvmbg-token' \
    bmkg:8081 bmkg.BMKG/GetSeismicEvents
  ```

- **3b. Scope media** — login media, lalu `GET /hazards` hanya mengembalikan 7 field
  ringkasan; `?fields=latitude` → `403`.

  ```bash
  TOKEN=$(curl -s -X POST http://localhost:9080/auth/login \
    -H 'Content-Type: application/json' \
    -d '{"username":"media-user","password":"media-pass-2026"}' \
    | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)

  # hanya berisi hazard_id, source, hazard_type, severity, area_name, occurred_at, ingested_at
  curl -s 'http://localhost:9080/hazards?limit=1' -H "Authorization: Bearer $TOKEN"

  # minta field mentah secara eksplisit → 403
  curl -s -o /dev/null -w 'raw field HTTP %{http_code}\n' \
    'http://localhost:9080/hazards?limit=1&fields=latitude' -H "Authorization: Bearer $TOKEN"
  ```

- **3c. Expiry + refresh** — login, tunggu >60 detik → `401`; panggil
  `POST /auth/refresh` dengan `refresh_token` → token baru; token lama tetap `401`;
  refresh token lama tidak bisa dipakai lagi (rotation).

  ```bash
  LOGIN=$(curl -s -X POST http://localhost:9080/auth/login \
    -H 'Content-Type: application/json' \
    -d '{"username":"field-user","password":"field-pass-2026"}')
  ACCESS=$(echo "$LOGIN"  | grep -o '"accessToken":"[^"]*"'  | cut -d'"' -f4)
  REFRESH=$(echo "$LOGIN" | grep -o '"refreshToken":"[^"]*"' | cut -d'"' -f4)

  curl -s -o /dev/null -w 'fresh token      HTTP %{http_code}\n' \
    'http://localhost:9080/hazards?limit=1' -H "Authorization: Bearer $ACCESS"

  sleep 62   # TTL access token = 60 detik

  curl -s -o /dev/null -w 'expired token    HTTP %{http_code}\n' \
    'http://localhost:9080/hazards?limit=1' -H "Authorization: Bearer $ACCESS"

  NEW=$(curl -s -X POST http://localhost:9080/auth/refresh \
    -H 'Content-Type: application/json' -d "{\"refresh_token\":\"$REFRESH\"}")
  NEW_ACCESS=$(echo "$NEW" | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)

  curl -s -o /dev/null -w 'refreshed token  HTTP %{http_code}\n' \
    'http://localhost:9080/hazards?limit=1' -H "Authorization: Bearer $NEW_ACCESS"
  curl -s -o /dev/null -w 'old token        HTTP %{http_code}\n' \
    'http://localhost:9080/hazards?limit=1' -H "Authorization: Bearer $ACCESS"
  curl -s -o /dev/null -w 'old refresh      HTTP %{http_code}\n' -X POST \
    http://localhost:9080/auth/refresh -H 'Content-Type: application/json' \
    -d "{\"refresh_token\":\"$REFRESH\"}"
  ```

- **3d. Tidak ada kredensial ter-hardcode** — semua rahasia dibaca dari environment;
  hanya `.env.example` yang ikut ter-commit.

### Problem 4 — Microservice & flexible storage

- **4a. Independen** — catat `StartedAt` container lain, stop/rebuild/start satu
  container (`--no-deps`); `StartedAt` container lain tidak berubah dan permintaan yang tidak bergantung padanya tetap `200`.

  ```bash
  docker compose ps
  docker inspect -f '{{.Name}} {{.State.StartedAt}}' \
    $(docker compose ps -q bnpb pvmbg bmkg client-api auth kafka)

  docker compose stop notifier
  KEY='local-dev-internal-key'
  curl -s -o /dev/null -w 'BNPB   %{http_code}\n' \
    -H "X-Internal-Key: $KEY" 'http://localhost:8085/hazards?limit=1'
  TOKEN=$(curl -s -X POST http://localhost:9080/auth/login \
    -H 'Content-Type: application/json' \
    -d '{"username":"field-user","password":"field-pass-2026"}' \
    | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)
  curl -s -o /dev/null -w 'client %{http_code}\n' \
    'http://localhost:9080/hazards?source=BMKG&limit=1' -H "Authorization: Bearer $TOKEN"

  docker compose build notifier
  docker compose up -d --no-deps notifier
  docker inspect -f '{{.Name}} {{.State.StartedAt}}' \
    $(docker compose ps -q bnpb pvmbg bmkg client-api auth kafka)
  ```

- **4b. Atribut dinamis** — field baru masuk ke JSON `attributes`, bukan kolom baru,
  tanpa migrasi. Record sebelum & sesudah `confidence_level` tersimpan berdampingan dan
  keduanya dapat dibaca.

  ```bash
  KEY='local-dev-internal-key'
  curl -s -H "X-Internal-Key: $KEY" 'http://localhost:8085/hazards?source=PVMBG&limit=3'   # tanpa confidence_level
  curl -s -u admin:local-dev-admin-password -X POST http://localhost:8082/admin/schema-version \
    -H 'Content-Type: application/json' -d '{"enabled":true}'
  sleep 25
  curl -s -H "X-Internal-Key: $KEY" 'http://localhost:8085/hazards?source=PVMBG&limit=10'  # ada confidence_level

  VOL=$(docker volume ls --format '{{.Name}}' | grep bnpb-data | head -n1)
  MSYS_NO_PATHCONV=1 docker run --rm -v "$VOL":/data keinos/sqlite3:latest \
    sqlite3 -header -column /data/bnpb.db \
    "SELECT source_ref_id, json_extract(attributes,'\$.confidence_level') AS confidence_level
       FROM hazard_events WHERE source='PVMBG' ORDER BY occurred_at DESC LIMIT 20;"
  MSYS_NO_PATHCONV=1 docker run --rm -v "$VOL":/data keinos/sqlite3:latest \
    sqlite3 /data/bnpb.db ".schema hazard_events"
  ```

  `MSYS_NO_PATHCONV=1` hanya perlu di Git Bash (Windows); diabaikan di Linux/macOS.

- **4c. Kepemilikan store** — hanya Aggregator yang memegang volume `bnpb-data`; akses
  langsung ke `:8085` butuh `X-Internal-Key` (`401` tanpa key). Komponen lain membaca
  lewat API Aggregator (`BNPB_API_URL`).

  ```bash
  curl -s -o /dev/null -w 'tanpa key %{http_code}\n' http://localhost:8085/hazards
  curl -s -o /dev/null -w 'key salah %{http_code}\n' \
    -H 'X-Internal-Key: nope' http://localhost:8085/hazards

  for c in bnpb client-api notifier; do echo "== $c =="; \
    docker inspect -f '{{range .Mounts}}{{.Name}} -> {{.Destination}}{{"\n"}}{{end}}' \
    "$(docker compose ps -q $c)"; done

  docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
    "$(docker compose ps -q client-api)" | grep -E '^BNPB_'
  ```

### Problem 5 — Event-driven architecture

- **5a. Publikasi ke broker** — log BNPB memuat `outcome=acknowledged` lalu
  `outbox publish succeeded`; BNPB tidak memanggil consumer secara langsung.

  ```bash
  docker compose logs --tail=100 bnpb \
    | grep -E 'outcome=acknowledged|outbox publish succeeded'

  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --describe --topic hazard-events
  ```
- **5b. Dua consumer independen**

  ```bash
  docker compose logs --tail=50 dashboard-updater
  docker compose logs --tail=50 notifier
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group dashboard-updater-group
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notifier-group
  ```

- **5c. Hentikan consumer** — `notifier` dimatikan, producer tetap publish, dashboard
  tetap memproses, lag `notifier-group` bertambah. Setelah dinyalakan lagi, consumer
  menyusul (catch-up) dari offset terakhir, lag kembali `0`.

  ```bash
  docker compose stop notifier
  sleep 25

  # producer & consumer lain tetap jalan
  docker compose logs --since 25s bnpb | grep 'outcome=acknowledged' | tail -n 3
  docker compose logs --since 25s dashboard-updater | grep 'result=success' | tail -n 3

  # lag notifier bertambah
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --describe --group notifier-group

  # nyalakan lagi → catch-up, lag kembali 0
  docker compose start notifier
  sleep 12
  docker compose logs --since 15s notifier | grep 'result=success' | head -n 5
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --describe --group notifier-group
  ```

- **5d. Consumer baru** — group baru membaca seluruh event retensi (dari `earliest`)
  tanpa mengubah producer.

  ```bash
  docker compose --profile analytics up -d --build analytics-consumer
  sleep 15
  docker compose logs --since 20s analytics-consumer | grep 'result=success' | head -n 5
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --describe --group analytics-group
  ```

**Semantik pengiriman:** **at-least-once**. Kafka menyimpan event 7 hari dan consumer
commit offset setelah berhasil memproses.

---

## 9. Struktur repositori

### P3 ingest pipeline (local Compose)

`ingest-pipeline` owns durable raw-message quarantine and field-report intake. Its API/producer and `field-report-consumer` run separately but share the C-owned `ingest-data` volume, so the consumer can be stopped while intake remains available. The local broker is single-node with replication factor 1; use a replicated Kafka deployment to demonstrate node-loss durability.

Start from a clean local config by copying the updated root `.env.example` to `.env`, then run the normal `docker compose up --build -d`. For a live incompatible PVMBG schema, use one of `rename`, `type_change`, or `missing_required`:

```bash
curl -u admin:local-dev-admin-password -X POST http://localhost:8082/admin/schema-version \
  -H 'Content-Type: application/json' -d '{"mode":"rename"}'
curl -H 'Authorization: Bearer local-dev-audit-token' \
  'http://localhost:8094/internal/audit/unprocessed?source=PVMBG&limit=10'
```

The poller stores the original PVMBG bytes before advancing its cursor. Internal Ops can inspect the exact raw bytes and replay after the mapper is updated:

```bash
curl -H 'Authorization: Bearer local-dev-ops-token' \
  http://localhost:8094/internal/ops/messages/<message_id>
curl -X POST http://localhost:8094/internal/ops/reprocess \
  -H 'Authorization: Bearer local-dev-ops-token' -H 'Content-Type: application/json' \
  -d '{"message_id":"<message_id>"}'
```

To see automatic retrying versus a parked schema failure, stop BNPB, park a retryable PVMBG report, and query the Audit list. It remains `retrying` with its attempt count increasing while BNPB is unavailable; after BNPB returns, the retry worker replays it and it disappears from `/unprocessed`. Schema incompatibilities stay `parked` until an Ops replay after the mapper update.

```bash
docker compose stop bnpb
RAW_B64=$(printf '%s' '{"report_id":"retry-demo-001","volcano_id":"merapi","alert_level":"WASPADA","reported_at":"2026-11-15T00:00:00Z","eruption_count_24h":1}' | base64 | tr -d '\n')
curl -X POST http://localhost:8094/internal/dlq/park \
  -H 'Authorization: Bearer local-dev-pipeline-internal-token' \
  -H 'Content-Type: application/json' \
  -d "{\"source\":\"PVMBG\",\"source_ref_id\":\"retry-demo-001\",\"reason\":\"canonical_api_unavailable\",\"retryable\":true,\"raw_base64\":\"$RAW_B64\"}"
curl -H 'Authorization: Bearer local-dev-audit-token' \
  'http://localhost:8094/internal/audit/unprocessed?source=PVMBG&limit=10'
docker compose start bnpb
```

The local replay adapter currently writes via BNPB's M1 storage API using the `pvmbg-v2` mapper. See [`doc/m2-orang-a-b-handoff.md`](doc/m2-orang-a-b-handoff.md) for the migration to the M2 canonical write contract and production Secret/proxy requirements.

Field users send a report with an idempotency key. `202` is returned only after the local Kafka broker acknowledges the record:

```bash
TOKEN=$(curl -s -X POST http://localhost:9080/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"field-user","password":"field-pass-2026"}' \
  | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)
curl -i -X POST http://localhost:9080/field-reports \
  -H "Authorization: Bearer $TOKEN" -H 'Idempotency-Key: field-report-demo-001' \
  -H 'Content-Type: application/json' \
  -d '{"report_id":"field-report-demo-001","reported_at":"2026-11-15T00:00:00Z","description":"Flood water rising near the bridge"}'
```

To show acceptance is independent of downstream processing, stop `field-report-consumer`, submit reports (the API remains up and returns acknowledged 202), then restart it and inspect the consumer logs. Repeating the request with the same key and body returns the same receipt; a different body with the same key returns 409. `docker compose stop field-report-consumer` / `docker compose start field-report-consumer` controls the consumer independently.

To inject poison Kafka records for the malformed-message demonstration, publish directly to `field-reports` with the Kafka CLI using malformed envelope bytes. The consumer parks invalid envelopes before committing their offsets; ordinary field-report API requests reject malformed JSON before publishing.

### Repository layout

```
.
├── docker-compose.yml            # orkestrasi seluruh container
├── .env.example                  # template konfigurasi root
├── services/
│   ├── bmkg/                     # mock seismik/tsunami (gRPC)
│   ├── pvmbg/                    # mock vulkanik (REST)
│   ├── bnpb/                     # aggregator + canonical store + outbox + producer
    │   ├── ingest-pipeline/          # P3 DLQ, reprocessing, field-report intake/consumer
│   ├── auth/                     # token service (JWT HS256)
│   ├── client-api/               # client-facing API + scope filter + rate limit
│   ├── dashboard-updater/        # consumer #1
│   ├── notifier/                 # consumer #2
│   └── analytics-consumer/       # consumer tambahan (profile)
├── client/                       # CLI Java (lihat client/README.md)
└── tools/loadtest/               # load generator untuk Problem 2
```

---

## 10. Milestone 2

### Setup sebelum Problem 1

**Prasyarat:** Docker pada mesin build, `kubectl` + Kustomize, `k6`, `jq`,
`curl`, `python3`, dan tiga VM k3s. Semua perintah dijalankan dari root
repository. Untuk P3/P4 lokal, cukup Docker Compose.

```bash
export KUBECONFIG=/path/ke/kubeconfig-cluster-demo
kubectl get nodes                 # ketiga node harus Ready
```

Build semua image, lalu impor ke setiap node k3s:

```bash
TAG=m2 bash scripts/build-images.sh
docker pull apache/kafka:3.9.0
docker pull docker.io/prom/prometheus:v3.5.0
docker pull docker.io/grafana/grafana:12.1.0
docker pull docker.io/grafana/tempo:2.8.2
docker pull docker.io/grafana/loki:3.5.3
docker pull docker.io/grafana/alloy:v1.10.2
docker pull docker.io/otel/opentelemetry-collector-contrib:0.131.0

docker save -o /tmp/bnpb-images.tar \
  apache/kafka:3.9.0 \
  docker.io/prom/prometheus:v3.5.0 \
  docker.io/grafana/grafana:12.1.0 \
  docker.io/grafana/tempo:2.8.2 \
  docker.io/grafana/loki:3.5.3 \
  docker.io/grafana/alloy:v1.10.2 \
  docker.io/otel/opentelemetry-collector-contrib:0.131.0 \
  $(docker images --format '{{.Repository}}:{{.Tag}}' \
    | grep '^bnpb-gateway/.*:m2$')

for host in ubuntu@192.168.56.10 ubuntu@192.168.56.11 ubuntu@192.168.56.12; do
  scp /tmp/bnpb-images.tar "$host:/tmp/bnpb-images.tar"
  ssh "$host" 'sudo k3s ctr images import /tmp/bnpb-images.tar && rm -f /tmp/bnpb-images.tar'
done
```

Siapkan Secret asli di luar git:

```bash
cp deploy/secrets.example.yaml deploy/secrets.yaml
# isi INTERNAL_API_KEY, token PVMBG/BMKG, P4_FEED_TOKEN, dan password aplikasi
kubectl label node node-app app-eligible=true --overwrite
kubectl label node node-control node-store app-eligible- --overwrite
kubectl apply -k deploy
kubectl -n sistem-informasi-kebencanaan-bnpb apply -f deploy/secrets.yaml
kubectl -n sistem-informasi-kebencanaan-bnpb rollout restart \
  deployment/aggregator deployment/client-api deployment/auth \
  deployment/satellite-processor deployment/field-notifier
```

Jangan menjalankan `kubectl apply -k deploy` lagi setelah Secret asli dipasang,
karena `deploy/11-secrets.dev.yaml` akan menimpa token development. Untuk
mengganti token feed satelit tanpa menyimpannya di git:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb patch secret bnpb-secrets \
  --type merge -p '{"stringData":{"P4_FEED_TOKEN":"<token>"}}'
kubectl -n sistem-informasi-kebencanaan-bnpb rollout restart \
  deployment/satellite-processor
```

Untuk P5 dengan profil ujian, pasang overlay sensor dan Secret `pantai-key`
sesuai key yang diberikan:

```bash
kubectl apply -k deploy/sensor-pantai/overlays/exam
```

Verifikasi deployment:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb get pods -o wide
kubectl -n sistem-informasi-kebencanaan-bnpb get pvc
kubectl -n sistem-informasi-kebencanaan-bnpb get pdb
kubectl -n tester-agent get daemonset,pods
kubectl -n tester-agent exec daemonset/tester-agent -- /app list
```

Untuk P3/P4 lokal:

```bash
docker compose down -v
docker compose up --build -d
```

Port-forward yang tetap hidup selama scaling/rolling update:

```bash
bash scripts/port-forward-loop.sh client-api 9080 8080 >/tmp/pf-client.log 2>&1 &
bash scripts/port-forward-loop.sh aggregator 18085 8085 >/tmp/pf-aggregator.log 2>&1 &
bash scripts/port-forward-loop.sh field-notifier 18097 8080 >/tmp/pf-notifier.log 2>&1 &
```

### Observability

Stack observability sudah menjadi bagian dari `deploy/kustomization.yaml`, jadi
`kubectl apply -k deploy` juga memasang Prometheus, Grafana, Tempo, Loki, Alloy,
dan OTel Collector. Pod observability dijadwalkan ke node berlabel
`app-eligible=true`; pada cluster demo, pakai node aplikasi:

```bash
kubectl label node node-app app-eligible=true --overwrite
kubectl label node node-control node-store app-eligible- --overwrite

kubectl -n sistem-informasi-kebencanaan-bnpb create secret generic bnpb-grafana-auth \
  --from-literal=admin-password='admin' --dry-run=client -o yaml | kubectl apply -f -

# Observability saja; jangan apply -k deploy lagi setelah Secret asli dipasang.
kubectl apply --validate=false -k deploy/kubernetes/observability
kubectl -n sistem-informasi-kebencanaan-bnpb rollout status \
  deployment/prometheus deployment/grafana deployment/tempo deployment/loki \
  deployment/alloy deployment/otel-collector --timeout=300s
```

Akses UI:

```bash
bash scripts/port-forward-loop.sh grafana 3000 3000 >/tmp/pf-grafana.log 2>&1 &
bash scripts/port-forward-loop.sh prometheus 19090 9090 >/tmp/pf-prometheus.log 2>&1 &
# Grafana: http://127.0.0.1:3000  (admin / password dari Secret bnpb-grafana-auth)
# Prometheus: http://127.0.0.1:19090
```

Cek target Prometheus:

```bash
curl -s 'http://127.0.0.1:19090/api/v1/targets?state=active' \
  | jq -r '.data.activeTargets[] | [.labels.job,.health] | @tsv'
```

### Milestone 2 - Problem 1: Distributed canonical storage (Citus)

P1 memakai Citus murni tanpa Patroni. Coordinator dan tiga worker adalah
PostgreSQL primary biasa; ketahanan node hilang berasal dari **Citus shard
replication factor 2**, bukan streaming standby.

```text
citus-coordinator  node-control  group 0
citus-worker-1     node-app      group 1
citus-worker-2     node-store    group 2
citus-worker-3     node-app      group 3 (ditambahkan saat uji node addition)
```

48 shard logis, 2 salinan per shard. `canonical-api` menjadi adapter protokol
legacy `canonical-store` di atas PostgreSQL/Citus.

#### Setup storage

```bash
kubectl label node node-control node-app node-store \
  store-eligible=true app-eligible=true --overwrite

# Direktori data lokal (PV/hostPath, bukan storage jaringan).
for spec in \
  '192.168.56.10 canonical-a-primary' \
  '192.168.56.11 canonical-b-primary' \
  '192.168.56.11 canonical-a-replica' \
  '192.168.56.12 canonical-c-primary'; do
  set -- $spec
  ssh ubuntu@"$1" "sudo install -d -m 0700 -o 1000 -g 1000 /var/lib/bnpb/$2"
done

# canonical-*-data PVC harus sudah Bound (dibuat oleh storage.yaml).
kubectl -n sistem-informasi-kebencanaan-bnpb get pvc | grep canonical-

kubectl apply --validate=false -f deploy/kubernetes/base/citus-services.yaml
kubectl apply --validate=false -f deploy/kubernetes/base/citus-pods.yaml
kubectl apply --validate=false -f deploy/kubernetes/base/citus-worker-3.yaml
kubectl -n sistem-informasi-kebencanaan-bnpb get pods -l citus=true -o wide
```

Inisialisasi extension/role dan daftarkan node. Ganti IP dengan IP pod dari
perintah di atas:

```bash
COORD=10.42.0.197
WORKER1=10.42.2.14
WORKER2=10.42.1.19
WORKER3=10.42.2.17

for pod in citus-coordinator citus-worker-1 citus-worker-2 citus-worker-3; do
  kubectl -n sistem-informasi-kebencanaan-bnpb exec "$pod" -- \
    psql -h 127.0.0.1 -U postgres -d canonical -v ON_ERROR_STOP=1 \
    -c 'CREATE EXTENSION IF NOT EXISTS citus;'
done

kubectl -n sistem-informasi-kebencanaan-bnpb exec citus-coordinator -- \
  psql -h 127.0.0.1 -U postgres -d canonical -v ON_ERROR_STOP=1 <<SQL
SELECT citus_set_coordinator_host('$COORD',5432);
SELECT citus_add_node('$WORKER1',5432);
SELECT citus_add_node('$WORKER2',5432);
SELECT citus_add_node('$WORKER3',5432);
SELECT citus_set_node_property('$COORD',5432,'shouldhaveshards',true);
SQL

# Role aplikasi (password dari Secret canonical).
kubectl -n sistem-informasi-kebencanaan-bnpb exec citus-coordinator -- \
  psql -h 127.0.0.1 -U postgres -d canonical -v ON_ERROR_STOP=1 < \
  services/bnpb/internal/canonical/schema.sql
```

Schema memakai `citus.shard_replication_factor=2`. Health function
`canonical.replication_ready()` mensyaratkan tiga node aktif dan dua salinan
per shard; `canonical.primaries_ready()` dipakai saat policy
`recovery.durability='single_copy'` setelah satu node hilang.

#### Adapter dan Aggregator

```bash
# Build static adapter (Go 1.26 auto-toolchain).
cd services/bnpb
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOTOOLCHAIN=auto \
  go build -trimpath -o /tmp/canonical-api ./cmd/canonical-api
cd ../..
ssh ubuntu@192.168.56.10 \
  'sudo install -d -m 0755 -o 1000 -g 1000 /var/lib/bnpb/canonical-api'
scp /tmp/canonical-api ubuntu@192.168.56.10:/tmp/canonical-api
ssh ubuntu@192.168.56.10 \
  'sudo mv /tmp/canonical-api /var/lib/bnpb/canonical-api/canonical-api &&
   sudo chmod 0755 /var/lib/bnpb/canonical-api/canonical-api'

kubectl apply --validate=false -f deploy/kubernetes/base/canonical-api.yaml
kubectl -n sistem-informasi-kebencanaan-bnpb patch cm bnpb-config \
  --type merge -p '{"data":{"STORAGE_URL":"http://canonical-api:8080"}}'
kubectl -n sistem-informasi-kebencanaan-bnpb scale deploy aggregator --replicas=3
```

Jika controller Kubernetes sedang sehat, gunakan Deployment/Service normal.
Pod Citus dan adapter dibuat sebagai Pod langsung dengan `nodeName` agar
storage tetap bisa dipulihkan saat controller-manager sedang recovering.

#### Uji P1

```bash
# 1. Sharding dan replikasi.
kubectl -n sistem-informasi-kebencanaan-bnpb exec citus-coordinator -- \
  psql -h 127.0.0.1 -U postgres -d canonical -At <<'SQL'
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='canonical.events'::regclass;
SELECT count(*) FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
 WHERE n.isactive AND p.shardstate=1;
SELECT canonical.replication_ready(), canonical.primaries_ready();
SQL

# 2. Pagination lintas shard (adapter).
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward pod/canonical-api 18080:8080
curl -s 'http://127.0.0.1:18080/v1/events?limit=5' | jq '{items:(.items|length),next_cursor,partial}'
curl -s 'http://127.0.0.1:18080/v1/events?limit=5&cursor=<next_cursor>' | jq '{items:(.items|length),partial}'

# 3. Node loss: hilangkan satu worker, policy single_copy, tulis tetap jalan.
kubectl -n sistem-informasi-kebencanaan-bnpb delete pod citus-worker-2 --force --grace-period=0
kubectl -n sistem-informasi-kebencanaan-bnpb exec citus-coordinator -- \
  psql -h 127.0.0.1 -U postgres -d canonical -c \
  "SELECT citus_disable_node('$WORKER2',5432,synchronous:=true);
   UPDATE canonical.recovery SET durability='single_copy', fenced_node='$WORKER2'
   WHERE singleton;"
# PUT ke /internal/canonical/events/... harus 201 dengan durability single_copy.

# 4. Node addition.
# Buat citus-worker-3 (PVC canonical-a-replica-data), lalu:
kubectl -n sistem-informasi-kebencanaan-bnpb exec citus-coordinator -- \
  psql -h 127.0.0.1 -U postgres -d canonical -c \
  "SELECT citus_add_node('$WORKER3',5432);
   SELECT citus_rebalance_start(NULL,false,'block_writes'::citus.shard_transfer_mode);"
```

#### Hasil uji P1 (cluster demo)

| Kriteria | Bukti |
|---|---|
| Multi-node storage | 1 coordinator + 3 worker Citus, semua `1/1 Ready` |
| Sharding | 48 shard `canonical.events`; 192 placement aktif |
| Distribusi | 22–25 shard per worker setelah rebalance |
| Cross-shard pagination | `/v1/events?limit=5` → 5 item + cursor; halaman 2 → 5 item; `partial=false` |
| Node loss | worker-2 dimatikan; GET 13 event `partial=false`; write `201 durability=single_copy`; worker kembali dan metadata diaktifkan |
| Node addition | worker-3 ditambahkan; 4 node; 192 placement tetap RF2; `replication_ready=t` |
| Analytics dashboard | Grafana datasource `Citus P1` + dashboard `/d/p1-citus/p1-citus-analytics` (event per source) |
| Adapter | Aggregator 3/3 Ready dengan `STORAGE_URL=http://canonical-api:8080` |

Catatan: setelah uji node-loss, tabel `events/outbox` di-reset dan schema
dijalankan ulang agar metadata RF2 kembali bersih; data uji bersifat disposable.

### Milestone 2 - Problem 1 (legacy): Interoperabilitas & evolusi skema

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/pvmbg 18082:8082 &
PVMBG_ADMIN_USERNAME="$(kubectl -n sistem-informasi-kebencanaan-bnpb get secret bnpb-secrets -o jsonpath='{.data.PVMBG_ADMIN_USERNAME}' | base64 -d)"
PVMBG_ADMIN_PASSWORD="$(kubectl -n sistem-informasi-kebencanaan-bnpb get secret bnpb-secrets -o jsonpath='{.data.PVMBG_ADMIN_PASSWORD}' | base64 -d)"
PVMBG_TOKEN="$(kubectl -n sistem-informasi-kebencanaan-bnpb get secret bnpb-secrets -o jsonpath='{.data.PVMBG_TOKEN}' | base64 -d)"

curl -s -u "$PVMBG_ADMIN_USERNAME:$PVMBG_ADMIN_PASSWORD" \
  -X POST http://127.0.0.1:18082/admin/schema-version \
  -H 'Content-Type: application/json' -d '{"enabled":true}' | jq .
curl -s -H "Authorization: Bearer $PVMBG_TOKEN" \
  'http://127.0.0.1:18082/volcanic-reports' \
  | jq '.reports[0] | {report_id,confidence_level}'
```

Ulangi dengan `{"enabled":false}` lalu `{"enabled":true}`. Field harus hilang
dan muncul kembali tanpa restart. Detail ada di Bagian 8.

### Milestone 2 - Problem 2: Ketahanan Aggregator

Problem 2 memakai kontrak kanonik M2 pada `contracts/m2/README.md` dan berbagi
ingest pipeline dengan Problem 3/4.

```bash
export BASE_URL=http://127.0.0.1:9080
export AGG_URL=http://127.0.0.1:18085
export INTERNAL_KEY="$(kubectl -n sistem-informasi-kebencanaan-bnpb get secret bnpb-secrets -o jsonpath='{.data.INTERNAL_API_KEY}' | base64 -d)"
export SKIP_TTL_ENV=1

./loadtest/duplicate-detector.sh
./scripts/integration-pause-leader.sh
./loadtest/scaling.sh
./loadtest/throughput.sh
./loadtest/rolling-update.sh
```

Kriteria 5 (freeze VM) memakai `VBoxManage controlvm <vm> savestate` lalu
`startvm` dan `./loadtest/duplicate-detector.sh`. Detail ada di Bagian 10.

Komponen:

- `services/coordinator` — lease per sumber, epoch monotonik, CAS checkpoint, dan outbox peringatan kritis.
- `services/canonical-store` — penyimpanan kanonik sementara sampai A menyediakan PostgreSQL/Citus. Mendukung upsert/retract berversi, fencing epoch otoritas, tombstone, audit checksum/location, dan query cursor.
- `services/bnpb` — poller BMKG/PVMBG/Sensor dengan leader election, readiness, graceful drain, audit proxy, dan kepemilikan aktif-pasif per sumber.
- `services/client-api` — titik masuk baca stateless yang dipakai load test.

### Menjalankan Problem 2

Gunakan setup di atas. Untuk stack lokal jalankan `cp .env.example .env` lalu
`make dev-up`. Untuk cluster jalankan `kubectl apply -k deploy` setelah image dan
Secret disiapkan.

### Pemeriksaan kriteria P2

Semua perintah dijalankan di cluster demo dari root repository. Prasyarat:
`kubectl`, `k6`, `jq`, `python3`, serta shell dengan `seq`/`awk`. Biarkan
port-forward berikut tetap berjalan:

```bash
# Raise the token TTL before starting the port-forwards. scaling/rolling scripts
# must not roll client-api/auth while a one-shot port-forward is in use.
kubectl -n sistem-informasi-kebencanaan-bnpb set env deployment/client-api TOKEN_TTL_SECONDS=3600
kubectl -n sistem-informasi-kebencanaan-bnpb set env deployment/auth ACCESS_TOKEN_TTL_SECONDS=3600
kubectl -n sistem-informasi-kebencanaan-bnpb rollout status deployment/client-api --timeout=120s
kubectl -n sistem-informasi-kebencanaan-bnpb rollout status deployment/auth --timeout=120s

# These loops survive pod restarts during scaling and rolling updates.
bash scripts/port-forward-loop.sh client-api 9080 8080 >/tmp/pf-client.log 2>&1 &
bash scripts/port-forward-loop.sh aggregator 18085 8085 >/tmp/pf-aggregator.log 2>&1 &
export BASE_URL=http://127.0.0.1:9080
export AGG_URL=http://127.0.0.1:18085
export INTERNAL_KEY='<INTERNAL_API_KEY dari bnpb-secrets>'
export SKIP_TTL_ENV=1
```

**Kriteria 1 — satu titik masuk, readiness nyata, penyebaran, PDB**

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb get deploy aggregator -o wide
kubectl -n sistem-informasi-kebencanaan-bnpb get pods -l app=aggregator -o wide
kubectl -n sistem-informasi-kebencanaan-bnpb get endpoints aggregator
kubectl -n sistem-informasi-kebencanaan-bnpb get pdb aggregator
kubectl -n sistem-informasi-kebencanaan-bnpb describe deploy aggregator \
  | grep -E 'Replicas|maxUnavailable|maxSurge|topologySpread|Readiness|preStop'
```

Expected: 3/3 ready, endpoint berisi 3 alamat pada minimal dua node, PDB
`minAvailable: 2`, dan `maxUnavailable: 0`.

**Kriteria 2 dan 6 — tepat sekali saat beban tinggi dan scaling 3→5→2**

```bash
./loadtest/scaling.sh
jq '.metrics.error_rate_excl_controlled' loadtest/results/scaling-summary.json
./loadtest/duplicate-detector.sh
```

Expected: error rate di bawah `0.01` di luar penolakan terkontrol, dan detector
menghasilkan `RESULT: PASS` (tidak ada baris kanonik ganda, tidak ada dua
instance pemanggil untuk sumber yang sama dalam satu detik).

**Kriteria 3 — instance pemimpin mati**

```bash
./scripts/integration-pause-leader.sh
```

Script ini menghapus pod pemimpin, menunggu pemimpin baru melalui metrics, lalu
menjalankan duplicate detector. Expected: takeover dalam 60 detik (TTL 15 detik)
dan detector tetap `RESULT: PASS`. Tester Agent `kill-leader` adalah alternatif:

```bash
kubectl -n tester-agent exec daemonset/tester-agent -- /app run practice kill-leader
```

**Kriteria 5 — holder freeze pada VM**

Ini tidak dapat diuji secara jujur di kind karena node berbagi kernel dan tidak
bisa disuspend sebagai VM. Jalankan pada cluster VM. Contoh VirtualBox:

```bash
VBoxManage controlvm <nama-vm-worker> savestate
# tunggu lebih lama dari LEASE_TTL (15s), lalu:
VBoxManage startvm <nama-vm-worker> --type headless
./loadtest/duplicate-detector.sh
kubectl -n sistem-informasi-kebencanaan-bnpb logs deploy/aggregator --tail=200 \
  | grep -E 'fenced|stale_epoch|lease_renew_failed|self_fenc'
```

Expected: instance yang bangun tidak menyimpan hasil sampai statusnya diperbarui;
epoch basi ditolak oleh canonical store.

**Kriteria 4 — pergeseran jam node**

Jalankan pada cluster VM (bukan kind), karena kind memakai clock host bersama:

```bash
kubectl -n tester-agent exec daemonset/tester-agent -- /app list
kubectl -n tester-agent exec daemonset/tester-agent -- /app run practice clock-skew
./loadtest/duplicate-detector.sh
kubectl -n sistem-informasi-kebencanaan-bnpb logs deploy/aggregator --tail=200 \
  | grep -E 'fenced|stale_epoch|lease_renew_failed'
```

Expected: tidak ada dua instance yang sama-sama berhasil menyimpan pada periode
polling yang sama; holder dengan epoch basi ditolak oleh canonical store.

**Kriteria 7 — 3 instance ≥ 2× throughput 1 instance**

```bash
./loadtest/throughput.sh
```

Expected: output berakhir dengan `ratio ... (target >= 2.0)` dan exit code 0.

**Kriteria 8 — rolling update tanpa permintaan gagal**

```bash
./loadtest/rolling-update.sh
jq '.metrics.unexpected_errors' loadtest/results/rolling-summary.json
```

Expected: `unexpected_errors=0`; penolakan terkontrol `429` tidak dihitung.

**Audit proxy**

```bash
export AUDIT_TOKEN='<AUDIT_TOKEN dari bnpb-secrets>'
for path in events checksum location unprocessed; do
  curl -s -H "Authorization: Bearer $AUDIT_TOKEN" \
    "http://127.0.0.1:18085/internal/audit/$path?limit=5" | jq .
done
```

Expected: respons JSON berisi `items`/`checksum`/`locations`, tanpa `500` polos
atau request yang menggantung.



---

## 11. Milestone 2 - Problem 3: ingest pipeline tests

Bagian ini menambahkan validasi M2 terpisah dari panduan M1 di atas. Jalankan dari
root repository setelah `docker compose up --build -d` dan seluruh health check
berhasil. Script memakai `curl`, `jq`, dan Docker Compose CLI. Nilai credential di
bawah adalah nilai lokal dari `.env.example`; sesuaikan jika `.env` memakai nilai
lain. Gunakan stack lokal/dev karena script menghentikan dan menyalakan kembali
`bnpb` serta `field-report-consumer`.

Prasyarat tambahan untuk mengirim byte UTF-8 cacat ke Kafka secara persis: Go 1.26+
(script membangun helper sementara dengan franz-go, lalu menjalankannya di container
broker; helper tidak disimpan ke repository).

Kontrak kompatibilitas yang dinyatakan untuk mapper PVMBG:

| Perubahan PVMBG | Hasil otomatis | Alasan |
| --- | --- | --- |
| Menambah field opsional, misalnya `confidence_level` | Diterima; nilai ekstra dipertahankan di `HazardEvent.attributes` | Mapper kanonik menyimpan field yang tidak memiliki kolom kanonik. |
| Mengganti nama `alert_level` menjadi `alert_status` | Tidak diterima otomatis; dikarantina sebagai `schema_incompatible` | Field kanonik wajib hilang. Setelah kompatibilitas `pvmbg-v2` tersedia, Ops dapat replay payload asli. |
| Mengubah tipe `alert_level` dari string menjadi object | Tidak diterima otomatis; dikarantina | Tipe tidak cocok dengan kontrak mapper. Replay v2 memahami bentuk object terdokumentasi. |
| Menghilangkan `report_id` wajib | Tidak diterima otomatis; dikarantina | Identitas source tidak dapat dipetakan otomatis. Replay v2 membuat ID stabil dari raw payload. |

Pengujian di bawah menegaskan bahwa kegagalan sementara (`retryable=true`) tetap
berstatus `retrying` dan dicoba ulang, sedangkan kegagalan skema (`retryable=false`)
tetap `parked` sampai ada perubahan mapper dan replay Ops.

Script memeriksa penerimaan field report dan idempotency, akses Ops/Audit, penyimpanan
raw bytes yang tidak valid UTF-8, retry serta pemulihan DLQ. Ia juga mengubah mode
schema PVMBG untuk memicu karantina kompatibilitas, lalu menjalankan replay oleh Ops.
Untuk uji poison, `curl` menjalankan pemeriksaan API, sementara helper Go kecil
mengirim byte Kafka persis (termasuk UTF-8 tidak valid) karena HTTP tidak menulis
langsung ke topic Kafka.

Simpan isi berikut sebagai `tools/p3-curl-test.sh`, lalu jalankan dengan
`bash tools/p3-curl-test.sh`:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail

CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:9080}"
INGEST_URL="${INGEST_URL:-http://localhost:8094}"
PVMBG_URL="${PVMBG_URL:-http://localhost:8082}"
BNPB_URL="${BNPB_URL:-http://localhost:8085}"
AUDIT_TOKEN="${AUDIT_TOKEN:-local-dev-audit-token}"
OPS_TOKEN="${OPS_TOKEN:-local-dev-ops-token}"
PIPELINE_INTERNAL_TOKEN="${PIPELINE_INTERNAL_TOKEN:-local-dev-pipeline-internal-token}"
FIELD_REPORT_TOKEN="${FIELD_REPORT_TOKEN:-local-dev-field-report-token}"
PVMBG_ADMIN_USER="${PVMBG_ADMIN_USER:-admin}"
PVMBG_ADMIN_PASSWORD="${PVMBG_ADMIN_PASSWORD:-local-dev-admin-password}"
FIELD_USER_PASSWORD="${FIELD_USER_PASSWORD:-field-pass-2026}"
MEDIA_USER_PASSWORD="${MEDIA_USER_PASSWORD:-media-pass-2026}"

tmp="$(mktemp -d)"
restore_bnpb=0
restore_consumer=0
cleanup() {
  if (( restore_bnpb )); then docker compose start bnpb >/dev/null || true; fi
  if (( restore_consumer )); then docker compose start field-report-consumer >/dev/null || true; fi
  curl -fsS --max-time 5 -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
    -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
    -d '{"mode":"baseline"}' >/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }
wait_for() {
  local description="$1"; shift
  for _ in $(seq 1 40); do
    if "$@"; then return 0; fi
    sleep 2
  done
  fail "timed out waiting for $description"
}

# Wrapper saves response body and asserts the expected HTTP status.
curl_status() {
  local expected="$1" output="$2"; shift 2
  local metrics actual elapsed
  metrics="$(curl -sS -o "$output" -w '%{http_code} %{time_total}' "$@")" || fail "curl failed: $*"
  actual="${metrics%% *}"
  elapsed="${metrics#* }"
  [[ "$actual" == "$expected" ]] || {
    cat "$output" >&2
    fail "expected HTTP $expected, received $actual ($*)"
  }
  if [[ "$expected" == 202 ]] && ! awk -v t="$elapsed" 'BEGIN { exit !(t <= 2.0) }'; then
    fail "202 response took ${elapsed}s; expected <= 2s"
  fi
}

echo 'P3.1 — health and access control'
curl_status 200 "$tmp/health" "$INGEST_URL/health"
curl_status 401 "$tmp/no-auth" "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1"
curl_status 401 "$tmp/audit-raw" -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/ops/messages/not-a-real-message"
curl_status 401 "$tmp/internal-raw" -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" \
  "$INGEST_URL/internal/ops/messages/not-a-real-message"
curl_status 401 "$tmp/field-raw" -H "Authorization: Bearer $FIELD_REPORT_TOKEN" \
  "$INGEST_URL/internal/ops/messages/not-a-real-message"
curl_status 200 "$tmp/audit" -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=10"
if jq -e '.items | all(has("raw_base64") | not)' "$tmp/audit" >/dev/null; then
  pass 'Audit can list metadata, and does not receive raw payloads'
else
  fail 'Audit listing unexpectedly contains raw payload data'
fi

echo 'P3.2 — lossless quarantine for arbitrary raw bytes'
bad_utf8_b64="//4=" # bytes FF FE: not valid UTF-8
bad_utf8_id="p3-raw-$RANDOM"
curl_status 201 "$tmp/park" -X POST "$INGEST_URL/internal/dlq/park" \
  -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$bad_utf8_id\",\"source\":\"FIELD_REPORT\",\"reason\":\"invalid_utf8_demo\",\"retryable\":false,\"raw_base64\":\"$bad_utf8_b64\"}"
raw_id="$(jq -r .message_id "$tmp/park")"
curl_status 200 "$tmp/audit-raw-metadata" -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000"
jq -e --arg id "$raw_id" '.items[] | select(.message_id == $id and .source == "FIELD_REPORT" and .reason == "invalid_utf8_demo" and .received_at != "" and .retryable == false and .status == "parked")' \
  "$tmp/audit-raw-metadata" >/dev/null || fail 'Audit metadata is missing the raw failure source, reason, time, or retry class'
curl_status 200 "$tmp/raw" -H "Authorization: Bearer $OPS_TOKEN" \
  "$INGEST_URL/internal/ops/messages/$raw_id"
raw_returned_b64="$(jq -r .raw_base64 "$tmp/raw")"
[[ "$raw_returned_b64" == "$bad_utf8_b64" ]] \
  || fail 'raw bytes changed while being quarantined'
pass 'Ops retrieves the exact original invalid UTF-8 byte sequence'

echo 'P3.3 — retry unavailable canonical API, then recover'
docker compose stop bnpb
restore_bnpb=1
retry_ref="p3-retry-$RANDOM"
retry_json="{\"report_id\":\"$retry_ref\",\"volcano_id\":\"merapi\",\"alert_level\":\"WASPADA\",\"reported_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"eruption_count_24h\":1}"
retry_b64="$(printf '%s' "$retry_json" | base64 | tr -d '\n')"
curl_status 201 "$tmp/retry-park" -X POST "$INGEST_URL/internal/dlq/park" \
  -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"source\":\"PVMBG\",\"source_ref_id\":\"$retry_ref\",\"reason\":\"canonical_api_unavailable\",\"retryable\":true,\"raw_base64\":\"$retry_b64\"}"
retry_id="$(jq -r .message_id "$tmp/retry-park")"
retry_attempts() {
  curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
    "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1000" >"$tmp/retrying"
  jq -r --arg id "$retry_id" '[.items[] | select(.message_id == $id and .status == "retrying" and .retryable == true)] | first | .attempts // 0' "$tmp/retrying"
}
for _ in $(seq 1 20); do
  attempts="$(retry_attempts)"
  [[ "$attempts" -ge 2 ]] && break
  sleep 1
done
[[ "${attempts:-0}" -ge 2 ]] || fail 'retryable message did not remain retrying with attempts increasing'
curl_status 200 "$tmp/retry-raw" -H "Authorization: Bearer $OPS_TOKEN" \
  "$INGEST_URL/internal/ops/messages/$retry_id"
jq -e '.source == "PVMBG" and .retryable == true and .reason == "canonical_api_unavailable" and .received_at != "" and .attempts >= 2 and .raw_base64 != ""' \
  "$tmp/retry-raw" >/dev/null || fail 'retryable raw message metadata is incomplete'
docker compose start bnpb
restore_bnpb=0
is_recovered() {
  curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
    "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1000" \
    | jq -e --arg id "$retry_id" 'all(.items[]; .message_id != $id)' >/dev/null
}
wait_for 'retry recovery after BNPB restart' is_recovered
pass 'retrying record was replayed and removed from the unprocessed list'

echo 'P3.4 — PVMBG schema drift, raw inspection, and replay'
canonical_baseline_ready() {
  curl -fsS -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
    "$BNPB_URL/hazards?source=PVMBG&limit=1000" \
    | jq -e 'length >= 20' >/dev/null
}
wait_for 'PVMBG baseline seed reports to be consumed before the schema test' canonical_baseline_ready
# An additive field is accepted and retained in canonical attributes.
curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
  -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
  -d '{"mode":"additive"}'
additive_seen() {
  curl -fsS -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
    "$BNPB_URL/hazards?source=PVMBG&limit=100" \
    | jq -e 'any(.[]; .attributes.confidence_level != null)' >/dev/null
}
wait_for 'additive PVMBG confidence_level to reach canonical storage' additive_seen
pass 'additive PVMBG attribute is retained in canonical HazardEvent.attributes'
curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
  -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
  -d '{"mode":"baseline"}'

for mode in rename type_change missing_required; do
  curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
    "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1000" \
    | jq '[.items[].message_id]' >"$tmp/schema-before"
  curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
    -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
    -d "{\"mode\":\"$mode\"}"
  found=''
  for _ in $(seq 1 35); do
    curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
      "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1000" >"$tmp/items"
    found="$(jq -r --slurpfile before "$tmp/schema-before" \
      '[.items[] | select(.reason | startswith("schema_incompatible:"))
       | select(.message_id as $id | ($before[0] | index($id)) == null)]
       | last | .message_id // empty' "$tmp/items")"
    if [[ -n "$found" ]]; then break; fi
    sleep 2
  done
  [[ -n "$found" ]] || fail "PVMBG $mode did not create a schema incompatibility quarantine"
  # Stop the incompatible mock mode at the first newly quarantined report so
  # the following source records do not accumulate extra failed entries.
  curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
    -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
    -d '{"mode":"baseline"}'
  # Ops-only route returns raw_base64; replay uses the same message_id.
  curl_status 200 "$tmp/detail" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$found"
  parked_attempts="$(jq -r .attempts "$tmp/detail")"
  sleep 3
  curl_status 200 "$tmp/detail-after-wait" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$found"
  jq -e --argjson attempts "$parked_attempts" '.status == "parked" and .retryable == false and .attempts == $attempts' \
    "$tmp/detail-after-wait" >/dev/null \
    || fail "PVMBG $mode permanent schema failure was retried automatically"
  cp "$tmp/detail-after-wait" "$tmp/detail"
  jq -e '.raw_base64 | type == "string" and length > 0' "$tmp/detail" >/dev/null \
    || fail "Ops raw inspection returned no original PVMBG payload for $mode"
  jq -e '.source == "PVMBG" and .retryable == false and .status == "parked" and .received_at != "" and (.reason | startswith("schema_incompatible:"))' "$tmp/detail" >/dev/null \
    || fail "PVMBG $mode failure was not durably classified as a permanent schema incompatibility"
  curl_status 200 "$tmp/replay" -X POST "$INGEST_URL/internal/ops/reprocess" \
    -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
    -d "{\"message_id\":\"$found\"}"
  jq -e '.status == "recovered"' "$tmp/replay" >/dev/null \
    || fail "replay did not recover PVMBG $mode record"
  curl_status 200 "$tmp/detail-recovered" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$found"
  jq -e '.status == "recovered"' "$tmp/detail-recovered" >/dev/null \
    || fail "PVMBG $mode message is not durably marked recovered"
  report_raw="$(jq -r .raw_base64 "$tmp/detail-recovered")"
  canonical_ref="$(printf '%s' "$report_raw" | base64 -d | jq -r '.report_id // empty')"
  if [[ -z "$canonical_ref" ]]; then
    canonical_ref="pvmbg-v2-$(printf '%s' "$report_raw" | base64 -d | sha256sum | awk '{print $1}')"
  fi
  curl_status 200 "$tmp/replayed-hazards" -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
    "$BNPB_URL/hazards?source=PVMBG&limit=1000"
  jq -e --arg id "$canonical_ref" '[.[] | select(.source_ref_id == $id)] | length == 1' \
    "$tmp/replayed-hazards" >/dev/null \
    || fail "PVMBG $mode replay did not produce exactly one canonical event"
  pass "$mode incompatibility was quarantined, inspectable by Ops, and replayed"
  sleep 11 # wait for a fresh PVMBG report before testing the next incompatible mode
done
curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
  -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
  -d '{"mode":"baseline"}'

# Two permanently failed messages for one source_ref_id cannot be replayed out
# of order. Once both are repaired, canonical storage still has one source row.
echo 'P3.4b — per-source_ref_id replay order and deduplication'
order_ref="p3-order-$RANDOM"
order_early="p3-order-early-$RANDOM"
order_late="p3-order-late-$RANDOM"
order_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
raw_early="{\"report_id\":\"$order_ref\",\"volcano_id\":\"merapi\",\"alert_level\":\"WASPADA\",\"reported_at\":\"$order_time\",\"confidence_level\":0.91}"
raw_late="{\"report_id\":\"$order_ref\",\"volcano_id\":\"merapi\",\"alert_level\":\"SIAGA\",\"reported_at\":\"$order_time\",\"confidence_level\":0.95}"
early_b64="$(printf '%s' "$raw_early" | base64 | tr -d '\n')"
late_b64="$(printf '%s' "$raw_late" | base64 | tr -d '\n')"
curl_status 201 "$tmp/order-early-park" -X POST "$INGEST_URL/internal/dlq/park" \
  -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_early\",\"source\":\"PVMBG\",\"source_ref_id\":\"$order_ref\",\"received_at\":\"2026-10-01T00:00:01Z\",\"reason\":\"schema_incompatible:test\",\"retryable\":false,\"raw_base64\":\"$early_b64\"}"
curl_status 201 "$tmp/order-late-park" -X POST "$INGEST_URL/internal/dlq/park" \
  -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_late\",\"source\":\"PVMBG\",\"source_ref_id\":\"$order_ref\",\"received_at\":\"2026-10-01T00:00:02Z\",\"reason\":\"schema_incompatible:test\",\"retryable\":false,\"raw_base64\":\"$late_b64\"}"
curl_status 409 "$tmp/order-out-of-order" -X POST "$INGEST_URL/internal/ops/reprocess" \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_late\"}"
curl_status 200 "$tmp/order-first" -X POST "$INGEST_URL/internal/ops/reprocess" \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_early\"}"
curl_status 200 "$tmp/order-second" -X POST "$INGEST_URL/internal/ops/reprocess" \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_late\"}"
curl_status 200 "$tmp/order-hazards" -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
  "$BNPB_URL/hazards?source=PVMBG&limit=1000"
jq -e --arg id "$order_ref" '[.[] | select(.source_ref_id == $id)] | length == 1' "$tmp/order-hazards" >/dev/null \
  || fail 'ordered replay created a duplicate canonical row for the same source_ref_id'
for order_id in "$order_early" "$order_late"; do
  curl_status 200 "$tmp/order-recovered" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$order_id"
  jq -e '.status == "recovered"' "$tmp/order-recovered" >/dev/null \
    || fail "ordered message $order_id was not marked recovered"
done
curl_status 200 "$tmp/order-replay-again" -X POST "$INGEST_URL/internal/ops/reprocess" \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_early\"}"
jq -e '.status == "recovered" and .replayed == true' "$tmp/order-replay-again" >/dev/null \
  || fail 'replaying an already recovered message was not idempotent'
curl_status 200 "$tmp/order-hazards-again" -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
  "$BNPB_URL/hazards?source=PVMBG&limit=1000"
jq -e --arg id "$order_ref" '[.[] | select(.source_ref_id == $id)] | length == 1' "$tmp/order-hazards-again" >/dev/null \
  || fail 'replaying an already recovered message created a duplicate canonical row'
pass 'out-of-order replay is rejected; ordered replay recovers both messages into one canonical source row'

echo 'P3.5 — field report scope, idempotency, and poison-record recovery'
login() {
  local username="$1" password="$2" output="$3"
  curl_status 200 "$output" -X POST "$CLIENT_API_URL/auth/login" \
    -H 'Content-Type: application/json' -d "{\"username\":\"$username\",\"password\":\"$password\"}"
}
login field-user "$FIELD_USER_PASSWORD" "$tmp/field-login"
field_token="$(jq -r '.data.accessToken // .accessToken // empty' "$tmp/field-login")"
[[ -n "$field_token" ]] || fail 'field-user login response did not include accessToken'
login media-user "$MEDIA_USER_PASSWORD" "$tmp/media-login"
media_token="$(jq -r '.data.accessToken // .accessToken // empty' "$tmp/media-login")"
[[ -n "$media_token" ]] || fail 'media-user login response did not include accessToken'

curl_status 403 "$tmp/media-write" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $media_token" -H 'Idempotency-Key: p3-media-forbidden' \
  -H 'Content-Type: application/json' -d '{"description":"must be forbidden"}'
pass 'media user cannot write a field report'

docker compose stop field-report-consumer
restore_consumer=1
key="p3-field-$RANDOM"
report='{"report_id":"p3-field-report","reported_at":"2026-11-15T00:00:00Z","description":"P3 end-to-end test report"}'
curl_status 202 "$tmp/accepted-1" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' -d "$report"
jq -e '.status == "accepted" and .durability == "broker-acknowledged" and .receipt_id != ""' "$tmp/accepted-1" >/dev/null \
  || fail '202 response did not contain the accepted receipt and broker acknowledgement'
curl_status 202 "$tmp/accepted-2" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' -d "$report"
receipt1="$(jq -r .receipt_id "$tmp/accepted-1")"
receipt2="$(jq -r .receipt_id "$tmp/accepted-2")"
[[ "$receipt1" == "$receipt2" ]] || fail 'same idempotency key/body returned different receipts'
curl_status 409 "$tmp/conflict" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' -d '{"description":"different content"}'

# More acknowledgements while the consumer is stopped must remain in Kafka.
backlog_receipts=()
for n in 1 2 3; do
  backlog_key="p3-backlog-$RANDOM-$n"
  backlog_report="{\"report_id\":\"$backlog_key\",\"reported_at\":\"2026-11-15T00:00:00Z\",\"description\":\"durability backlog $n\"}"
  curl_status 202 "$tmp/backlog-$n" -X POST "$CLIENT_API_URL/field-reports" \
    -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $backlog_key" \
    -H 'Content-Type: application/json' -d "$backlog_report"
  backlog_receipts+=("$(jq -r .receipt_id "$tmp/backlog-$n")")
done

docker compose start field-report-consumer
restore_consumer=0
processed_once() {
  local receipt success
  for receipt in "$receipt1" "${backlog_receipts[@]}"; do
    success="$(docker compose logs --no-color field-report-consumer 2>/dev/null \
      | grep -F "receipt_id=$receipt" | grep -c 'result=success' || true)"
    [[ "$success" == 1 ]] || return 1
  done
  return 0
}
wait_for 'all 202-acknowledged field reports to be processed once' processed_once

# Kill immediately after a fresh 202 acknowledgement, then restart. Kafka retains
# the record; the consumer's SQLite idempotency key prevents a second side effect.
crash_key="p3-crash-$RANDOM"
crash_report="{\"report_id\":\"$crash_key\",\"reported_at\":\"2026-11-15T00:00:00Z\",\"description\":\"consumer crash window test\"}"
curl_status 202 "$tmp/crash-accepted" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $crash_key" \
  -H 'Content-Type: application/json' -d "$crash_report"
crash_receipt="$(jq -r .receipt_id "$tmp/crash-accepted")"
restore_consumer=1
docker compose kill -s SIGKILL field-report-consumer
docker compose start field-report-consumer
restore_consumer=0
crash_processed_once() {
  local success
  success="$(docker compose logs --no-color field-report-consumer 2>/dev/null \
    | grep -F "receipt_id=$crash_receipt" | grep -c 'result=success' || true)"
  [[ "$success" == 1 ]]
}
wait_for 'report acknowledged before consumer SIGKILL to be processed once' crash_processed_once

curl_status 400 "$tmp/invalid-json" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: p3-invalid-json" \
  -H 'Content-Type: application/json' -d '{"description":'
curl_status 200 "$tmp/ingest-health" "$INGEST_URL/health"
pass '202 returns within 2s while consumer is stopped; queued reports survive downtime/SIGKILL and each has one successful logical insertion; invalid JSON is rejected'

echo 'P3.6 — poison Kafka records are quarantined without stopping consumer'
login field-user "$FIELD_USER_PASSWORD" "$tmp/field-login-final"
field_token="$(jq -r '.data.accessToken // .accessToken // empty' "$tmp/field-login-final")"
before="$(curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000" \
  | jq '[.items[].message_id]')"
declare -A poison_restart_counts
for service in kafka ingest-pipeline field-report-consumer bnpb client-api auth pvmbg; do
  container="$(docker compose ps -q "$service")"
  [[ -n "$container" ]] || fail "$service container is missing before poison test"
  poison_restart_counts[$service]="$(docker inspect -f '{{.RestartCount}}' "$container")"
done
cat >"$tmp/p3-poison-producer.go" <<'GO'
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if len(os.Args) != 3 { log.Fatal("usage: p3-poison-producer <broker> <nonce>") }
	client, err := kgo.NewClient(kgo.SeedBrokers(os.Args[1]), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil { log.Fatal(err) }
	defer client.Close()
	nonce := os.Args[2]
	truncated := []byte(fmt.Sprintf("{\"broken\":\"%s", nonce))
	deep := []byte(strings.Repeat("[", 70) + fmt.Sprintf("\"%s\"", nonce) + strings.Repeat("]", 70))
	oversized := append(bytes.Repeat([]byte("x"), 262145), []byte(nonce)...)
	invalidUTF8 := append([]byte{0xff, 0xfe}, []byte(nonce)...)
	values := [][]byte{truncated, deep, oversized, invalidUTF8}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i, value := range values {
		record := &kgo.Record{Topic: "field-reports", Key: []byte(fmt.Sprintf("p3-poison-%d", i)), Value: value}
		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil { log.Fatal(err) }
	}
}
GO
broker_arch="$(docker image inspect apache/kafka:3.9.0 --format '{{.Architecture}}')"
case "$broker_arch" in
  amd64|arm64) ;;
  *) fail "unsupported Kafka image architecture for Go helper: $broker_arch" ;;
esac
(
  cd services/ingest-pipeline
  CGO_ENABLED=0 GOOS=linux GOARCH="$broker_arch" go build -trimpath \
    -o "$tmp/p3-poison-producer" "$tmp/p3-poison-producer.go"
)
docker compose cp "$tmp/p3-poison-producer" kafka:/tmp/p3-poison-producer
poison_nonce="p3-$RANDOM-$(date +%s)"
MSYS_NO_PATHCONV=1 docker compose exec -u 0 -T kafka /bin/sh -c \
  "chmod +x /tmp/p3-poison-producer && /tmp/p3-poison-producer kafka:9092 $poison_nonce"
poison_parked() {
  curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
    "$INGEST_URL/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000" \
    | jq -e --argjson old "$before" '[.items[] | select(.message_id as $id | ($old | index($id)) == null)] | length >= 4' >/dev/null
}
wait_for 'truncated, deep, oversized, and invalid UTF-8 cases to be quarantined' poison_parked
curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000" >"$tmp/poison-items"
jq --argjson old "$before" '[.items[] | select(.message_id as $id | ($old | index($id)) == null)]' \
  "$tmp/poison-items" >"$tmp/poison-new"
jq -e '
  any(.[]; .reason == "malformed_json_or_invalid_utf8")
  and any(.[]; .reason == "json_nesting_exceeds_64_levels")
  and any(.[]; .reason | startswith("payload_too_large:"))
  and all(.[]; .source == "FIELD_REPORT" and .received_at != "" and .retryable == false and .status == "parked")' \
  "$tmp/poison-new" >/dev/null \
  || fail 'one or more malformed input classes did not retain the expected quarantine reason'

# Inspect every poison payload through the Ops-only path and compare exact bytes.
deep_expected=""
for _ in $(seq 1 70); do deep_expected+="["; done
deep_expected+="\"$poison_nonce\""
for _ in $(seq 1 70); do deep_expected+="]"; done
truncated_payload="{\"broken\":\"$poison_nonce"
truncated_b64="$(printf '%s' "$truncated_payload" | base64 | tr -d '\n')"
deep_b64="$(printf '%s' "$deep_expected" | base64 | tr -d '\n')"
oversize_b64="$( { head -c 262145 /dev/zero | tr '\000' x; printf '%s' "$poison_nonce"; } | base64 | tr -d '\n')"
invalid_utf8_b64="$(printf '\377\376%s' "$poison_nonce" | base64 | tr -d '\n')"
truncated_count=0
deep_count=0
oversize_count=0
utf8_count=0
while IFS= read -r poison_id; do
  poison_id="${poison_id%$'\r'}" # Windows jq output may use CRLF.
  [[ -n "$poison_id" ]] || continue
  curl_status 200 "$tmp/poison-detail" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$poison_id"
  raw="$(jq -r .raw_base64 "$tmp/poison-detail")"
  case "$raw" in
    "$invalid_utf8_b64") utf8_count=$((utf8_count + 1)) ;;
    "$truncated_b64") truncated_count=$((truncated_count + 1)) ;;
    "$deep_b64") deep_count=$((deep_count + 1)) ;;
    "$oversize_b64") oversize_count=$((oversize_count + 1)) ;;
    *) fail "Ops raw bytes did not match a sent poison record ($poison_id)" ;;
  esac
done < <(jq -r '.[].message_id' "$tmp/poison-new")
[[ "$utf8_count" == 1 && "$truncated_count" == 1 && "$deep_count" == 1 && "$oversize_count" == 1 ]] \
  || fail 'expected one exact raw DLQ payload for each of the four poison classes'

# A clean field report sent after poison records still completes successfully.
control_key="p3-after-poison-$RANDOM"
control_report="{\"report_id\":\"$control_key\",\"reported_at\":\"2026-11-15T00:00:00Z\",\"description\":\"valid report after poison records\"}"
curl_status 202 "$tmp/after-poison-accepted" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $control_key" \
  -H 'Content-Type: application/json' -d "$control_report"
control_receipt="$(jq -r .receipt_id "$tmp/after-poison-accepted")"
control_processed() {
  [[ "$(docker compose logs --no-color field-report-consumer 2>/dev/null \
    | grep -F "receipt_id=$control_receipt" | grep -c 'result=success' || true)" == 1 ]]
}
wait_for 'valid report after poison records to be processed once' control_processed

for service in kafka ingest-pipeline field-report-consumer bnpb client-api auth pvmbg; do
  container="$(docker compose ps -q "$service")"
  [[ -n "$container" ]] || fail "$service container is missing"
  [[ "$(docker inspect -f '{{.State.Running}}' "$container")" == true ]] \
    || fail "$service is not running after malformed messages"
  [[ "$(docker inspect -f '{{.RestartCount}}' "$container")" == "${poison_restart_counts[$service]}" ]] \
    || fail "$service restarted after malformed messages"
done
pass 'all four malformed classes are quarantined, service containers remain running, and a later valid report succeeds once'

echo 'All P3 checks passed.'
```

### Pemetaan hasil ke requirement P3

| Bagian script | Problem yang dibuktikan | Kenapa hasilnya sesuai |
| --- | --- | --- |
| 1 | Deklarasi kompatibilitas skema | Tabel kompatibilitas di atas membedakan additive field yang diterima otomatis dari rename, type change, dan penghapusan field wajib yang harus dikarantina. Mock PVMBG menghasilkan setiap mode dan script memeriksa hasilnya. |
| 2 | DLQ utuh dengan sumber, alasan, waktu, dan kelas retry | Audit memeriksa metadata `source`, `reason`, `received_at`, `retryable`, `attempts`, dan `status`; Ops membandingkan bytes asli dari payload terpotong, UTF-8 invalid, nested, dan oversized dengan base64 hasil inspeksi. Retryable terus mencoba dan pulih saat BNPB hidup; schema failure tetap `parked` sampai replay. |
| 3 | Pesan rusak tidak membuat komponen crash/restart; pesan sehat tetap diproses | Helper mengirim empat payload byte yang rusak langsung ke Kafka. Script menunggu DLQ, memastikan semua container tetap hidup tanpa kenaikan `RestartCount`, lalu mengirim field report valid dan menunggu satu hasil sukses. |
| 4 | Replay setelah kompatibilitas ditambahkan, tanpa duplikat dan dengan urutan per `source_ref_id` | Masing-masing schema failure diproses oleh mapper replay v2 dan menghasilkan satu canonical event. Uji dua pesan untuk satu source ID menolak replay yang lebih baru dahulu, lalu menerima urutan benar dan memastikan satu baris kanonik. |
| 5 | Hanya BNPB Internal Ops dapat memeriksa raw failures | Endpoint raw menolak akses tanpa token, Audit, pipeline-internal, dan field-report token (`401`); Ops dapat memeriksa bytes persis. |
| 6 | `POST /field-reports` cepat memberi 202, tahan outage/crash, dan tidak memproses ganda | Dengan consumer mati, setiap request `202` harus selesai ≤2 detik. Beberapa laporan tertahan di Kafka, kemudian consumer di-SIGKILL setelah 202; setelah hidup lagi, setiap receipt memiliki satu sukses/insertion log. Idempotent retry mengembalikan receipt sama; konten berbeda mendapat `409`. |

Script ini memvalidasi perilaku pipeline lokal. Broker Compose hanya satu node dengan
replication factor 1, sehingga hasil ini tidak membuktikan ketahanan ketika node broker
hilang; uji tersebut memerlukan cluster Kafka multi-node. Konsumen yang berhenti atau
restart diuji terpisah dari kegagalan node broker.

---

## 12. Milestone 2 - Problem 4: satellite stream processor

The P4 service is `services/satellite-processor`. It keeps one SSE connection to the
satellite feed, persists each input and offset before acknowledging it locally, and
processes detections by UTC acquisition time. A transactional SQLite state store holds
the inbox, detection records, cluster lineage, processing checkpoint, and canonical
write outbox. Contiguous SSE records are committed in bounded SQLite batches with the
checkpoint in the same transaction; an interrupted batch is replayed from the last
committed offset. Patch updates are applied to known active detections. Updates whose
target is absent are skipped and counted as `orphan_updates` so they cannot stall later
feed records. The service writes versioned wildfire upserts/retractions through the
canonical API; it never writes directly to Aggregator storage.

The initial allowed lateness is configurable with `SATELLITE_ALLOWED_LATENESS` and is
set to six hours in local Compose. This is separate from the six-hour maximum time
difference for connecting two detections. Events arriving beyond the watermark still
go through the correction path and increment the late-detection count. The built-in
`/dashboard` and `/metrics` show offsets, backlog, watermark, singleton and late counts,
active fires, and p95 feed-receipt-to-canonical-ack latency.

### Local P4 acceptance script

Run the local mock scenarios from a Bash shell with Docker Compose, `curl`, and `jq`:

```bash
bash tools/p4-curl-test.sh
```

The script builds the P4 processor and runs a local SSE mock plus the M2 canonical
contract stub. Its numbered checks cover:

| Script check | P4 criterion | What it verifies |
| --- | --- | --- |
| P4.1 | Wildfire mapping and deduplication | Cross-satellite grouping, confidence-low exclusion, singleton count, exact detection keys, and mapped fields. |
| P4.2 | Event-time and late arrivals | Watermark behavior, accepted late correction, and late counter. |
| P4.3 | Feed updates and retractions | Versioned update, shrinking fire, and retract while retaining fire identity. |
| P4.4 | 60× burst and catch-up | A 400-event accelerated mock burst, all 200 expected clusters, zero remaining processing lag, elapsed catch-up, and p95. |
| P4.5 | Processor crash recovery | SIGKILL after a durable input, restart, resume, and one final fire. |
| P4.6 | Reference comparison | Compares active fires when `P4_REFERENCE_FILE` contains the assistant reference result; otherwise reports a skip. |
| P4.7 | Live feed | Use `bash tools/p4-curl-test.sh --live` after the exam session is started. Requires the token, province reference, and canonical API settings; reports durable/processed offsets and current lag during the live window. |
| P4.8 | Feed disconnect and resume | Local test forcibly closes SSE, appends records, then checks that the processor resumes to the latest contiguous offset. |

Local mode uses only test credentials and synthetic province points. It does not contact
the assistant feed. The live mode never calls `POST /v1/exam/start`; start the exam feed
at the scheduled demo time before running it. Set these environment variables first:

```bash
export P4_FEED_TOKEN='<assistant group token>'
export P4_PROVINCE_FILE='/path/to/bnpb-province-centroids.json'
export P4_CANONICAL_URL='http://bnpb:8085'
export P4_CANONICAL_TOKEN='<satellite-processor canonical write token>'
export P4_FEED_URL='https://satellite.naufarrel.dev'
export P4_LIVE_SECONDS=180
bash tools/p4-curl-test.sh --live
```

The script reads `P4_FEED_TOKEN` from the repository root `.env` when it is not already
exported in the shell. It never prints the token. P4.7 reports the live offsets and
backlog at the end of its capture window; P4.4 separately checks recovery from a bounded
60× burst. The official province-centroid table and deployed canonical API credentials
are still required for production-valid live mapping and writes.

`P4_REFERENCE_FILE` may point to a JSON file in the script's expected `{"fires":[...]}`
shape to enable P4.6 comparison. The official province-centroid table and assistant
reference output are not in this repository; the local test fixture is deliberately
synthetic and must not be used as production reference data. The local canonical stub
is in-memory and declares its durability simulated. A live acceptance run must point
at the deployed A/B canonical API to demonstrate actual canonical persistence.

---

## 13. Milestone 2 - Problem 5: Ketahanan Sensor Pantai

Problem 5 menjalankan image Sensor Pantai yang disediakan dan memakai jalur
lease/epoch yang sama dengan Problem 2.

Komponen:

- `services/bnpb` jalur sensor — timeout pendek, retry dengan full jitter dan retry budget, circuit breaker, health berbasis progres, fallback cursor, dedupe komposit, cache last-known-good, dan outbox peringatan kritis.
- `services/ingest-pipeline` — DLQ/replay durabel milik C untuk observasi sensor yang tidak valid.
- `services/field-notifier` — penerima webhook Tim Lapangan dengan dedupe dan metrik latensi end-to-end.
- `services/client-api` — admission control berbasis prioritas dengan kapasitas khusus Tim Lapangan.
- `deploy/sensor-pantai/` — manifest yang disediakan, blok resources wajib, dan overlay ujian.

### Jalankan secara lokal

`make dev-up` sudah menjalankan image asli `sustuna/sensor-pantai` dengan profil
practice. Token sensor dibaca dari `PANTAI_AUTH_TOKEN`.

### Overlay ujian

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb apply -f pantai-key.secret.yaml
kubectl kustomize deploy/sensor-pantai/overlays/exam | kubectl apply -f -
```

### Setup P5

P5 memakai setup bersama di **Bagian 10 - Setup sebelum Problem 1**: image,
Secret, deployment, dan verifikasi cluster. Setelah setup tersebut selesai,
jalankan port-forward yang tahan restart dan ambil kredensial dari Secret:

```bash
bash scripts/port-forward-loop.sh client-api 9080 8080 >/tmp/pf-client.log 2>&1 &
bash scripts/port-forward-loop.sh aggregator 18085 8085 >/tmp/pf-aggregator.log 2>&1 &
bash scripts/port-forward-loop.sh field-notifier 18097 8080 >/tmp/pf-notifier.log 2>&1 &

export BASE_URL=http://127.0.0.1:9080
export AGG_URL=http://127.0.0.1:18085
export INTERNAL_KEY="$(kubectl -n sistem-informasi-kebencanaan-bnpb get secret bnpb-secrets -o jsonpath='{.data.INTERNAL_API_KEY}' | base64 -d)"
export AUDIT_TOKEN="$(kubectl -n sistem-informasi-kebencanaan-bnpb get secret bnpb-secrets -o jsonpath='{.data.AUDIT_TOKEN}' | base64 -d)"
```

Prasyarat perintah: `kubectl`, `curl`, `jq`, `k6`, dan shell Unix/Git Bash.

### Pemeriksaan kriteria P5

**Kriteria 1 — peringatan kritis, p95 ≤ 5 detik, nol hilang**

```bash
curl -s http://127.0.0.1:18097/stats | jq
curl -s http://127.0.0.1:18097/metrics | grep critical_alert_pipeline_seconds
curl -s http://127.0.0.1:18085/metrics \
  | grep -E 'critical_outbox_enqueued_total|critical_outbox_delivered_total'
```

Expected: `duplicates` pada notifier hanya berasal dari retry, p95 pipeline di
bawah 5 detik, dan jumlah delivered sama dengan enqueued.

**Kriteria 2 — observasi tidak valid disisihkan, tidak disimpan**

Asumsi pengujian: profil Sensor Pantai yang diberikan asisten (profil ujian)
mengirim observasi yang melanggar kontrak dasar, misalnya field wajib hilang,
nilai di luar rentang, timestamp tidak valid, station tidak dikenal, atau
quality di luar enum. Tidak ada proxy atau perubahan image sensor.

Tunggu minimal satu siklus polling, lalu periksa DLQ dan metrik:

```bash
curl -s -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$AGG_URL/internal/audit/unprocessed?source=SENSOR_PANTAI&limit=1000" | jq .
curl -s "$AGG_URL/metrics" | grep dlq_parked_total
```

Expected: setiap record tidak valid memiliki `reason` (`schema`, `field_type`,
`non_finite`, `range`, `timestamp`, `future_timestamp`, `unknown_station`, atau
`quality`), `retryable=false`, dan `status=parked`. Record tersebut tidak boleh
muncul sebagai baris kanonik.

Untuk membandingkan dengan canonical storage, ambil daftar `source_ref_id` dari
DLQ dan pastikan tidak ada yang muncul di:

```bash
curl -s -H "X-Internal-Key: $INTERNAL_KEY" \
  "$AGG_URL/hazards?source=SENSOR_PANTAI&limit=1000" | jq .
```

**Kriteria 3 — bulkhead untuk BMKG/PVMBG**

Buat sensor benar-benar tidak tersedia (lebih jelas daripada mengganti seed).
Pada kind, gunakan image tag lokal dari langkah instalasi jika deployment
memakai digest yang tidak dapat di-import:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb scale deployment/sensor-pantai --replicas=0
sleep 20

# Ukur latensi baca khusus BMKG selama sensor mati.
for i in $(seq 1 200); do
  curl -s -o /dev/null -w '%{time_total}\n' \
    -H "X-Internal-Key: $INTERNAL_KEY" \
    "$AGG_URL/hazards?source=BMKG&limit=50"
done | sort -n | awk 'NR==190 { printf "p95=%.3fs\n", $1 }'

kubectl -n sistem-informasi-kebencanaan-bnpb scale deployment/sensor-pantai --replicas=1
```

Expected: p95 di bawah 300 ms; jalur sensor tidak boleh memblokir BMKG/PVMBG.

**Kriteria 4 — tidak ada lonjakan permintaan dan baca terdegradasi tetap cepat**

Biarkan sensor dalam keadaan `replicas=0` dari kriteria 3, lalu:

```bash
curl -s "$AGG_URL/sensor/status" | jq
curl -s "$AGG_URL/metrics" \
  | grep -E 'sensor_requests_total|sensor_seconds_since_last_fresh_observation|sensor_breaker_state'
curl -s -D - -o /dev/null \
  -H "X-Internal-Key: $INTERNAL_KEY" \
  "$AGG_URL/hazards?source=SENSOR_PANTAI&limit=1"
```

Expected: breaker terbuka, request sensor berhenti bertambah atau hanya probe
half-open, dan `/hazards` menjawab cepat dengan `X-Data-Freshness: degraded`,
`X-Source-SENSOR_PANTAI-Status: stale`, dan `Stale-Since`. Setelah selesai:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb scale deployment/sensor-pantai --replicas=1
```

**Kriteria 5 — Tim Lapangan dilayani, Media ditolak terkontrol**

```bash
k6 run -e BASE_URL="$BASE_URL" -e FIELD_VUS=4000 -e MEDIA_VUS=4000 loadtest/k6/priority.js
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/client-api 19080:8080 &
curl -s http://127.0.0.1:19080/metrics | grep admission_rejected_total
```

Expected: permintaan Tim Lapangan tetap berhasil; Media menerima `429`
terkontrol dengan `Retry-After`, tercatat pada `role="media"`.

**Kriteria 6 — kombinasi kegagalan pulih sendiri**

Daftar practice yang benar dari Tester Agent adalah:

```bash
kubectl -n tester-agent exec daemonset/tester-agent -- /app list
```

Gunakan nama yang benar, misalnya:

```bash
./scripts/gameday.sh
./scripts/gameday.sh kill-leader
./scripts/gameday.sh kill-leader wipe-node
```

`wipe-node` bersifat destruktif dan hanya boleh dijalankan pada cluster VM.
`partition` dan `clock-skew` tidak berjalan baik di kind. Example for a real VM
clock/partition run is under P2 criteria 4/5.

Expected: takeover lease, probe breaker, fallback cursor, retry outbox, dan
pemulihan cache basi terjadi tanpa intervensi manual. Tidak ada `kill-pod` atau
`kill-node` di daftar practice Tester Agent.

**Kriteria 7 — dashboard, metrik, dan jejak permintaan**

Grafana, Prometheus, Tempo, Loki, dan OTel Collector terpasang melalui
`deploy/kubernetes/observability` dan sudah termasuk dalam `kubectl apply -k deploy`.

```bash
bash scripts/port-forward-loop.sh grafana 3000 3000 >/tmp/pf-grafana.log 2>&1 &
bash scripts/port-forward-loop.sh prometheus 19090 9090 >/tmp/pf-prometheus.log 2>&1 &
# Buka http://127.0.0.1:3000, login admin, lalu buka dashboard BNPB M2.
# Korelasikan request lewat trace_id/correlation ID di log.
```

Bukti metrik dan trace juga dapat diambil langsung:

```bash
curl -s http://127.0.0.1:19090/api/v1/targets?state=active \
  | jq -r '.data.activeTargets[] | [.labels.job,.health] | @tsv'
curl -s "$AGG_URL/metrics" | grep -E 'http_requests_total|sensor_|lease_'
curl -s http://127.0.0.1:18097/metrics | grep -E 'critical_alert|field_notifier'
kubectl -n sistem-informasi-kebencanaan-bnpb logs deploy/aggregator --tail=100 \
  | grep -E 'trace_id|corr_id|span_name'
```

**Kriteria 8 — anggaran SLO**

Prometheus tersedia dari Service `prometheus` dan sudah menerima scrape dari
Aggregator, client-api, auth, canonical-store, coordinator, field-notifier, dan
satellite-processor.

```bash
curl -s http://127.0.0.1:18097/metrics \
  | grep -E 'critical_alert_pipeline_seconds_(bucket|count|sum)|critical_outbox'

curl -sG http://127.0.0.1:19090/api/v1/query \
  --data-urlencode 'query=histogram_quantile(0.95, sum(rate(critical_alert_pipeline_seconds_bucket[24h])) by (le))'
```

Anggaran: `used = jumlah event > 5 detik / (0,005 × jumlah event kritis pada
window)`. Gunakan `critical_alert_pipeline_seconds_count` sebagai penyebut dan
`critical_outbox_delivered_total`/`enqueued_total` untuk memeriksa kehilangan
event.

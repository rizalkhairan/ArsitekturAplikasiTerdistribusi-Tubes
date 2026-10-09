# Tugas Besar IF4031 — Arsitektur Aplikasi Terdistribusi

Platform koordinasi kebencanaan yang menjembatani **BMKG**, **PVMBG**, dan **BNPB**.
BMKG dan PVMBG adalah mock service pasif; BNPB melakukan polling, memetakan data ke
format kanonik `HazardEvent`, menyimpannya, dan mempublikasikannya ke message broker
untuk dikonsumsi beberapa consumer independen.

Cakupan: dua instansi sumber (BMKG, PVMBG), dua jenis bencana (seismik, vulkanik),
BNPB sebagai node koordinasi.

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

- **3b. Scope media** — login media, lalu `GET /hazards` hanya mengembalikan 6 field
  ringkasan; `?fields=latitude` → `403`.

  ```bash
  TOKEN=$(curl -s -X POST http://localhost:9080/auth/login \
    -H 'Content-Type: application/json' \
    -d '{"username":"media-user","password":"media-pass-2026"}' \
    | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)

  # hanya berisi hazard_id, source, hazard_type, severity, area_name, occurred_at
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

```
.
├── docker-compose.yml            # orkestrasi seluruh container
├── .env.example                  # template konfigurasi root
├── services/
│   ├── bmkg/                     # mock seismik/tsunami (gRPC)
│   ├── pvmbg/                    # mock vulkanik (REST)
│   ├── bnpb/                     # aggregator + canonical store + outbox + producer
│   ├── auth/                     # token service (JWT HS256)
│   ├── client-api/               # client-facing API + scope filter + rate limit
│   ├── dashboard-updater/        # consumer #1
│   ├── notifier/                 # consumer #2
│   └── analytics-consumer/       # consumer tambahan (profile)
├── client/                       # CLI Java (lihat client/README.md)
└── tools/loadtest/               # load generator untuk Problem 2
```

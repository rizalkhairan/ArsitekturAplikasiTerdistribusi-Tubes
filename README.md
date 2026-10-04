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
                         ┌──────────────────────┐
                         │   CLI (Java)         │
                         └──────────┬───────────┘
                                    │ HTTP (Bearer JWT)
                         ┌──────────▼───────────┐        ┌──────────────┐
                         │ Client-Facing API    │───────▶│ Auth Service │
                         │      :9080           │ proxy  │    :8000     │
                         └──────────┬───────────┘        └──────────────┘
                                    │ HTTP (X-Internal-Key)
                         ┌──────────▼───────────┐
                         │ BNPB Aggregator      │  owns Canonical Store (SQLite)
                         │  read API  :8085     │  owns outbox
                         │  health    :8086     │
                         └───┬──────────┬───────┘
                gRPC        │          │ REST
        ┌────────────────────▼──┐   ┌───▼──────────────────┐
        │ BMKG mock  :8081      │   │ PVMBG mock  :8082     │
        └───────────────────────┘   └───────────────────────┘
                                    │ publish HazardEvent
                         ┌──────────▼───────────┐
                         │ Kafka (KRaft) :9092  │
                         └───┬──────────┬───────┴────────┐
                             │          │                │
                   ┌─────────▼──┐  ┌────▼─────┐   ┌──────▼──────────┐
                   │ dashboard- │  │ notifier │   │ analytics-      │
                   │ updater    │  │  :8092   │   │ consumer :8093  │
                   │  :8091     │  └──────────┘   │ (profile)       │
                   └────────────┘                 └─────────────────┘
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
# 1. Salin template environment (file .env di-ignore oleh Git)
cp .env.example .env
cp services/bmkg/.env.example services/bmkg/.env
cp services/pvmbg/.env.example services/pvmbg/.env
cp services/bnpb/.env.example services/bnpb/.env
cp services/client-api/.env.example services/client-api/.env

# 2. Nyalakan seluruh sistem (satu perintah orkestrasi)
docker compose up --build -d

# 3. Tunggu ±20 detik, lalu cek status
docker compose ps
```

Untuk memulai benar-benar bersih (menghapus volume Kafka/DB):

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

### Root `.env` (dipakai Docker Compose untuk interpolasi)

| Variabel | Default contoh | Keterangan |
| --- | --- | --- |
| `JWT_SECRET` | `local-dev-jwt-secret` | Kunci penandatanganan JWT (harus sama untuk Auth & Client-Facing API) |
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

**2a. Head-of-line blocking** — set `PVMBG_MIN_DELAY_MS=3000` dan
`PVMBG_MAX_DELAY_MS=3000`, recreate pvmbg (`docker compose up -d --force-recreate pvmbg`).
Permintaan BMKG-only tetap cepat karena read API membaca Canonical Store lokal dan
poller berjalan paralel per sumber. Hasil uji kami: **p95 BMKG-only 282 ms (< 300 ms)**
saat PVMBG 3 detik.

**2b. Load test** — ≥50 koneksi paralel ≥60 detik memakai
[`tools/loadtest`](tools/loadtest/README.md):

```bash
NET=$(docker network ls --format '{{.Name}}' | grep tubes | head -n1)
docker run --rm --network "$NET" -v "$PWD/tools/loadtest:/src" -w /src \
  golang:1.26-alpine go run . -base http://client-api:8080 \
  -path '/hazards?limit=50' -concurrency 50 -duration 60s -label sustained
```

Hasil uji (50 concurrency, 60 s, Client-Facing API): **45.133 request, 752 rps,
p50 47 ms, p95 187 ms, p99 289 ms, 0 error, 0 throttled**. Saat concurrency
melebihi `MAX_CONCURRENT_REQUESTS`, kelebihan ditolak `429` (controlled rejection)
dan sistem tidak crash.

**2c. Outage terkendali**

```bash
# nyalakan outage
curl -u admin:local-dev-admin-password -X POST http://localhost:8082/admin/outage \
  -H 'Content-Type: application/json' -d '{"enabled":true}'
```

- Permintaan seismik tetap `200`.
- Permintaan vulkanik dijawab `200` dengan data terakhir dari Canonical Store,
  **disertai penanda kebaruan** berupa header:
  `X-Data-Freshness: degraded`, `X-Source-PVMBG-Status: stale`,
  `X-Source-PVMBG-Stale-Since`, `X-Source-PVMBG-Error`. Tidak `500`, tidak menggantung.
- Setelah outage dimatikan, status kembali `ok` tanpa restart.

### Problem 3 — Autentikasi antar-service & trust

- **3a. Isolasi kredensial antar instansi**

  ```bash
  # key BMKG dipakai ke PVMBG → 401
  curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:8082/volcanic-reports?since=2000-01-01T00:00:00Z' \
    -H 'X-BMKG-Key: local-dev-bmkg-key'

  # token PVMBG / key salah dipakai ke BMKG gRPC → Unauthenticated
  # (butuh grpcurl + file proto; jaringan compose bernama <project>_default)
  docker run --rm --network "$(docker network ls --format '{{.Name}}' | grep tubes | head -n1)" \
    -v "$PWD/services/bmkg/proto:/proto" fullstorydev/grpcurl:latest \
    -plaintext -import-path /proto -proto bmkg.proto \
    -H 'x-bmkg-key: local-dev-pvmbg-token' \
    bmkg:8081 bmkg.BMKG/GetSeismicEvents
  ```

- **3b. Scope media** — login media, lalu `GET /hazards` hanya mengembalikan 6 field
  ringkasan; `?fields=latitude` → `403`.

- **3c. Expiry + refresh** — login, tunggu >60 detik → `401`; panggil
  `POST /auth/refresh` dengan `refresh_token` → token baru; token lama tetap `401`;
  refresh token lama tidak bisa dipakai lagi (rotation).

- **3d. Tidak ada kredensial ter-hardcode** — semua rahasia dibaca dari environment;
  hanya `.env.example` yang ikut ter-commit.

### Problem 4 — Microservice & flexible storage

- **4a. Independen** — hentikan/rebuild/start satu container tanpa menyentuh yang lain.
  Contoh: `docker compose stop client-api`; BNPB read API tetap melayani
  (`curl -H 'X-Internal-Key: local-dev-internal-key' http://localhost:8085/hazards`);
  lalu `docker compose build client-api && docker compose up -d client-api`. Uptime
  container lain tidak berubah.
- **4b. Atribut dinamis** — record sebelum dan sesudah munculnya `confidence_level`
  tersimpan berdampingan di Canonical Store dan keduanya dapat dibaca (tanpa migrasi).
- **4c. Kepemilikan store** — `GET http://localhost:8085/hazards` tanpa
  `X-Internal-Key` → `401`; hanya Aggregator yang memegang volume `bnpb-data`, dan
  Client-Facing API membaca melalui API Aggregator.

### Problem 5 — Event-driven architecture

- **5a. Publikasi ke broker** — log BNPB memuat `outcome=acknowledged` lalu
  `outbox publish succeeded`; BNPB tidak memanggil consumer secara langsung.
- **5b. Dua consumer independen**

  ```bash
  docker compose logs --tail=50 dashboard-updater
  docker compose logs --tail=50 notifier
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group dashboard-updater-group
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notifier-group
  ```

- **5c. Hentikan consumer** — `docker compose stop notifier`; producer tetap publish,
  dashboard tetap memproses, lag `notifier-group` bertambah. `docker compose start notifier`
  → consumer menyusul (catch-up) dari offset terakhir, lag kembali `0`.
- **5d. Consumer baru** — `docker compose --profile analytics up -d --build analytics-consumer`.
  Group baru membaca seluruh event retensi (dari `earliest`) tanpa mengubah producer.

**Semantik pengiriman:** **at-least-once**. Kafka menyimpan event 7 hari dan consumer
commit offset setelah berhasil memproses. Karena itu consumer wajib idempoten;
setiap consumer memakai cache `event_id` (bounded) sehingga redelivery dicatat
`result=duplicate` dan tidak mengulang efek.

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
├── client/                       # CLI Java (lihat client/readme.md)
└── tools/loadtest/               # load generator untuk Problem 2
```

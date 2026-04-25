# Event Aggregator Service

Layanan aggregator event berbasis Python menggunakan FastAPI dan SQLite untuk menangani penerimaan data secara berkecepatan tinggi melalui In-Memory Queue dengan jaminan _Idempotency_

## Prerequisites

- Docker & Docker Compose
- Git

## How to Build

Aplikasi ini menggunakan Dockerfile berbasis `python:3.11-slim` dan disetel agar beroperasi dengan _non-root user_ (`appuser`) untuk best practice.

1. Jalankan aggregator dan publisher:
   ```bash
   docker compose up -d --build
   ```
2. Untuk melihat aktivitas API:
   ```bash
   docker compose logs -f
   ```
3. Menghentikan server:
   ```bash
   docker compose down
   ```

## Menjalankan Unit Tests (Pytest)

Proyek ini mencakup suite Unit Tests lengkap (berlokasi di folder `tests/`) yang memvalidasi kapabilitas dedup persisten, schema, dan performa (_small stress batch_).

Jalankan test langsung melalui runtime container:

```bash
docker compose exec aggregator python -m pytest -p no:cacheprovider tests/test_main.py -v
```

## Asumsi Desain

1. **At-Least-Once Delivery**: Asumsi utamanya adalah pihak pengirim (Publisher) mungkin mengirim ulang payload yang sama jika terjadi error asinkron.
2. **Pemisahan Async Queue**: Untuk mempertahankan _throughput_ respon API yang cepat, penyimpanan ke _database_ diisolasi menggunakan _background queue_ menggunakan `asyncio`.
3. **Ordering (Pengurutan)**: Strict _Total Ordering_ tidak diterapkan pada aggregator logika komutatif (seperti metrik/log) untuk fleksibilitas dan menghindari _bottleneck_ pada queue distribusi.

## Endpoints

1. `POST /publish`
   Menerima payload single JSON atau _Array of JSON_ (_batch mode_).
   ```json
   {
     "topic": "...",
     "event_id": "...",
     "timestamp": "ISO8601",
     "source": "...",
     "payload": {...}
   }
   ```
2. `GET /events?topic={nama_topik}`
   Mengambil kembali semua event unik yang berhasil direkam pada suatu topik tertentu.

3. `GET /stats`
   Mengembalikan metrik sistem operasional (total input `received`, berhasil `unique_processed`, ditolak `duplicate_dropped`, daftar semua topik aktif `topics`, dan durasi aktif server).

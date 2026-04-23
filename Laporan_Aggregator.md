# Laporan Aggregator Event Python

## Arsitektur & Implementasi
Aggregator Event ini dibangun menggunakan FastAPI dan asyncio untuk menangani event processing pipeline secara asynchronous, di mana penerimaan data (Producer) dan pemrosesan data (Consumer) dipisah menggunakan in-memory queue (`asyncio.Queue`).

Beberapa komponen penting:
1. **Model Event**: Divalidasi oleh payload Pydantic (`EventModel`) yang mengharuskan field seperti `topic`, `event_id`, `timestamp`, `source`, dan `payload`.
2. **Deduplikasi**: Consumer menyimpan setiap event ke dalam SQLite database. Kolom `(topic, event_id)` didefinisikan sebagai *Primary Key* komposit. Jika ada event duplikat dengan `topic` dan `event_id` yang sama, SQLite (`aiosqlite`) akan menolaknya dengan error `IntegrityError` (disebabkan oleh constraint unique). Ini memastikan *idempotency* karena secara atomik event hanya dicatat sekali, tahan banting meskipun terjadi system restart.
3. **Internal Queue & Async**: `POST /publish` hanya berfungsi mem-push event ke dalam `asyncio.Queue` untuk efisiensi latensi, lalu worker task background akan mengambil event-event dari queue.

## Tentang Total Ordering

**Apakah Total Ordering dibutuhkan?**
Dalam konteks layanan agregator event (seperti metrik analitik, log sistem, tracking status sensor), *Total Ordering* **umumnya tidak dibutuhkan secara ketat**. 

*Total Ordering* mensyaratkan setiap *node/event* diproses dalam satu urutan mutlak yang sama dari hulu ke hilir. Pada agregator, event yang dikumpulkan sering kali *commutative* – artinya memproses event A lalu B, akan memberikan hasil akhir / metrik total yang sama dengan memproses B lalu A, asalkan dua-duanya adalah agregasi jumlah (*count*, *sum*, dll). 

Meskipun demikian, ada beberapa _caveat_ (pengecualian) di mana *Ordering* menjadi sangat penting:
1. **Pemrosesan State Transitions Penuh**: Apabila event yang datang adalah representasi dari mutasi *State* (Contoh: "User Balance +$10", lalu "User Balance -$50"), maka *ordering* sangatlah penting karena keterlambatan berpotensi mengakibatkan *race condition* mutasi state jika event diterapkan bukan dalam urutan aslinya.
2. **Keterbatasan Aggregator (Tanpa Sequence)**: Jika arsitektur ini memproses suatu antrean dan kemudian didistribusikan ke log *eventual consistency* lainnya tanpa *sequence_number* absolut, pengamatan data secara logis dari luar bisa terlihat terbalik urutan kronologis terjadinya event tersebut.

Oleh karena itu, implementasi ini lebih befokus pada penerapan pola jaminan pengiriman **at-least-once delivery** melalui deduplikasi ketat yang menolak *(drop)* duplikat event, tanpa membuang sumber daya untuk memastikan *Strict Total Ordering* yang akan menjadi *bottleneck* besar di sistem terdistribusi.

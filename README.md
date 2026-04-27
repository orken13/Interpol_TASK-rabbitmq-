# 🔴 Interpol Red Notice Pipeline


Interpol'ün kırmızı liste (Red Notice) verisini periyodik olarak çeken, RabbitMQ kuyruğuna yazan, veritabanına kaydeden ve web arayüzü üzerinden canlı olarak sunan 3 container'lı Docker sistemi.

---

## 📐 Mimari Genel Bakış

```
┌──────────────────────────────────────────────────────────────────┐
│                         docker-compose                           │
│                                                                  │
│  ┌──────────────┐    ┌──────────────────┐    ┌───────────────┐  │
│  │  producer    │───▶│    rabbitmq      │───▶│   consumer    │  │
│  │ container_a  │    │   container_c    │    │  container_b  │  │
│  │ Python 3.12  │    │ rabbitmq:3-mgmt  │    │Python 3.12-slim│ │
│  │web_scarping.py│   │  AMQP: 5673      │    │web_server.py  │  │
│  └──────────────┘    │  Mgmt: 15673     │    └───────────────┘  │
│         │            └──────────────────┘           │           │
│   Interpol API                                  SQLite DB (than PostgreSQL)        │
└──────────────────────────────────────────────────────────────────┘
```

| Servis | Container Adı | Kaynak | Port |
|---|---|---|---|
| **rabbitmq** | `rabbitmq` | `container_c/` + `rabbitmq:3-management` | 5673 (AMQP), 15673 (UI) |
| **producer** | `producer` | `container_a/` | — |
| **consumer** | `consumer` | `container_b/` | 5000 (Flask) |

---

## 🗂️ Proje Yapısı

```
interpol-pipeline/
│
├── docker-compose.yml
├── .env
├── .env.example
├── README.md
│
├── container_a/                  # Producer — Interpol veri çekici
│   ├── Dockerfile                # python:3.12
│   ├── requirements.txt
│   └── web_scarping.py           # Ana script
│
├── container_b/                  # Consumer — Web sunucu
│   ├── Dockerfile                # python:3.12-slim
│   ├── requirements.txt
│   ├── web_server.py             # Flask uygulaması
│   └── templates/
│       └── index.html
│
├── container_c/                  # RabbitMQ config
│   └── Dockerfile                # rabbitmq:3-management üstüne
│


---

## ⚙️ Kurulum ve Çalıştırma

### Gereksinimler

- Docker >= 24.x
- Docker Compose >= 2.x

### 1. Repoyu Klonla

```bash
git clone https://github.com/kullanici-adi/interpol-pipeline.git
cd interpol-pipeline
```

### 2. Ortam Değişkenlerini Ayarla

```bash
cp .env.example .env
```

`.env` içeriği:

```env
# RabbitMQ
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672          # Container içi port (dışarıya 5673 olarak açılır)
RABBITMQ_USER=admin
RABBITMQ_PASS=admin123
RABBITMQ_QUEUE=interpol_notices

# Producer (container_a)
FETCH_INTERVAL_SECONDS=300
INTERPOL_API_URL=https://ws-public.interpol.int/notices/v1/red

# Consumer (container_b)
FLASK_PORT=5000
DB_PATH=/app/data/notices.db
```

> **Not:** Container'lar arası iletişimde port **5672** kullanılır (Docker internal network).
> Dışarıdan erişim için **5673** (AMQP) ve **15673** (RabbitMQ UI) kullanılır.

### 3. Çalıştır

```bash
docker-compose up --build
```

### 4. Servislere Eriş

| Servis | URL |
|---|---|
| Web Arayüzü (consumer) | http://localhost:5000 |
| RabbitMQ Yönetim Paneli | http://localhost:15673 |

---

## 🐳 Docker Detayları

### docker-compose.yml

```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management
    build: ./container_c
    container_name: rabbitmq
    ports:
      - "5673:5672"      # AMQP — dışarı 5673, içeride 5672
      - "15673:15672"    # Yönetim UI

  producer:
    build: ./container_a
    container_name: producer
    depends_on:
      - rabbitmq         # RabbitMQ hazır olmadan başlamaz

  consumer:
    build: ./container_b
    container_name: consumer
    depends_on:
      - rabbitmq         # RabbitMQ hazır olmadan başlamaz
```

---

### container_a/Dockerfile (Producer)

```dockerfile
FROM python:3.12

WORKDIR /app

COPY web_scarping.py /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "web_scarping.py"]
```

- `python:3.12` tam imaj kullanılır (scraping için gerekli sistem kütüphaneleri dahil).
- `web_scarping.py` Interpol API'den veriyi çekip RabbitMQ'ya yayınlar.

---

### container_b/Dockerfile (Consumer)

```dockerfile
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    python3-dev \
    build-essential \
    python3-distutils \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip setuptools
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "web_server.py"]
```

- `python:3.12-slim` küçük imaj kullanılır, gerekli sistem paketleri sonradan eklenir.
- `libpq-dev` PostgreSQL istemci desteği içindir (SQLite yerine Postgres kullanılacaksa).
- `web_server.py` hem kuyruğu dinler hem Flask web sunucuyu çalıştırır.

---

## 🔌 Servis Açıklamaları

### Producer (`container_a` — `web_scarping.py`)

- `FETCH_INTERVAL_SECONDS` aralıklarla Interpol Red Notice API'yi çeker.
- Her notice'i JSON formatında RabbitMQ kuyruğuna (`RABBITMQ_QUEUE`) yayınlar.
- `entity_id` alanı mesaj kimliği olarak kullanılır.

### Consumer (`container_b` — `web_server.py`)

- Flask başlarken arka thread'de RabbitMQ'yu dinler.
- Gelen mesaj veritabanında aranır:
  - **Yeni kayıt** → eklenir, arayüze yansır.
  - **Mevcut kayıt değişmişse** → güncellenir, ⚠️ **ALARM** olarak işaretlenir.
- Tüm kayıtlar ekleme/güncelleme zaman damgasıyla gösterilir.

### RabbitMQ (`container_c`)

- Standart `rabbitmq:3-management` imajı üzerine inşa edilir.
- Host makineden `5673` (AMQP) ve `15673` (Yönetim UI) portlarıyla erişilir.
- Container ağı içinde producer ve consumer `5672` portunu kullanır.

---

## 🖥️ Web Arayüzü

| Alan | Açıklama |
|---|---|
| İsim / Soyisim | Aranan kişi bilgisi |
| Uyruk | Kişinin ülkesi |
| Doğum Tarihi | |
| Suç Tanımı | İlgili suç kategorisi |
| Eklenme Zamanı | Sisteme ilk girdiği tarih-saat |
| Güncelleme Zamanı | Son değişiklik tarihi |
| Durum | 🟢 Yeni &nbsp;/&nbsp; ⚠️ Güncellendi 

---

## 🐳 Sık Kullanılan Komutlar

```bash
# Başlat
docker-compose up --build

# Arka planda başlat
docker-compose up -d --build

# Logları takip et
docker-compose logs -f

# Yalnızca producer logları
docker-compose logs -f producer

# Durdur
docker-compose down

# Verileri sıfırla (volume dahil)
docker-compose down -v
```

---

## ⚠️ Bilinen Notlar

- `depends_on` yalnızca container'ın **başladığını** bekler, RabbitMQ'nun **hazır olduğunu** garantilemez. Üretim ortamı için `web_scarping.py` ve `web_server.py` içinde bağlantı retry mekanizması eklenmelidir.
- RabbitMQ port çakışması olmaması için host makinede `5672` ve `15672` portları başka servis tarafından kullanılıyorsa compose dosyasındaki dış portlar değiştirilebilir.

---


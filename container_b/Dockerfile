FROM python:3.12-slim

WORKDIR /app

# Gerekli sistem bağımlılıklarını yükle
RUN apt-get update && apt-get install -y \
    python3-dev \
    build-essential \
    python3-distutils \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Python bağımlılıklarını yükle
COPY requirements.txt .
RUN pip install --upgrade pip setuptools
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyalarını kopyala
COPY . .

# Web server başlat
CMD ["python", "web_server.py"]

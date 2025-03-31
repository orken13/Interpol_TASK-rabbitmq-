FROM python:3.12

# Çalışma dizinini belirle
WORKDIR /app

# Dosyaları kopyala
COPY web_scarping.py /app/
COPY requirements.txt /app/

# Bağımlılıkları yükle
RUN pip install --no-cache-dir -r requirements.txt

# Çalıştırılacak komut
CMD ["python", "web_scarping.py"]

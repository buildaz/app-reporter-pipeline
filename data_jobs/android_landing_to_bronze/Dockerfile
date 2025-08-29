FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

COPY config.yaml .

COPY android_landing_to_bronze.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "android_landing_to_bronze.py"]
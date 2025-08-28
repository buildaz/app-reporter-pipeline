FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

COPY config.yaml .

COPY ios_ingestion.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "ios_ingestion.py"]
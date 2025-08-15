docker build -t app-reporter-android-ingestion:latest .
docker tag app-reporter-android-ingestion:latest gcr.io/site-reporter-436515/app-reporter-android-ingestion:latest
docker push gcr.io/site-reporter-436515/app-reporter-android-ingestion:latest --location=southamerica-east1

# gcloud run jobs replace job.yaml
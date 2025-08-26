docker build -t app-reporter-android-ingestion:latest .
docker tag app-reporter-android-ingestion:latest southamerica-east1-docker.pkg.dev/site-reporter-436515/app-reporter-artifacts/app-reporter-android-ingestion:latest
docker push southamerica-east1-docker.pkg.dev/site-reporter-436515/app-reporter-artifacts/app-reporter-android-ingestion:latest

envsubst < job.yaml | gcloud run jobs replace -
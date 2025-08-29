from datetime import datetime, timezone, timedelta
import json
import logging
import os

from google.cloud import storage
import pandas as pd
import serpapi
import yaml

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

with open('config.yaml', 'r') as file:
    MODULE_NAME = os.path.basename(__file__).replace('.py', '')
    CONFIG = yaml.safe_load(file)[MODULE_NAME]

SERP_CLIENT = serpapi.Client(api_key=os.getenv('SERPAPI_KEY'))
GCS = storage.Client()
LANDING_BUCKET = GCS.bucket(CONFIG['landing_bucket'][os.getenv('RUNTIME', 'dev')])
BRONZE_BUCKET = GCS.bucket(CONFIG['bronze_bucket'][os.getenv('RUNTIME', 'dev')])

INGESTION_TIMESTAMP = datetime.now(timezone(offset=timedelta(hours=-3)))
LOGGER.info(f"Starting landing to bronze transfer at {INGESTION_TIMESTAMP.strftime('%Y-%m-%d %H:%M:%S')}")

BUCKET_PREFIX = CONFIG['bucket_prefix']

if __name__ == '__main__':
    bronze_metadata_blob = BRONZE_BUCKET.blob(CONFIG['bronze_metadata'])
    if not bronze_metadata_blob.exists():
        LOGGER.info('No data of previous ingestions found, exiting.')
        exit(0)
    bronze_metadata = json.loads(bronze_metadata_blob.download_as_text())
    LOGGER.info(f'Found {len(bronze_metadata)} Android apps in the bronze metadata')

    for app in bronze_metadata:
        app_last_ingestion = app['last_ingestion']
        app_reviews_blob = LANDING_BUCKET.blob(
            f'{BUCKET_PREFIX}/{app_last_ingestion}/{app["id"]}_{app["country"]}_{app["lang"]}.json'
        )
        app_reviews = pd.json_normalize(json.loads(app_reviews_blob.download_as_string()), max_level=1)
        if app_reviews.empty:
            LOGGER.info(f'No reviews found for {app["name"]} ({app["id"]})')
            continue
        LOGGER.info(f'Found {len(app_reviews)} reviews for {app["name"]} ({app["id"]}) at ingestion {app_last_ingestion}')
        parquet_path = f'gs://{CONFIG["bronze_bucket"][os.getenv("RUNTIME", "dev")]}/{BUCKET_PREFIX}/{app_last_ingestion}/{app["id"]}_{app["country"]}_{app["lang"]}.parquet'
        app_reviews.to_parquet(parquet_path, index=False)
        LOGGER.info(f'Uploaded {len(app_reviews)} reviews for {app["name"]} ({app["id"]} → {app["country"]}-{app["lang"]}) to {parquet_path}')

    landing_reviews_folder_blobs = LANDING_BUCKET.list_blobs(prefix=f'{BUCKET_PREFIX}/')
    for review_blob in landing_reviews_folder_blobs:
        review_blob.delete()
    LOGGER.info(f'Deleted landing folder {BUCKET_PREFIX}/')

    LOGGER.info('Ingestion to bronze completed successfully')
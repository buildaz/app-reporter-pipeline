from datetime import datetime, timezone, timedelta
import json
import logging
import os

import dateparser
from google.cloud import storage
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
LOGGER.info(f"Starting ingestion at {INGESTION_TIMESTAMP.strftime('%Y-%m-%d %H:%M:%S')}")

BUCKET_PREFIX = 'ios_reviews'

def filter_date(review, filter_date):
    try:
        date = dateparser.parse(review['review_date']).date()
        return date >= filter_date
    except Exception as e:
        LOGGER.error(f'Error parsing date {review["review_date"]} for review: {e}')
        return False

if __name__ == '__main__':
    landing_metadata_blob = LANDING_BUCKET.blob(CONFIG['landing_metadata'])
    if landing_metadata_blob.exists():
        landing_metadata = json.loads(landing_metadata_blob.download_as_text())
        LOGGER.info(f"Found {len(landing_metadata)} iOS apps in the landing metadata")
    else:
        landing_metadata = list()
        LOGGER.info("No new iOS apps found in the landing database")
    bronze_metadata_blob = BRONZE_BUCKET.blob(CONFIG['bronze_metadata'])
    if bronze_metadata_blob.exists():
        bronze_metadata = json.loads(bronze_metadata_blob.download_as_text())
        LOGGER.info(f"Found {len(bronze_metadata)} iOS apps in the bronze metadata")
    else:
        bronze_metadata = list()
        LOGGER.info("No iOS apps found in the bronze database, starting fresh ingestion")
    metadata = bronze_metadata + landing_metadata
    if not metadata:
        LOGGER.info("No iOS apps to process, exiting.")
        exit(0)

    for app in metadata:
        if not app['active']: continue
        app_last_ingestion = datetime.strptime(app['last_ingestion'], '%Y-%m-%d')
        all_reviews = list()
        LOGGER.info(f'Fetching data for {app["name"]} ({app["id"]}) in \"{app["country"]}\" since {app["last_ingestion"]}')
        page = 1
        while True:
            params = {
                **CONFIG['serpapi_params'],
                'product_id': app['id'],
                'country': app['country'],
                'page': page
            }
            results = SERP_CLIENT.search(params=params)
            if 'error' in results:
                LOGGER.error(f"Error fetching data for {app['name']} ({app['id']}) in {app['country']}-{app['lang']}: {results['error']}")
                break
            reviews = results.get('reviews', [])
            if not reviews:
                LOGGER.info(f"No more reviews found for {app['name']} ({app['id']}) in {app['country']}-{app['lang']} on page {page}")
                break
            filtered_reviews = list(filter(lambda review: filter_date(review, app_last_ingestion.date()), reviews))
            all_reviews.extend(filtered_reviews)
            LOGGER.info(f"Fetched {len(reviews)} reviews, {len(filtered_reviews)} new since last ingestion, total collected: {len(all_reviews)}")
            if len(filtered_reviews) < len(reviews):
                LOGGER.info(f"Stopping pagination for {app['name']} ({app['id']}) as older reviews were encountered")
                break
            page += 1
        if all_reviews:
            blob_path = f"{BUCKET_PREFIX}/{app['id']}_{app['country']}_{INGESTION_TIMESTAMP.strftime('%Y%m%d_%H%M%S')}.json"
            blob = LANDING_BUCKET.blob(blob_path)
            blob.upload_from_string(json.dumps(all_reviews), content_type='application/json')
            LOGGER.info(f'Uploaded {len(all_reviews)} reviews for {app["name"]} ({app["id"]}) to {blob_path}')
        else:
            LOGGER.info(f'No new reviews to upload for {app["name"]} ({app["id"]})')

    active_metadata = [app for app in metadata if app['active']]
    for app in active_metadata:
        app['last_ingestion'] = INGESTION_TIMESTAMP.strftime('%Y-%m-%d')
    bronze_metadata_blob.upload_from_string(json.dumps(active_metadata, ensure_ascii=False, indent=4), content_type='application/json')
    LOGGER.info(f'Updated bronze metadata with {len(active_metadata)} apps')

    if landing_metadata_blob.exists():
        landing_metadata_blob.delete()
        LOGGER.info('Cleared landing metadata after successful ingestion')
from datetime import datetime, timezone, timedelta
import json
import logging
import os

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

BUCKET_PREFIX = 'android_reviews'

if __name__ == "__main__":
    landing_metadata_blob = LANDING_BUCKET.blob(CONFIG['landing_metadata'])
    if landing_metadata_blob.exists():
        landing_metadata = json.loads(landing_metadata_blob.download_as_text())
        LOGGER.info(f"Found {len(landing_metadata)} Android apps in the landing metadata")
    else:
        LOGGER.info("No new Android apps found in the landing database")
    bronze_metadata_blob = BRONZE_BUCKET.blob(CONFIG['bronze_metadata'])
    if bronze_metadata_blob.exists():
        bronze_metadata = json.loads(bronze_metadata_blob.download_as_text())
        LOGGER.info(f"Found {len(bronze_metadata)} Android apps in the bronze metadata")
    else:
        bronze_metadata = list()
        LOGGER.info("No Android apps found in the bronze database, starting fresh ingestion")
    metadata = bronze_metadata + landing_metadata

    for app in metadata:
        for platform in CONFIG['platforms']:
            LOGGER.info(f"Fetching data for {app['name']} ({app['id']}) in {app['country']}-{app['lang']} since {app['last_ingestion']}")
            pagination_token = None
            all_reviews = list()
            while True:
                params = {
                    **CONFIG['serpapi_params'],
                    'product_id': app['id'],
                    'gl': app['country'],
                    'hl': app['lang'],
                    'platform': 'phone'
                }
                if pagination_token:
                    params['next_page_token'] = pagination_token
                result = SERP_CLIENT.search(params=params)
                if 'error' in result:
                    LOGGER.error(f"Error fetching data for {app.name}: {result['error']}")
                    break
                filtered_reviews = list(
                    filter(
                        lambda review: datetime.strptime(review['iso_date'], '%Y-%m-%dT%H:%M:%SZ').date() < app.last_ingestion.date(),
                        result['reviews']
                    )
                )
                if not filtered_reviews:
                    LOGGER.info(f"No reviews found for {app.name} ({app.id}) in {app.country}-{app.lang} before last ingestion date.")
                    break
                reviews = [
                    {
                        'review_id': review['id'],
                        'title': review['title'],
                        'rating': review['rating'],
                        'created_at': review['iso_date'],
                        'fetched_at': INGESTION_TIMESTAMP.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'content': review.get('snippet', ''),
                        'app_id': app['id'],
                        'lang': app['lang'],
                        'provider': app['provider'],
                        'peer_group': app['peer_group'],
                        'country': app['country'],
                        'platform': platform,
                    } for review in result['reviews']
                ]
                all_reviews.extend(reviews)
                LOGGER.info(f"Fetched {len(reviews)} reviews for {app.name} ({app.id})")
                pagination_token = result.get('next_page_token')
                if not pagination_token:
                    break
            if all_reviews:
                blob_path = f"{BUCKET_PREFIX}/{INGESTION_TIMESTAMP.strftime('%Y-%m-%d')}/{app['id']}_{app['country']}_{app['lang']}.json"
                blob = LANDING_BUCKET.blob(blob_path)
                blob.upload_from_string(json.dumps(all_reviews, ensure_ascii=False, indent=4), content_type='application/json')
                LOGGER.info(f"Uploaded {len(all_reviews)} reviews for {app['name']} ({app['id']}) to {blob_path}")
            else:
                LOGGER.info(f"No new reviews to upload for {app['name']} ({app['id']})")

    for app in landing_metadata:
        app['last_ingestion'] = INGESTION_TIMESTAMP.strftime('%Y-%m-%dT%H:%M:%SZ')
    bronze_metadata_blob = BRONZE_BUCKET.blob(CONFIG['bronze_metadata'])
    bronze_metadata_blob.upload_from_string(json.dumps(bronze_metadata, ensure_ascii=False, indent=4), content_type='application/json')
    LOGGER.info(f"Updated bronze metadata with {len(landing_metadata)} apps")
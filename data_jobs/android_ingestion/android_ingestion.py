from datetime import datetime, timezone, timedelta
import logging
import os

import dotenv
import pandas as pd
import serpapi
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Date, PrimaryKeyConstraint
import yaml


dotenv.load_dotenv()

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

with open('config.yaml', 'r') as file:
    MODULE_NAME = os.path.basename(__file__).replace('.py', '')
    CONFIG = yaml.safe_load(file)[MODULE_NAME]

pgsql = create_engine(CONFIG['pg_url'].get(os.getenv('RUNTIME', 'dev')), echo=True, pool_pre_ping=True)
Session = sessionmaker(bind=pgsql)
session = Session()

Base = declarative_base()

SERP_CLIENT = serpapi.Client(api_key=os.getenv('SERPAPI_KEY'))

class AndroidApp(Base):
    __tablename__ = 'android_apps'

    id = Column(String, primary_key=True, index=True, nullable=False)
    lang = Column(String, nullable=False)
    country = Column(String, nullable=False)

    name = Column(String, nullable=False)
    peer_group = Column(String, nullable=False)
    last_ingestion = Column(Date, nullable=True)
    created_at = Column(Date, default=datetime.now(timezone(offset=timedelta(hours=-3))), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('id', 'lang', 'country', name='pk_android_apps'),
        {'extend_existing': True}
    )

INGESTION_TIMESTAMP = datetime.now(timezone(offset=timedelta(hours=-3)))
LOGGER.info(f"Starting ingestion at {INGESTION_TIMESTAMP.strftime('%Y-%m-%d %H:%M:%S')}")

BUCKET_PREFIX = 'android_reviews'

if __name__ == "__main__":
    android_apps = session.query(AndroidApp).all()
    if not android_apps:
        LOGGER.info("No Android apps found in the database.")
        exit(0)
    temp_dfs = list()
    for platform in CONFIG['platforms']:
        for app in android_apps:
            LOGGER.info(f"Fetching data for {app.name} ({app.id}) in {app.country}-{app.lang} for platform {platform}")
            pagination_token = None
            while True:
                params = {
                    **CONFIG['serpapi_params'],
                    'product_id': app.id,
                    'gl': app.country,
                    'hl': app.lang,
                    'platform': platform
                }
                if pagination_token:
                    params['next_page_token'] = pagination_token
                result = SERP_CLIENT.search(params=params)
                if 'error' in result:
                    LOGGER.error(f"Error fetching data for {app.name} in platform {platform}: {result['error']}")
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
                temp_dfs.append(pd.DataFrame([
                    {
                        'review_id': review['id'],
                        'title': review['title'],
                        'rating': review['rating'],
                        'iso_date': review['iso_date'],
                        'content': review['snippet'],
                        'app_id': app.id,
                        'lang': app.lang,
                        'country': app.country,
                        'platform': platform,
                    } for review in result['reviews']
                ]))
                pagination_token = result.get('serpapi_pagination', {}).get('next_page_token')
                if not pagination_token: break

    if temp_dfs:
        df = pd.concat(temp_dfs, ignore_index=True)

    destiny_bucket = CONFIG['destiny_bucket'].get(os.getenv('RUNTIME', 'dev'))
    LOGGER.info(f"Saving data to {destiny_bucket} at {INGESTION_TIMESTAMP.strftime('%Y%m%d_%H%M%S')}.parquet")
    df.to_parquet(f'{destiny_bucket}/{BUCKET_PREFIX}/{INGESTION_TIMESTAMP.strftime("%Y%m%d_%H%M%S")}.parquet', index=False)

    session.query(AndroidApp).update(
        {
            AndroidApp.last_ingestion: INGESTION_TIMESTAMP.date(),
        },
        synchronize_session=False
    )
    session.commit()

    LOGGER.info("Ingestion completed successfully.")
from datetime import datetime, timezone, timedelta
import json
import os

import dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, text, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import yaml

dotenv.load_dotenv()

with open('config.yaml', 'r') as file:
    MODULE_NAME = os.path.basename(__file__).replace('.py', '')
    CONFIG = yaml.safe_load(file)[MODULE_NAME]

DATABASE_URL = CONFIG['pg_url'][os.getenv('RUNTIME', 'dev')]

engine = create_engine(DATABASE_URL, echo=True, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class AndroidApp(Base):
    __tablename__ = 'android_apps'

    id = Column(String, primary_key=True, index=True, nullable=False)
    lang = Column(String, nullable=False)
    country = Column(String, nullable=False)

    name = Column(String, nullable=False)
    peer_group = Column(String, nullable=False)
    last_ingestion = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now(timezone(offset=timedelta(hours=-3))), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('id', 'lang', 'country', name='pk_android_apps'),
        {'extend_existing': True}
    )

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    print("Database tables created successfully.")

    with open('android_apps.json', 'r') as f:
        data = json.load(f)

    session = SessionLocal()
    try:
        for app in data:
            android_app = AndroidApp(
                id=app['id'],
                lang=app['lang'],
                country=app['country'],
                name=app['name'],
                peer_group=app['peer_group'],
                last_ingestion=datetime.strptime(app['last_ingestion'], '%Y-%m-%d').date() if 'last_ingestion' in app else None,
            )
            session.merge(android_app)
        session.commit()
        print(f"Data {app['id']}-{app['lang']}-{app['country']} inserted successfully.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")

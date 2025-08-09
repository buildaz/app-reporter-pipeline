import json

from sqlalchemy import create_engine, Column, Integer, String, DateTime, text, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone, timedelta

DATABASE_URL = "postgresql+psycopg2://buildaz:buildazio@localhost:5432/app_reporter"

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

    with open('db/android_apps.json', 'r') as f:
        data = json.load(f)

    session = SessionLocal()
    try:
        for app in data:
            android_app = AndroidApp(
                id=app['id'],
                lang=app['lang'],
                country=app['country'],
                name=app['name'],
                peer_group=app['peer_group']
            )
            session.merge(android_app)  # Use merge to avoid duplicates
        session.commit()
        print(f"Data {app['id']}-{app['lang']}-{app['country']} inserted successfully.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")

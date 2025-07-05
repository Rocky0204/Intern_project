from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "sqlite:///pluto.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
session_local = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db():
    with session_local() as db:
        yield db

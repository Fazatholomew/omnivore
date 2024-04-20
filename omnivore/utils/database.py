from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, Session
from os import getenv


def init_db():
    engine = create_engine(
        getenv("DATABASE_URL", "sqlite:///instance/omnivore.db"),
        echo=True,
        connect_args={"check_same_thread": False},
    )

    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )

    return db_session

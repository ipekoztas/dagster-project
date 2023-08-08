import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

user = 'postgress'
password = 'mysecretpassword'
host = '127.0.0.1'
port = 5432
database = 'postgres'
  
connection = create_engine(
    url=f"postgresql://{user}:{password}@{host}:{port}/{database}"
)

#host.docker.internal
engine = create_engine('postgresql+psycopg2://postgres:mysecretpassword@127.0.0.1:5432/postgres')


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
   

def get_db():
    db = SessionLocal()
    try:
        return db
    except:
        db.close()

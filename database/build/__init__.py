from pathlib import Path

from config import cfg

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from database.models import Base
from database.models.pitchbook import PbCompany
from database.models.ref_country import RefCountry

# Connect to the database using SQLAlchemy
with Path(cfg['path']['sqlite_file']) as sqlite_file_path:
    engine = create_engine(f"sqlite:///{sqlite_file_path}", echo=True)

Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()



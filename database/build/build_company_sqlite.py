"""
This program builds the company SQLite database from Pitchbook's csv file.
"""

import os
import csv
import json
from importlib import resources

from pathlib import Path

import configparser as cfp

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime

from database.models.pitchbook import Base
from database.models.pitchbook import PbCompany

cfg = cfp.ConfigParser(interpolation=cfp.ExtendedInterpolation())

with resources.path('config', 'config.ini') as path:
    cfg.read(path)


def get_company_data(file_path):
    """
    This function gets the data from the csv file
    """
    with open(file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data = [row for row in csv_reader]
        return data


def populate_database(session, company_data):
    # insert the data
    for row in company_data:

        company = (
            session.query(PbCompany)
            .filter(PbCompany.pb_id == row["pb_id"])
            .one_or_none()
        )
        if company is None:
            company = PbCompany(
                pb_id=row["pb_id"],
                company_exchange=row["company_exchange"],
                company_ticker=row["company_ticker"],
                company_name=row["company_name"],
                company_legal_name=row["company_legal_name"],
                company_former_name=row["company_former_name"],
                company_aka_name=row["company_aka_name"],
                company_HQ_country_2DID_iso=row["company_HQ_country_2DID_iso"],
                company_website=row["company_website"],
                extract_date=datetime.strptime(row["extract_date"], '%m/%d/%Y').date(),
                extract_source=row["extract_source"]
            )
            session.add(company)

        session.commit()

    session.close()


def main():
    print("Starting")

    # get the company data into a dictionary structure
    with Path(cfg['pitchbook']['company_csv_file']) as csv_file_path:
        data = get_company_data(csv_file_path)
        company_data = data

    # # get the file_path to the database file
    # with resources.path("database", "clean-performers.db") as sqlite_file_path:
    #     # does the database exist?
    #     if os.path.exists(sqlite_file_path):
    #         os.remove(sqlite_file_path)

    # Connect to the database using SQLAlchemy
    with Path(cfg['path']['sqlite_file']) as sqlite_file_path:
        engine = create_engine(f"sqlite:///{sqlite_file_path}", echo=True)

    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    populate_database(session, company_data)

    print("Finished")


if __name__ == "__main__":
    main()
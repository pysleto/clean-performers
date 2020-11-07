"""
This program builds the company SQLite database from Pitchbook's csv file.
"""

from pathlib import Path
import configparser as cfp
from importlib import resources

import csv

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd

from database.scripts.writing import save_query_to_csv

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


def populate_database(session, table, data):
    """
    This function checks data already in table and adds new observations to table
    """
    # insert the data
    for row in data:

        company = (
            session.query(table)
            .filter(table.pb_id == row["pb_id"])
            .one_or_none()
        )
        if company is None:
            company = table(
                pb_id=row["pb_id"],
                exchange=row["company_exchange"],
                ticker=row["company_ticker"],
                name=row["company_name"],
                name_legal=row["company_legal_name"],
                name_former=row["company_former_name"],
                name_aka=row["company_aka_name"],
                hq_country_2did_iso=row["company_HQ_country_2DID_iso"],
                website=row["company_website"],
                extract_date=row["extract_date"],
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

    populate_database(session, PbCompany, company_data)

    # Save an extract of the company table to a csv file
    company_check = session.query(
        PbCompany.pb_id, PbCompany.exchange, PbCompany.ticker, PbCompany.name, PbCompany.name_clean,
        PbCompany.name_legal, PbCompany.name_former, PbCompany.name_aka, PbCompany.hq_country_2did_iso,
        PbCompany.website, PbCompany.extract_date, PbCompany.extract_source
    ).all()

    fieldnames = ['pb_id', 'exchange', 'ticker', 'name', 'name_clean', 'name_legal', 'name_former',
                  'name_aka', 'hq_country_2did_iso', 'website', 'extract_date', 'extract_source']

    save_query_to_csv(Path(cfg['path']['project']).joinpath('check.csv'),
                      company_check,
                      fieldnames
                      )

    print("Finished")


if __name__ == "__main__":
    main()
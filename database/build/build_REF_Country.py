"""
This program builds the country table from a reference csv file.
"""

from pathlib import Path
import configparser as cfp
from importlib import resources

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database.scripts.read import get_data_from_csv
from database.scripts.write import save_query_to_csv

from database.models.ref_country import Base
from database.models.ref_country import RefCountry

cfg = cfp.ConfigParser(interpolation=cfp.ExtendedInterpolation())

with resources.path('config', 'config.ini') as path:
    cfg.read(path)


def populate_database(session, table, data):
    """
    This function checks data already in table and adds new observations to table
    """
    # insert the data
    for row in data:

        country = (
            session.query(table)
            .filter(table.country_2did_iso == row["country_2did_iso"])
            .one_or_none()
        )
        if country is None:
            country = table(
                country_2did_iso=row['country_2did_iso'],
                country_name_iso=row['country_name_iso'],
                country_name_simple=row['country_name_simple'],
                country_2did_soeur=row['country_2did_soeur'],
                country_3did_iso=row['country_3did_iso'],
                country_flag=row['country_flag'],
                jrc_region=row['jrc_region'],
                iea_region=row['iea_region'],
                is_oecd=row['is_oecd'],
                is_iea=row['is_iea'],
                is_mi=row['is_mi'],
                is_eu27=row['is_eu27'],
                is_tax_haven=row['is_tax_haven']
            )

            session.add(country)

        session.commit()

    session.close()


def main():
    print("Starting")

    # get the company data into a dictionary structure
    with Path(cfg['ref']['country_csv_file']) as csv_file_path:
        data = get_data_from_csv(csv_file_path)
        country_data = data

    # Connect to the database using SQLAlchemy
    with Path(cfg['path']['sqlite_file']) as sqlite_file_path:
        engine = create_engine(f"sqlite:///{sqlite_file_path}", echo=True)

    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    populate_database(session, RefCountry, country_data)

    # Save an extract of the company table to a csv file
    country_check = session.query(
        RefCountry.country_2did_iso, RefCountry.country_name_iso, RefCountry.country_name_simple,
        RefCountry.country_2did_soeur, RefCountry.country_3did_iso, RefCountry.country_flag, RefCountry.jrc_region,
        RefCountry.iea_region, RefCountry.is_oecd, RefCountry.is_iea, RefCountry.is_mi, RefCountry.is_eu27,
        RefCountry.is_tax_haven
    ).all()

    fieldnames = ['country_2did_iso', 'country_name_iso', 'country_name_simple', 'country_2did_soeur',
                  'country_3did_iso', 'country_flag', 'jrc_region', 'iea_region', 'is_oecd', 'is_iea', 'is_mi',
                  'is_eu27', 'is_tax_haven']

    save_query_to_csv(Path(cfg['path']['dbb']).joinpath('REF_Country.csv'),
                      country_check,
                      fieldnames
                      )

    print("Finished")


if __name__ == "__main__":
    main()
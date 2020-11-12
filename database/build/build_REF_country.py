"""
This program builds the country table from a reference csv file.
"""

from pathlib import Path

from config import cfg

from database.build import session

from database.scripts.read import get_data_from_csv
from database.scripts.write import save_query_to_csv

from database.models.country import RefCountry


def populate_database(current_session, table, data):
    """
    This function checks data already in table and adds new observations to table
    """
    # insert the data
    for row in data:

        country = (
            current_session.query(table)
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
                country_flag_http=row['country_flag'],
                jrc_region=row['jrc_region'],
                iea_region=row['iea_region'],
                is_oecd=row['is_oecd'],
                is_iea=row['is_iea'],
                is_mi=row['is_mi'],
                is_eu27=row['is_eu27'],
                is_tax_haven=row['is_tax_haven']
            )

            current_session.add(country)

        current_session.commit()

    current_session.close()


def main():
    print("Starting")

    # TODO: Upload a new field: Pitchbook country
    # get the company data into a dictionary structure
    with Path(cfg['ref']['country_csv_file']) as csv_file_path:
        data = get_data_from_csv(csv_file_path)
        country_data = data

    populate_database(session, RefCountry, country_data)

    # Save an extract of the company table to a csv file
    country_check = session.query(
        RefCountry.country_2did_iso, RefCountry.country_name_iso, RefCountry.country_name_simple,
        RefCountry.country_2did_soeur, RefCountry.country_3did_iso, RefCountry.country_flag_http, RefCountry.jrc_region,
        RefCountry.iea_region, RefCountry.is_oecd, RefCountry.is_iea, RefCountry.is_mi, RefCountry.is_eu27,
        RefCountry.is_tax_haven
    ).order_by(RefCountry.country_2did_iso).all()

    fieldnames = ['country_2did_iso', 'country_name_iso', 'country_name_simple', 'country_2did_soeur',
                  'country_3did_iso', 'country_flag_http', 'jrc_region', 'iea_region', 'is_oecd', 'is_iea', 'is_mi',
                  'is_eu27', 'is_tax_haven']

    save_query_to_csv(Path(cfg['path']['dbb']).joinpath('REF_Country.csv'),
                      country_check,
                      fieldnames
                      )

    print("Finished")


if __name__ == "__main__":
    main()
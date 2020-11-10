"""
This program builds the PB_Company table from a reference Pitchbook's csv file.
"""

from pathlib import Path

from config import cfg

from database.build import session

from database.scripts.read import get_data_from_csv
from database.scripts.write import save_query_to_csv

from database.models.pitchbook import PbCompany
from database.models.ref_country import RefCountry


def populate_database(current_session, table, data):
    """
    This function checks data already in table and adds new observations to table
    """
    # insert the data
    for row in data:

        company = (
            current_session.query(table)
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
                country_2did_iso=row["company_HQ_country_2DID_iso"],
                website=row["company_website"],
                extracted_on=row["extract_date"],
                extracted_from=row["extract_source"]
            )

            current_session.add(company)

        current_session.commit()

    current_session.close()


def main():
    print("Starting")

    # get the company data into a dictionary structure
    with Path(cfg['pitchbook']['company_csv_file']) as csv_file_path:
        data = get_data_from_csv(csv_file_path)
        company_data = data

    populate_database(session, PbCompany, company_data)

    # Save an extract of the company table to a csv file
    company_check = session.query(
        PbCompany.pb_id, PbCompany.exchange, PbCompany.ticker, PbCompany.name, PbCompany.name_clean,
        PbCompany.name_legal, PbCompany.name_former, PbCompany.name_aka,
        PbCompany.country_2did_iso, RefCountry.country_name_simple, RefCountry.jrc_region,
        PbCompany.website, PbCompany.extracted_on, PbCompany.extracted_from
    )
    company_check = company_check.join(RefCountry).all()

    fieldnames = ['pb_id', 'exchange', 'ticker', 'name', 'name_clean', 'name_legal', 'name_former',
                  'name_aka',
                  'country_2did_iso', 'country_name_simple', 'jrc_region',
                  'website', 'extracted_on',
                  'extracted_from']

    save_query_to_csv(Path(cfg['path']['dbb']).joinpath('PB_Company.csv'),
                      company_check,
                      fieldnames
                      )

    print("Finished")


if __name__ == "__main__":
    main()
"""
This program builds the ORBIS_Company table from a reference ORBIS company identification csv file.
"""

from pathlib import Path

from config import cfg

from database.build import session

from database.scripts.read import get_data_from_csv
from database.scripts.write import save_query_to_csv

from database.models.orbis import OrbisCompany, OrbisGuo
from database.models.country import RefCountry


def populate_companies(current_session, table, data):
    """
    This function checks data already in table and adds new observations to table
    """
    # insert the data
    for row in data:

        company = (
            current_session.query(table)
                .filter(table.bvd9_id == row["BvD9 number"])
                .one_or_none()
        )
        if company is None:
            company = table(
                name=row["Company name Latin alphabet"],
                bvd9_id=row["BvD9 number"],
                is_quoted=row["Quoted"],
                conso_code=row["Consolidation code"],
                ticker_id=row["Ticker symbol"],
                isin_id=row["ISIN number"],
                bvd_id=row["BvD ID number"],
                lei_id=row["LEI (Legal Entity Identifier)"],
                website=row["Website address"],
                country_2did_iso=row["Country ISO code"],
                nace_code=row["NACE Rev. 2, core code (4 digits)"],
                subs_n=row["No of subsidiaries (ultimately-owned included)"],
                guo_bvd9_id=row["GUO - BvD9 number"],
                extracted_on=row["extracted_on"],
                extracted_from=row["extracted_from"]
            )

            current_session.add(company)

        current_session.commit()

    current_session.close()


def populate_guos(current_session, table, data):
    """
    This function checks data already in table and adds new observations to table
    """
    # insert the data
    for row in data:

        company = (
            current_session.query(table)
                .filter(table.bvd9_id == row["GUO - BvD9 number"])
                .one_or_none()
        )
        if company is None:
            company = table(

                bvd9_id=row["GUO - BvD9 number"],
                name=row["GUO - Name"],
                type=row["GUO - Type"],
                bvd_id=row["GUO - BvD ID number"],
                lei_id=row["GUO - Legal Entity Identifier (LEI)"],
                country_2did_iso=row["GUO - Country ISO code"],
                direct_percent=row["GUO - Direct %"],
                extracted_on=row["extracted_on"],
                extracted_from=row["extracted_from"]
            )

            current_session.add(company)

        current_session.commit()

    current_session.close()


def main():
    print("Starting")

    # get the company data into a dictionary structure
    with Path(cfg['orbis']['company_csv_file']) as csv_file_path:
        data = get_data_from_csv(csv_file_path)
        company_data = data

    populate_companies(session, OrbisCompany, company_data)
    populate_guos(session, OrbisGuo, company_data)

    # Save an extract of the company table to a csv file
    company_check = session.query(
        OrbisCompany.bvd9_id, OrbisCompany.name, OrbisCompany.name_clean, OrbisCompany.is_quoted, 
        OrbisCompany.conso_code, OrbisCompany.ticker_id, OrbisCompany.isin_id, OrbisCompany.bvd_id, OrbisCompany.lei_id,
        OrbisCompany.website,
        OrbisCompany.country_2did_iso, RefCountry.country_name_simple, RefCountry.jrc_region,
        OrbisCompany.nace_code, OrbisCompany.subs_n,
        OrbisCompany.extracted_on, OrbisCompany.extracted_from, OrbisCompany.updated_on
    )
    company_check = company_check.join(RefCountry).all()

    fieldnames = ['bvd9_id', 'name', 'name_clean', 'is_quoted', 'conso_code', 'ticker_id', 'isin_id', 'bvd_id',
                  'lei_id', 'website',
                  'country_2did_iso', 'country_name_simple', 'jrc_region',
                  'nace_code', 'subs_n',
                  'extracted_on', 'extracted_from', 'updated_on']

    save_query_to_csv(Path(cfg['path']['dbb']).joinpath('ORBIS_Company.csv'),
                      company_check,
                      fieldnames
                      )
    # Save an extract of the guo table to a csv file
    guo_check = session.query(
        OrbisGuo.bvd9_id, OrbisGuo.name, OrbisGuo.name_clean, OrbisGuo.type, OrbisGuo.bvd_id, OrbisGuo.lei_id,
        OrbisGuo.country_2did_iso, RefCountry.country_name_simple, RefCountry.jrc_region,
        OrbisGuo.direct_percent,
        OrbisGuo.extracted_on, OrbisGuo.extracted_from, OrbisGuo.updated_on
    )
    guo_check = guo_check.join(RefCountry).all()

    guo_fieldnames = ['bvd9_id', 'name', 'name_clean', 'type', 'bvd_id', 'lei_id',
                  'country_2did_iso', 'country_name_simple', 'jrc_region',
                  'direct_percent',
                  'extracted_on', 'extracted_from', 'updated_on']

    save_query_to_csv(Path(cfg['path']['dbb']).joinpath('ORBIS_Guo.csv'),
                      guo_check,
                      guo_fieldnames
                      )

    print("Finished")


if __name__ == "__main__":
    main()

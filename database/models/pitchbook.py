from datetime import datetime

import csv

from sqlalchemy import Column, Integer, String, Date, ForeignKey, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

from database.scripts.cleaning import clean_name, fill_up

Base = declarative_base()


# TODO: enrich with country_ref data
class PbCompany(Base):
    __tablename__ = "PbCompany"

    pb_id = Column(String, primary_key=True)
    exchange = Column(String)
    ticker = Column(String)
    name = Column(String)
    name_clean = Column(String)
    name_legal = Column(String)
    name_former = Column(String)
    name_aka = Column(String)
    hq_country_2did_iso = Column(String)
    website = Column(String)
    extract_date = Column(Date)
    extract_source = Column(String)

    def __repr__(self):
        return "<Company ('%s', '%s')>" % (self.company_name, self.extract_source)

    def __init__(self, pb_id, exchange, ticker, name, name_legal, name_former, name_aka, hq_country_2did_iso, website,
                 extract_date, extract_source):
        self.pb_id = pb_id
        self.exchange = exchange
        self.ticker = ticker
        self.name = name
        self.name_clean = clean_name(fill_up(name_legal, name))
        self.name_legal = name_legal
        self.name_former = name_former
        self.name_aka = name_aka
        self.hq_country_2did_iso = hq_country_2did_iso
        self.website = website
        self.extract_date = datetime.strptime(extract_date, '%m/%d/%Y').date()
        self.extract_source = extract_source


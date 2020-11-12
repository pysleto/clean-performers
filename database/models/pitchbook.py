from datetime import datetime

from sqlalchemy import Column, Integer, String, Date, DateTime, ForeignKey, Table
from sqlalchemy.orm import relationship, backref

from database.models import Base

from database.scripts.clean import clean_name, fill_col


class PbCompany(Base):
    __tablename__ = "PB_companies"

    pb_id = Column(String(25), primary_key=True)
    exchange = Column(String(25))
    ticker_id = Column(String(25))
    name = Column(String(100))
    name_clean = Column(String(100), nullable=False, index=True)
    name_legal = Column(String(100))
    name_former = Column(String(100))
    name_aka = Column(String(100))
    country_2did_iso = Column(String(2), ForeignKey('REF_countries.country_2did_iso'))
    website = Column(String(255))
    extracted_on = Column(Date(), nullable=False)
    extracted_from = Column(String(100), nullable=False)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    country = relationship('RefCountry', uselist=False)

    def __repr__(self):
        return "<Company ('%s', '%s')>" % (self.name, self.extract_from)

    def __init__(self, pb_id, exchange, ticker_id, name, name_legal, name_former, name_aka, country_2did_iso, website,
                 extracted_on, extracted_from):
        self.pb_id = pb_id
        self.exchange = exchange
        self.ticker_id = ticker_id
        self.name = name
        self.name_clean = clean_name(fill_col(name_legal, name))
        self.name_legal = name_legal
        self.name_former = name_former
        self.name_aka = name_aka
        self.country_2did_iso = country_2did_iso
        self.website = website
        self.extracted_on = datetime.strptime(extracted_on, '%m/%d/%Y').date()
        self.extracted_from = extracted_from

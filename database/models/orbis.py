from datetime import datetime

import csv

from sqlalchemy import Column, Integer, String, Date, DateTime, ForeignKey, Table, Boolean
from sqlalchemy.orm import relationship, backref

from database.models import Base

from database.scripts.clean import clean_name, fill_col


class OrbisCompany(Base):
    __tablename__ = "ORBIS_companies"

    bvd9_id = Column(String(25), primary_key=True)
    name = Column(String(100), nullable=False)
    name_clean = Column(String(100))
    is_quoted = Column(Boolean)
    conso_code = Column(String(2))
    ticker_id = Column(String(25))
    isin_id = Column(String(25))
    bvd_id = Column(String(25))
    lei_id = Column(String(25))
    website = Column(String(255))
    country_2did_iso = Column(String(2), ForeignKey('REF_countries.country_2did_iso'))
    nace_code = Column(String(4))
    subs_n = Column(Integer())
    guo_bvd9_id = Column(String(25), ForeignKey('ORBIS_guos.bvd9_id'))
    extracted_on = Column(Date(), nullable=False)
    extracted_from = Column(String(100), nullable=False)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    country = relationship('RefCountry', uselist=False)

    def __repr__(self):
        return "<Company ('%s', '%s')>" % (self.name, self.extract_from)

    def __init__(self, bvd9_id, name, is_quoted, conso_code, ticker_id, isin_id, bvd_id, lei_id, website,
                 country_2did_iso, nace_code, subs_n, guo_bvd9_id, extracted_on, extracted_from):
        self.bvd9_id = bvd9_id
        self.name = name
        self.name_clean = clean_name(name)
        self.is_quoted = is_quoted == 'Yes'
        self.conso_code = conso_code
        self.ticker_id = ticker_id
        self.isin_id = isin_id
        self.bvd_id = bvd_id
        self.lei_id = lei_id
        self.website = website
        self.country_2did_iso = country_2did_iso
        self.nace_code = nace_code
        self.subs_n = subs_n
        self.guo_bvd9_id = guo_bvd9_id
        self.extracted_on = datetime.strptime(extracted_on, '%m/%d/%Y').date()
        self.extracted_from = extracted_from


class OrbisGuo(Base):
    __tablename__ = "ORBIS_guos"

    bvd9_id = Column(String(25), primary_key=True)
    name = Column(String(100), nullable=False)
    name_clean = Column(String(100))
    type = Column(String(25))
    bvd_id = Column(String(25))
    lei_id = Column(String(25))
    country_2did_iso = Column(String(2), ForeignKey('REF_countries.country_2did_iso'))
    # TODO: Replace n.a. values in all models
    direct_percent = Column(Integer())
    extracted_on = Column(Date(), nullable=False)
    extracted_from = Column(String(100), nullable=False)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    country = relationship('RefCountry', uselist=False)

    def __repr__(self):
        return "<Company ('%s', '%s')>" % (self.name, self.extract_from)

    def __init__(self, bvd9_id, name, type, bvd_id, lei_id, country_2did_iso, direct_percent, extracted_on,
                 extracted_from):
        self.bvd9_id = bvd9_id
        self.name = name
        self.name_clean = clean_name(name)
        self.type = type
        self.bvd_id = bvd_id
        self.lei_id = lei_id
        self.country_2did_iso = country_2did_iso
        self.direct_percent = direct_percent
        self.extracted_on = datetime.strptime(extracted_on, '%m/%d/%Y').date()
        self.extracted_from = extracted_from

from sqlalchemy import Column, Integer, String, Date, ForeignKey, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PbCompany(Base):
    __tablename__ = "PB_Company"

    pb_id = Column(String, primary_key=True)
    company_exchange = Column(String)
    company_ticker = Column(String)
    company_name = Column(String)
    company_legal_name = Column(String)
    company_former_name = Column(String)
    company_aka_name = Column(String)
    company_HQ_country_2DID_iso = Column(String)
    company_website = Column(String)
    extract_date = Column(Date)
    extract_source = Column(String)

    def __repr__(self):
        return "<Company ('%s', '%s')>" % (self.company_name, self.extract_source)
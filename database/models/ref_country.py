from sqlalchemy import Column, Integer, String, Date, ForeignKey, Table, Boolean
from sqlalchemy.orm import relationship, backref

from database.models import Base
from database.models.pitchbook import PbCompany


class RefCountry(Base):
    __tablename__ = "REF_Country"

    country_2did_iso = Column(String(50), primary_key=True)
    country_name_iso = Column(String(50), nullable=False)
    country_name_simple = Column(String(50), nullable=False)
    country_2did_soeur = Column(String(50))
    country_3did_iso = Column(String(50))
    country_flag = Column(String(255))
    jrc_region = Column(String(50), nullable=False)
    iea_region = Column(String(50))
    is_oecd = Column(Boolean)
    is_iea = Column(Boolean)
    is_mi = Column(Boolean)
    is_eu27 = Column(Boolean, nullable=False)
    is_tax_haven = Column(Boolean)

    company = relationship('PbCompany', back_populates='country')

    def __repr__(self):
        return "<Country ('%s', '%s')>" % (self.country_2did_iso, self.country_name_simple)

    def __init__(self, country_2did_iso, country_name_iso, country_name_simple, country_2did_soeur, country_3did_iso,
                 country_flag, jrc_region, iea_region, is_oecd, is_iea, is_mi, is_eu27, is_tax_haven):

        self.country_2did_iso = country_2did_iso
        self.country_name_iso = country_name_iso
        self.country_name_simple = country_name_simple
        self.country_2did_soeur = country_2did_soeur
        self.country_3did_iso = country_3did_iso
        self.country_flag = country_flag
        self.jrc_region = jrc_region
        self.iea_region = iea_region
        self.is_oecd = is_oecd == 'TRUE'
        self.is_iea = is_iea == 'TRUE'
        self.is_mi = is_mi == 'TRUE'
        self.is_eu27 = is_eu27 == 'TRUE'
        self.is_tax_haven = is_tax_haven == 'TRUE'

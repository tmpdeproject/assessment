
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy     import Column, Integer, String, DateTime, Text, Numeric, Date
from sqlalchemy.orm import declarative_base

from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

class source_data_EUR(Base):
    __tablename__ = 'source_data_EUR'

    id        = Column(Integer(),     primary_key=True)
    upload_dt = Column(DateTime() ,   nullable=False)
    currency  = Column(String(3)  ,   nullable=False)
    rate      = Column(Numeric(10,4), nullable=False)

    def __init__(self,upload_dt, currency, rate):
        self.upload_dt = upload_dt
        self.currency  = currency
        self.rate      = rate 

class source_data_USD(Base):
    __tablename__ = 'source_data_USD'

    id        = Column(Integer(),  primary_key=True)
    upload_dt = Column(DateTime(), nullable=False)
    api_data  = Column(JSONB,      nullable=False)

    def __init__(self,upload_dt, api_data):
        self.upload_dt = upload_dt
        self.api_data  = api_data

class be_holidays(Base):
    __tablename__ = 'be_holidays'

    id                    = Column(String(3),  primary_key=True)
    event_date            = Column(Date(),     nullable=False)
    holiday_description   = Column(Text(),     nullable=False)

    def __init__(self,event_date, holiday_description):
        self.event_date = event_date
        self.holiday_description = holiday_description
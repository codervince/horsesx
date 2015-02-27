from sqlalchemy import create_engine, Column, Integer, Float, String, Time, Date, ForeignKey, UniqueConstraint, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.orm import relationship, backref
import settings
from sqlalchemy import *


# ModelBase = declarative_base()

Base = declarative_base()
engine = create_engine(URL(**settings.DATABASE))
metadata = MetaData(bind=engine)

ModelBase = declarative_base()

# class HKTrackwork(Base):
#     __table__ = Table('hk_trackwork', metadata, autoload=True)

# class EventType(Base):
#     __table__ = Table('hk_trackwork_type', metadata, autoload=True)

# class HKVet(Base):
#     __table__ = Table('hk_vet', metadata, autoload=True)

# class Horse(Base):
#     __table__ = Table('horse', metadata, autoload=True)

# class Owner(Base):
#     __table__ = Table('owner', metadata, autoload=True)

# class Gear(Base):
#     __table__ = Table('hk_gear', metadata, autoload=True)


class EventType(ModelBase):
    __tablename__ = "hk_trackwork_type"
    id = Column(Integer, primary_key=True)
    name = Column("name", String(100), unique=True)
    UniqueConstraint('name', name='EventTypeName_uidx')


class Owner(ModelBase):
    __tablename__ = "owner"
    __table_args__ = (
        CheckConstraint(
            "homecountry in ('HKG', 'SIN', 'AUS', 'NZL', 'RSA', 'ENG', "
            "'IRE', 'DUB', 'IRE', 'SCO', 'MAC')"), )
    id = Column(Integer, primary_key=True)
    name = Column("name", String(255), unique=True)
    homecountry = Column('homecountry', String(3), nullable=False)


class Horse(ModelBase):
    __tablename__ = "horse"
    __tableargs__ = ( 
        CheckConstraint('Racecountry in ("HKG", "SIN", "AUS", "NZL", "RSA". "ENG", "IRE", "DUB", "IRE", "SCO", "MAC")')
        )
    id = Column(Integer, primary_key=True)
    Code = Column("code", String(6), nullable=False, unique=True)
    Name = Column("name", String(255), nullable=False)
    Sex = Column("sex", String(2), nullable=True)
    Racecountry = Column('racecountry', String(5), nullable=False)
    Countryoforigin = Column('countryoforigin', String(5), nullable=False)
    ImportType = Column("importtype", String(10))
    SireName = Column("sirename", String(255))
    DamName = Column("damname", String(255))
    DamSireName = Column("damsirename", String(255))
    SalePriceYearling = Column("salepriceyearling", Float)
    YearofBirth = Column("yearofbirth", Integer)
    # runners = relationship("HKRunner", backref="horse")
    trackevents = relationship("HKTrackwork", backref="horse")
    vetevents = relationship("HKVet", backref="horse")


class HKTrackwork(ModelBase):
    __tablename__ = "hk_trackwork"
    __table_args__ = (
        UniqueConstraint(
            'eventdate', 'eventtypeid', 'horseid',
            name='HKTrackwork_EventDateDescrHorseId_uidx'),)
    id = Column(Integer, primary_key=True)
    eventdate = Column("eventdate", Date, nullable=False)
    eventvenue = Column("eventvenue", String(100))
    eventdescription = Column("eventdescription", String(255))
    eventtypeid = Column("eventtypeid", Integer,
                         ForeignKey('hk_trackwork_type.id'))
    ownerid = Column("ownerid", Integer, ForeignKey("owner.id"))
    gearid = Column("gearid", Integer, ForeignKey("hk_gear.id"))
    horseid = Column("horseid", Integer, ForeignKey('horse.id'))


class HKVet(ModelBase):
    __tablename__ = "hk_vet"
    __table_args__ = (
        UniqueConstraint(
            'eventdate', 'details', 'horseid',
            name='HKVet_EventDateDetailsHorseId_uidx'), )
    id = Column(Integer, primary_key=True)
    horseid = Column("horseid", Integer, ForeignKey('horse.id'))
    eventdate = Column("eventdate", Date, nullable=False)
    details = Column("details", String(255))
    passeddate = Column("passeddate", Date, nullable=False)


class Gear(ModelBase):
    __tablename__ = "hk_gear"
    id = Column(Integer, primary_key=True)
    name = Column("name", String(255), unique=True)
    UniqueConstraint('name', name='GearName_uidx')


def get_engine():
    return create_engine(URL(**settings.DATABASE), pool_size=20, max_overflow=0)
    # return DBDefer(URL(**settings.DATABASE))


def create_schema(engine):
    ModelBase.metadata.create_all(engine)

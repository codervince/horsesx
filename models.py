from sqlalchemy import create_engine, Column, Integer, Float, String, Time, Date, ForeignKey, UniqueConstraint, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
import settings
from sqlalchemy import *


# ModelBase = declarative_base()

Base = declarative_base()
engine = create_engine(URL(**settings.DATABASE))
metadata = MetaData(bind=engine)

ModelBase = declarative_base()

class HKTrackwork(Base):
    __table__ = Table('hk_trackwork', metadata, autoload=True)

class EventType(Base):
    __table__ = Table('hk_trackwork_type', metadata, autoload=True)

class HKVet(Base):
    __table__ = Table('hk_vet', metadata, autoload=True)

class Horse(Base):
    __table__ = Table('horse', metadata, autoload=True)

class Owner(Base):
    __table__ = Table('owner', metadata, autoload=True)

class Gear(Base):
    __table__ = Table('hk_gear', metadata, autoload=True)


def get_engine():
    return create_engine(URL(**settings.DATABASE), pool_size=20, max_overflow=0)
    # return DBDefer(URL(**settings.DATABASE))

def create_schema(engine):
    ModelBase.metadata.create_all(engine)
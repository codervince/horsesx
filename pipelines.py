# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
from horsesx.models import *
from horsesx.items import *
from sqlalchemy.orm import sessionmaker, exc
from scrapy.exceptions import DropItem

from collections import defaultdict
from datetime import datetime

import pprint


class SQLAlchemyPipeline(object):
    def __init__(self):
        engine = get_engine()
        create_schema(engine)

        self.Session = sessionmaker(bind=engine)
        self.cache = defaultdict(lambda: defaultdict(lambda: None))
        # metadata = sa.MetaData(dbdefer.engine) 
        # TODO: get horsecolors!
    # @dbdefer    
    def process_item(self, item, spider):
        if not isinstance(item, (Horses2Item,)):
            return item

        session = self.Session()
        if isinstance(item, Horses2Item):
            trackwork = HKTrackwork(eventdate=item["EventDate"],
                                    eventvenue=item["EventVenue"],
                                    eventdescription=item["EventDescription"],
                                    # ImportType=item["ImportType"],
                                    # SireName=item["SireName"],
                                    # DamName=item["DamName"],
                                    # DamSireName=item["DamSireName"],
                                    eventtypeid=self.get_id(session, EventType, "name", {"name": item["EventType"]}),
                                    ownerid=self.get_id(session, Owner, "name",
                                                        {"name": item["Owner"], "homecountry": item["Homecountry"]}),
                                    gearid=self.get_id(session, Gear, "name", {"name": item["Gear"]}),
                                    horseid=self.get_id(session, Horse, "code",
                                                        {"code": item["HorseCode"], "name": item["HorseName"], \
                                                         "homecountry": item["Homecountry"], \
                                                         "sirename": item["SireName"], 
                                                         "damname": item["DamName"], \
                                                         "damsirename": item["DamSireName"],
                                                         "importtype": item["ImportType"]
                                                        }))
        try:
            session.add(trackwork)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item

    def get_id(self, session, model, unique, fields):
        fval = fields[unique]
        id = self.cache[model][fval]
        if id is None:
            # log.msg("[%s] %s cache missed for '%s'" % (self.__class__.__name__, model, fval), logLevel=log.DEBUG)
            try:
                id = session.query(model).filter(getattr(model, unique) == fval).one().id
            except exc.NoResultFound:
                item = model(**fields)
                session.add(item)
                session.flush()
                session.refresh(item)
                id = item.id
            self.cache[model][fval] = id
        # else:
        # log.msg("[%s] %s cache hit for '%s'" % (self.__class__.__name__, model, fval), logLevel=log.DEBUG)
        return id
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from __future__ import print_function

import logging
from Queue import Queue, Empty
from collections import defaultdict, Counter

from horsesx.items import *
from horsesx.models import *
from scrapy.signalmanager import SignalManager
from scrapy.signals import spider_closed
from scrapy.xlib.pydispatch import dispatcher
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.expression import ClauseElement
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThreadPool
from twisted.python.threadable import isInIOThread
from twisted.python.threadpool import ThreadPool


# Needed for multithreading, as I remember
Session = scoped_session(sessionmaker(bind=engine))


def get_or_create(model, defaults=None, **kwargs):
    ''' Short get or create implementation. Like in Django.

    This can run within the read_pool

    We don't use write scheduling here, because of very low amount of writes.
    Optimization unneeded, as I think.

    We use this function to prevent IntegrityError messages.
    '''

    defaults = defaults or {}

    session = Session()  # I'm thread safe =)

    query = session.query(model).filter_by(**kwargs)

    instance = query.first()

    created = False

    if not instance:
        params = dict(
            (k, v) for k, v in kwargs.iteritems()
            if not isinstance(v, ClauseElement))
        params.update(defaults)
        instance = model(**params)

        try:
            session.add(instance)
            session.commit()
            created = True
        except IntegrityError:
            session.rollback()
            instance = query.one()
            created = False
        except Exception:
            session.close()
            raise

    session.refresh(instance)  # Refreshing before session close
    session.close()
    return instance, created


class DBScheduler(object):
    ''' Database operation scheduler

    We will have one or more read thread and only one write thread.
    '''

    log = logging.getLogger('horsesx.DBScheduler')

    def __init__(self):
        from twisted.internet import reactor  # Imported here.inside

        self.reactor = reactor

        engine = get_engine()
        create_schema(engine)

        self.read_pool = ThreadPool(
            minthreads=1, maxthreads=16, name="ReadPool")

        self.write_pool = ThreadPool(
            minthreads=1, maxthreads=1, name="WritePool")

        self.read_pool.start()
        self.write_pool.start()

        self.signals = SignalManager(dispatcher.Any).connect(
            self.stop_threadpools, spider_closed)

        self.counters = defaultdict(lambda: Counter())

        self.cache = defaultdict(
            lambda: dict())

        self.write_queue = Queue()
        self.writelock = False  # Write queue mutex

    def stop_threadpools(self):
        self.read_pool.stop()
        self.write_pool.stop()
        for counter, results in self.counters.iteritems():
            print(counter)
            for modelname, count in results.iteritems():
                print('  ', modelname.__name__, '-', count)

    def _do_save(self):
        assert not isInIOThread()

        while not self.write_queue.empty():
            items = []

            try:
                self.writelock = True
                try:
                    while True:
                        items.append(self.write_queue.get_nowait())
                except Empty:
                    pass

                session = Session()

                try:
                    session.add_all(items)
                    session.commit()
                except:
                    session.rollback()
                    raise
                finally:
                    session.close()
            finally:
                self.writelock = False

    def save(self, obj):
        self.write_queue.put(obj)

        if self.writelock:
            return None
        else:
            return deferToThreadPool(
                self.reactor, self.write_pool, self._do_save)

    def _do_get_id(self, model, unique, fval, fields):
        assert not isInIOThread()

        return Session().query(model).filter(
            getattr(model, unique) == fval).one().id

    @inlineCallbacks
    def get_id(self, model, unique, fields):
        ''' Get an ID from the cache or from the database.

        If doesn't exist - create an item.
        All database operations are done from
        the separate thread

        '''
        assert isInIOThread()

        fval = fields[unique]

        try:
            result = self.cache[model][fval]
            self.counters['hit'][model] += 1
            returnValue(result)
        except KeyError:
            self.counters['miss'][model] += 1

        selectors = {unique: fval}

        result, created = yield deferToThreadPool(
            self.reactor, self.read_pool,
            get_or_create,
            model, fields, **selectors)

        result = result.id

        if created:
            self.counters['db_create'][model] += 1
        else:
            self.counters['db_hit'][model] += 1

        self.cache[model][fval] = result
        returnValue(result)


class SQLAlchemyPipeline(object):

    def __init__(self):

        self.scheduler = DBScheduler()

    @inlineCallbacks
    def process_item(self, item, spider):
        if isinstance(item, Horses2Item):

            ownerid = self.scheduler.get_id(
                Owner, 'name',
                {
                    "name": item["Owner"],
                    "homecountry": item["Homecountry"]
                })

            gearid = self.scheduler.get_id(
                Gear, "name",
                {
                    "name": item["Gear"]
                })

            eventtypeid = self.scheduler.get_id(
                EventType, "name",
                {
                    "name": item["EventType"]
                })

            horseid = self.scheduler.get_id(
                Horse, 'code',
                {
                    "code": item["HorseCode"],
                    "name": item["HorseName"],
                    "homecountry": item["Homecountry"],
                    "sirename": item["SireName"],
                    "damname": item["DamName"],
                    "damsirename": item["DamSireName"],
                    "importtype": item["ImportType"]
                })

            ownerid = yield ownerid
            gearid = yield gearid
            eventtypeid = yield eventtypeid
            horseid = yield horseid

            trackwork = HKTrackwork(
                eventdate=item["EventDate"],
                eventvenue=item["EventVenue"],
                eventdescription=item["EventDescription"],
                # ImportType=item["ImportType"],
                # SireName=item["SireName"],
                # DamName=item["DamName"],
                # DamSireName=item["DamSireName"],
                eventtypeid=eventtypeid,
                ownerid=ownerid,
                gearid=gearid,
                horseid=horseid)

            self.scheduler.save(trackwork)

        returnValue(item)

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
from sqlalchemy import update, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.expression import ClauseElement
from theseus import Tracer
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThreadPool
from twisted.python.threadable import isInIOThread
from twisted.python.threadpool import ThreadPool


# Needed for multithreading, as I remember
Session = scoped_session(sessionmaker(bind=engine))


def get_or_create(model, defaults=None, **kwargs):
    ''' Short get or create implementation. Like in Django.
    This can run within the thread_pool
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
            session.rollback()
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

        self.tracer = Tracer()
        self.tracer.install()

        self.thread_pool = ThreadPool(
            minthreads=1, maxthreads=16, name="ReadPool")

        # There should be only one pool in the write_pool
        # Never increase maxtreads value
        self.write_pool = ThreadPool(
            minthreads=1, maxthreads=1, name="WritePool")

        self.thread_pool.start()
        self.write_pool.start()

        self.signals = SignalManager(dispatcher.Any).connect(
            self.stop_threadpools, spider_closed)

        self.counters = defaultdict(lambda: Counter())

        self.cache = defaultdict(
            lambda: dict())

        self.write_queue = Queue()
        self.writelock = False  # Write queue mutex

        self.managed_save_queue = Queue()
        self.managed_save_lock = False  # Write queue mutex

        # Same as write queue. But for upsert
        # This is not as fast as simple save
        self.upsert_queue = Queue()
        self.upsertlock = False

    def stop_threadpools(self):
        self.thread_pool.stop()
        self.write_pool.stop()
        for counter, results in self.counters.iteritems():
            print(counter)
            for modelname, count in results.iteritems():
                print('  ', modelname.__name__, '-', count)

        with open('callgrind.theseus', 'wb') as f:
            self.tracer.write_data(f)

        self.tracer.uninstall()

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
                except IntegrityError:
                    session.rollback()
                    # For now return code is ignored
                    self.log.exception('Error occured while saving items')
                except Exception:
                    session.rollback()
                    # TODO implement saving one by one here
                    # to save as much as possible
                    raise
                finally:
                    session.close()
            finally:
                self.writelock = False

    def save(self, obj):
        ''' Save object.

        Very effective if we know, that object
        doesn't exist within the database.

        If the object already exists - ignore it.

        TODO: Maybe we need to implement strategy when
        object update will be needed on duplicate.
        '''

        self.write_queue.put(obj)

        if self.writelock:
            return None
        else:
            return deferToThreadPool(
                self.reactor, self.write_pool, self._do_save)

    def _do_update_if_changed(self, model, selector, updated):
        ''' Update model matching some *selector* dict and
        if it's changed.

        For each custom situation custom query should be built
        using *case* function.

        This function is very general.
        '''

        assert not isInIOThread()

        result_query = update(model)

        for field, value in selector.iteritems():
            result_query = result_query.where(
                getattr(model, field) == value)

        result_query = result_query.where(
            reduce(or_, [getattr(model, field) != value
                         for field, value in updated.iteritems()]))

        result_query = result_query.values(**updated)

        session = Session()

        try:
            result = session.execute(result_query)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return result.rowcount

    def update_if_changed(self, model, selector, updated):
        return deferToThreadPool(
            self.reactor, self.thread_pool,
            self._do_update_if_changed, model, selector, updated)

    def _do_update(self, model, selector, updated):
        assert not isInIOThread()

        session = Session()

        try:
            result = session.query(model).filter_by(**selector).update(updated)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return result.rowcount

    def update(self, model, selector, updated):
        ''' Update model matching some *selector* dict and
        replacing it's values from *updated* dict.

        Universal solution, but slow. Really slow.

        For each custom situation custom query should be built
        using *case* function.
        '''

        return deferToThreadPool(
            self.reactor, self.thread_pool,
            self._do_update, model, selector, updated)

    def _do_exists(self, model, selector):
        return bool(Session().query(model.id).filter_by(**selector).scalar())

    def exists(self, model, selector):
        ''' Check whether object matching selector exists '''
        return deferToThreadPool(
            self.reactor, self.thread_pool,
            self._do_exists, model, selector)

    def _do_get_id(self, model, unique, fval, fields):
        assert not isInIOThread()

        return Session().query(model).filter(
            getattr(model, unique) == fval).one().id

    @inlineCallbacks
    def get_id(self, model, unique, fields, update_existing=False):
        ''' Get an ID from the cache or from the database.
        If doesn't exist - create an item.
        All database operations are done from
        the separate thread

        - update_existing: Update object if it exists within the database.
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
            self.reactor, self.thread_pool,
            get_or_create,
            model, fields, **selectors)

        result = result.id

        if created:
            self.counters['db_create'][model] += 1
        else:
            self.counters['db_hit'][model] += 1
            if update_existing:
                updated = yield self.update_if_changed(
                    model, {unique: fval}, fields)

                if updated:
                    self.counters['updated'][model] += 1
                else:
                    self.counters['unchanged'][model] += 1

        self.cache[model][fval] = result
        returnValue(result)


class SQLAlchemyPipeline(object):

    def __init__(self):
        self.scheduler = DBScheduler()

    @inlineCallbacks
    def process_item(self, item, spider):
        if isinstance(item, VetItem):
        #do horse
            horseid = self.scheduler.get_id(
                Horse, 'code',
                {
                    "code": item["HorseCode"],
                    "name": item["HorseName"],
                    "homecountry": item["Homecountry"]
                })
            horseid = yield horseid

            vetwork = HKVet(
                eventdate = item["VetDate"],
                details = item["VetDetails"],
                passeddate = item["VetPassedDate"],
                horseid = horseid
                )
        #do vet table eventdate details passeddate
            self.scheduler.save(vetwork)

        if isinstance(item, Horses2Item):

            ownerid = self.scheduler.get_id(
                Owner, 'name',
                {
                    "name": item["Owner"],
                    "homecountry": item["Homecountry"]
                }, update_existing=True)

            gearid = self.scheduler.get_id(
                Gear, "name",
                {
                    "name": item["Gear"]
                }, update_existing=True)

            eventtypeid = self.scheduler.get_id(
                EventType, "name",
                {
                    "name": item["EventType"]
                }, update_existing=True)

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
                }, update_existing=True)

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

            exists = yield self.scheduler.exists(
                HKTrackwork,
                {
                    'eventdate': item["EventDate"],
                    'eventdescription': item["EventDescription"],
                    'horseid': horseid
                })

            if exists:
                res = yield self.scheduler.update_if_changed(
                    HKTrackwork,
                    {
                        'eventdate': item["EventDate"],
                        'eventdescription': item['EventDescription'],
                        'horseid': horseid
                    },
                    {
                        'eventtypeid': eventtypeid,
                        'ownerid': ownerid,
                        'gearid': gearid,
                        'eventvenue': item['EventVenue'],

                    })

                if res:
                    self.scheduler.counters['updated'][HKTrackwork] += 1
                else:
                    self.scheduler.counters['unchanged'][HKTrackwork] += 1
            else:
                self.scheduler.save(trackwork)
                self.scheduler.counters['create'][HKTrackwork] += 1

        returnValue(item)

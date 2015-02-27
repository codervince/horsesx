# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class Horses2Item(scrapy.Item):
    HorseCode = scrapy.Field()
    HorseName = scrapy.Field()
    EventDate = scrapy.Field()
    EventType = scrapy.Field()
    EventVenue = scrapy.Field()
    EventDescription = scrapy.Field()
    Gear = scrapy.Field()
    SireName = scrapy.Field()
    DamName = scrapy.Field()
    DamSireName = scrapy.Field()
    ImportType = scrapy.Field()
    Owner = scrapy.Field()
    Homecountry = scrapy.Field()
    CountryofOrigin = scrapy.Field()
    YearofBirth = scrapy.Field()

class VetItem(scrapy.Item):
    HorseCode = scrapy.Field()
    HorseName = scrapy.Field()
    Homecountry = scrapy.Field()
    VetDate = scrapy.Field()
    VetDetails = scrapy.Field()
    VetPassedDate = scrapy.Field()
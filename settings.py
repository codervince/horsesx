# -*- coding: utf-8 -*-

# Scrapy settings for horses2 project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'horses2x'

SPIDER_MODULES = ['horsesx.spiders']
NEWSPIDER_MODULE = 'horsesx.spiders'

# ITEM_PIPELINES = {'scrapy.contrib.pipeline.images.ImagesPipeline': 1}
ITEM_PIPELINES = {
    "horsesx.pipelines.SQLAlchemyPipeline": 10
}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'horses2 (+http://www.yourdomain.com)'

DATABASE = {'drivername': 'postgres',
            'host': 'localhost',
            'port': '5432',
            'username': 'horsesx',
            'password': '123456',
            'database': 'horsesx'}


USER_AGENT = "Googlebot/2.1 ( http://www.google.com/bot.html)"
LOG_LEVEL = 'INFO'

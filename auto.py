import string
import os
from twisted.internet import reactor
from scrapy.crawler import Crawler
# from horses2.spiders import Horses2xspider
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
#this line is dangerous for impport statements elsewhere!
# from spiders.horses2 import Horses2xspider 
#get this meetings codes
letters = string.ascii_uppercase
newletters = letters.replace('ABCDEFGHIJKL', '')
digits = string.digits

def main():
	for l in newletters:
		horsecodes = []
		counter = 0;
		numcode = '001'
		while numcode < '999':  
			numcode = format(counter, '03')
			horsecodes.append(l + numcode)
			counter +=1
		runscrapy(horsecodes)


def runscrapy(horsecodes):
	'''takes a list of horsecodes, runs scrapy, keeps track of urls which do not hit'''
	# misses = {}
	# config init
	
	# settings = get_project_settings()
	# crawler = Crawler(settings)
	# crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
	# crawler.configure()
	# TW = Horses2xspider(horses=horsecodes)
	# crawler.crawl(TW)
	# crawler.start()
	# log.start()
	# log.msg('Reactor activated...')
	# reactor.run()
	# log.msg('Reactor stopped.')

	#trackwork spider
	#old style command line usage:
	# #TRACK no URLS
	# spider = Horses2xspider()
	# settings = get_project_settings()
	# crawler = Crawler(settings)
	# crawler.configure()
	# crawler.crawl(spider)
	# crawler.start()
	#do in batches of 999
	horsecodes= ",".join(horsecodes)
	# print horsecodes
	os.system("scrapy crawl horses2x -a horses=" + horsecodes) 
	#use scrapyd
	
#run trackwork spider

if __name__ == "__main__":
	main()


#run vet spider




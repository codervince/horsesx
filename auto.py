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
# newletters = letters.replace('ABCDEFGHIJKL', '')
newletters = ['S']
digits = string.digits

#better option would be to get today's horsecodes

def gettodayshorsecodes(racedate):
	pass



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
	# os.system("scrapy crawl horses2x -a horse='P222'")

def runscrapy(horsecodes):
	'''takes a list of horsecodes, runs scrapy, keeps track of urls which do not hit'''
	# misses = {}
	# config init
	horsecodes= ",".join(horsecodes)
	# print horsecodes
	# os.system("scrapy crawl horses2x -a horse='P222'")
	os.system("curl http://localhost:6802/schedule.json -d project=horsesx -d spider=horses2x -d setting=DOWNLOAD_DELAY=2 -d horses=" + horsecodes + "")
	# os.system("scrapy crawl horses2x -a horses=" + horsecodes) 
	#use scrapyd

#run trackwork spider
if __name__ == "__main__":
	main()


#run vet spider




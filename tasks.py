from __future__ import absolute_import
from Crawling.celery import app
import requests
import urllib
import re
import os
import MySQLdb
from bs4 import BeautifulSoup

def fatchUrl(url,incrementDepth,cur,connection):
	print "incrementDepth"+str(incrementDepth)
	source_code = requests.get(url)
	plain_text = source_code.text
	soup = BeautifulSoup(plain_text,'html.parser')
	for link in soup.find_all('a'):
		links = link.get('href')
		regexresult = re.search("http",str(links))
		if links != None and regexresult is None:
			regexresult1 = re.search("//", str(links))
			regexresult2 = re.search("#", str(links))
			if regexresult1 is None and regexresult2 is None:
				conUrl = "http://en.wikipedia.org"+ str(links)
			elif regexresult2 is not None:
				conUrl = "http://en.wikipedia.org/wiki/Python_(programming_language)"+ str(links)
			elif regexresult1 is not None:
				conUrl = "http:"+ str(links)
			else:
				conUrl = str(links)
			queryInsUrl = '''INSERT INTO crawl_url 
						(url,parenturl,depth)
						VALUES (%s,%s,%s)'''
			cur.execute(queryInsUrl, (conUrl,url,incrementDepth))
			connection.commit()

@app.task
def consumer(depth):
	connection = MySQLdb.Connection("localhost","username","password","database_name")
	cur = connection.cursor()
	query = '''CREATE TABLE IF NOT EXISTS `crawl_url` (
    `id` int(11) unsigned NOT NULL auto_increment,
    `url` varchar(255) default '',
    `parenturl` varchar(255) default '',
    `depth` int(3) unsigned NOT NULL,
    PRIMARY KEY  (`id`))'''
	cur.execute(query)
	incrementDepth = depth+1
	if depth == 0:
		url = 'http://en.wikipedia.org/wiki/Python_(programming_language)'
		fatchUrl(url,incrementDepth,cur,connection)
	else:
		qrySelectUrl = '''SELECT url FROM crawl_url WHERE depth = %s'''
		cur.execute(qrySelectUrl, (depth))
		urlFetchQry = cur.fetchall()
		print urlFetchQry
		for rowUrl in urlFetchQry:
			url = rowUrl[0]
			print "hello"+str(url)
			fatchUrl(url,incrementDepth,cur,connection)

@app.task
def producer(depth):
	regionPath = '/home/username/crawling/'
	incrementDepth = depth+1
	connection = MySQLdb.Connection("localhost","username","password","database_name")
	cur = connection.cursor()
	depthFol = 'depth'+str(depth)
	if not os.path.isdir(regionPath):
		print "Created directory: "+regionPath
		os.makedirs(regionPath)
	depthfolder = os.path.join(regionPath,depthFol)+'/'
	if not os.path.isdir(depthfolder):
		print "Created directory: "+depthfolder
		os.makedirs(depthfolder)
	if depth == 0:
		urllib.urlretrieve("http://en.wikipedia.org/wiki/Python_(programming_language)", filename=regionPath+"index.html")
	selUrlQry = '''SELECT url FROM crawl_url
					WHERE depth = %s'''
	cur.execute(selUrlQry, (incrementDepth))
	urlFetchQry = cur.fetchall()
	for rowUrl in urlFetchQry:
		url = rowUrl[0]
		regexresult = re.search("http://en.wikipedia.org", str(url))
		regexresult1 = re.search("File:", str(url))
		if regexresult is not None and regexresult1 is None:
			filename = url.split('/')
			filename = filename[-1]
			try:
				urllib.urlretrieve(url, filename=regionPath+depthFol+'/'+filename+".html")
			except:
				print "link is wrong"
				with (open(regionPath+"errors.txt",'a')) as f:
				    f.write(url)
for depth in range(0,4):
	consumer.delay(depth)
	producer.delay(depth)
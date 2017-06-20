#!/usr/bin/env python

# Grab Google Scholar case decisions from search results
# and dump files into GridFS

import httplib
from BeautifulSoup import BeautifulStoneSoup
import re
import time
import random
import urllib
import urlparse as UP

host = 'search.library.duke.edu'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:8.0.1) Gecko/20100101 Firefox/8.0.1', 'Content-Type':'application/xml', 'Accept':'application/xml'}

conn = httplib.HTTPConnection(host)
count = 0

id = 'DUKE003157848'

query_dict = {'id': id, 'output-format': 'xml'}

query_str = urllib.urlencode(query_dict, True)
search_url = '/search?' + query_str
print search_url
# sys.stdout.flush()
	
# sys.stdout.flush()
time.sleep(0.2 + 0.2*random.random())
conn.request("GET", search_url, None, headers)

resp = conn.getresponse()
print "Response Status:", resp.status
# sys.stdout.flush()

if resp.status == 200:
	xml = resp.read()
	# http://stackoverflow.com/questions/1208916/decoding-html-entities-with-python
	soup = BeautifulStoneSoup(xml, convertEntities=BeautifulStoneSoup.HTML_ENTITIES, smartQuotesTo=None)
	doc = {}
	
	# Just to show where we are
	cc = soup.find('call-number')
	if cc is not None:
		print cc.item.text.split(' ')[0]
		# sys.stdout.flush()
	
	props = soup.find('properties')
	
	for prop in props.findAll(recursive=False):
		items = prop.findAll('item')
		if len(items) > 1:
			doc[prop.name] = [item.string for item in items]
		else:
			doc[prop.name] = items[0].string
	
	print doc

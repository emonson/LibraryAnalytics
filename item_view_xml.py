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

def get_soup(id):

  # id is a string, e.g. 
  # id = 'DUKE003157848'
  
	host = 'search.library.duke.edu'
	headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:8.0.1) Gecko/20100101 Firefox/8.0.1', 'Content-Type':'application/xml', 'Accept':'application/xml'}
	conn = httplib.HTTPConnection(host)

	query_dict = {'id': id, 'output-format': 'xml'}
	
	query_str = urllib.urlencode(query_dict, True)
	search_url = '/search?' + query_str
	print search_url
	# sys.stdout.flush()
		
	# sys.stdout.flush()
	# time.sleep(0.2 + 0.2*random.random())
	conn.request("GET", search_url, None, headers)
	
	resp = conn.getresponse()
	# print "Response Status:", resp.status
	# sys.stdout.flush()
	
	soup = None
	
	if resp.status == 200:
		xml = resp.read()
		# http://stackoverflow.com/questions/1208916/decoding-html-entities-with-python
		soup = BeautifulStoneSoup(xml, convertEntities=BeautifulStoneSoup.HTML_ENTITIES, smartQuotesTo=None)

	conn.close()
	return soup
	
	
def get_xml_object(id):
	
	soup = get_soup(id)
	
	if soup is not None:
		doc = {}
		cc = soup.find('call-number')
		if cc is not None:
			print repr(cc.item.text.split(' ')[0])
			# sys.stdout.flush()
		
		props = soup.find('properties')
		
		if props is not None:
			for prop in props.findAll(recursive=False):
				items = prop.findAll('item')
				if len(items) > 1:
					doc[prop.name] = [item.string for item in items]
				else:
					doc[prop.name] = items[0].string
		
		return doc

def get_call_number(id):
	
	soup = get_soup(id)
	
	if soup is not None:
		cc = soup.find('call-number')
				
		if cc is not None:
			return cc.item.text

	return None

if __name__ == '__main__':

  # id = 'DUKE003157848'
  id = 'DUKE003856078'
  obj = get_xml_object(id)
  print obj

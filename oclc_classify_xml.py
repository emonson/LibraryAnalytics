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
	
	oclc_host = 'classify.oclc.org'
	oclc_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:8.0.1) Gecko/20100101 Firefox/8.0.1', 'Content-Type':'application/xml', 'Accept':'application/xml'}
	oclc_conn = httplib.HTTPConnection(oclc_host)

	query_dict = {'oclc': id, 'summary': 'true'}
	
	query_str = urllib.urlencode(query_dict, True)
	search_url = '/classify2/Classify?' + query_str
	print search_url
	# sys.stdout.flush()
		
	# sys.stdout.flush()
	oclc_conn.request("GET", search_url, None, oclc_headers)
	
	resp = oclc_conn.getresponse()
	# print "Response Status:", resp.status
	# sys.stdout.flush()
	
	soup = None
	
	if resp.status == 200:
		xml = resp.read()
		# http://stackoverflow.com/questions/1208916/decoding-html-entities-with-python
		soup = BeautifulStoneSoup(xml, convertEntities=BeautifulStoneSoup.HTML_ENTITIES, smartQuotesTo=None)

	oclc_conn.close()
	return soup
	

def get_lcc_class(id):
	
	soup = get_soup(id)
	
	if soup is not None:
		cc = soup.find('lcc')
		if cc is not None:
			print 'oclc classify:', cc.mostpopular['sfa']
			return cc.mostpopular['sfa']

	return None

if __name__ == '__main__':

	id = '07636341'
	obj = get_lcc_class(id)
	print obj

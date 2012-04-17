import sys, BaseHTTPServer, urllib, urlparse, xml.dom.pulldom, xml.dom.minidom, cgi, socket, xml.sax.saxutils, StringIO, string, codecs, time, datetime, os
from urllib import urlencode

if len(sys.argv) > 2:
	parmType = sys.argv[1]
	parmValue = sys.argv[2]
	summaryInd = 'false'
	if len(sys.argv) > 3: summaryInd = sys.argv[3]
else:
	print 'Usage: python classifySample.py <param-type> <param-value> [true]'
	print 'For example: python classifySample.py isbn 0679442723 true'
	sys.exit(1)

print 'Sample Python Client for Classify 2'
print 'Searching for: ' + parmType + '=' + parmValue

base = 'http://classify.oclc.org/classify2/Classify?'
summaryBase = '&summary=true'

def getText(nodelist):
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc

xdoc = ''
try:
	if summaryInd == 'true': nextURL = base + urlencode({parmType:parmValue.encode('utf-8')}) + summaryBase
	else: nextURL = base + urlencode({parmType:parmValue.encode('utf-8')})
	print nextURL	
	urlobj = urllib.urlopen(nextURL)
	response = urlobj.read()
	urlobj.close()		
	xdoc = xml.dom.minidom.parseString(response)
except UnicodeDecodeError:
	print 'UnicodeDecodeError: ' + nextURL
except IOError:
	print 'IOError: ' + nextURL

response = xdoc.getElementsByTagName('response')[0]
respCode = response.attributes["code"].value
print 'Method response: ' + respCode

if respCode == '0' or respCode == '2':
	recommendations = xdoc.getElementsByTagName('recommendations')[0]
	if recommendations:
		ddc = recommendations.getElementsByTagName('ddc')[0]
		if ddc:
			for mostPopular in ddc.getElementsByTagName('mostPopular'):
				holdings = mostPopular.attributes["holdings"].value
				nsfa = mostPopular.attributes["nsfa"].value
				sfa = mostPopular.attributes["sfa"].value
				print 'DDC mostPopular: class=' + sfa + ' normalized=' + nsfa + ' holdings=' + holdings
				
		lcc = recommendations.getElementsByTagName('lcc')[0]
		if lcc:
			for mostPopular in lcc.getElementsByTagName('mostPopular'):
				holdings = mostPopular.attributes["holdings"].value
				nsfa = mostPopular.attributes["nsfa"].value
				sfa = mostPopular.attributes["sfa"].value
				print 'LCC mostPopular: class=' + sfa + ' normalized=' + nsfa + ' holdings=' + holdings
elif respCode == '4':
	works = xdoc.getElementsByTagName('works')[0]
	print 'Works found: ' + getText(works.childNodes)
	
	editions = xdoc.getElementsByTagName('editions')[0]
	for edition in editions.getElementsByTagName('edition'):
		author = edition.attributes["author"].value
		title = edition.attributes["title"].value
		editionCount = edition.attributes["editions"].value
		date = edition.attributes["date"].value
		format = edition.attributes["format"].value
		swid = edition.attributes["swid"].value
		print 'title=' + title.encode('ascii', 'replace') + ' author=' + author.encode('ascii', 'replace') + ' editions=' + editionCount + ' date=' + date + ' format=' + format + ' swid=' + swid

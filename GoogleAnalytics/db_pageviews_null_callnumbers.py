#!/usr/bin/python
# -*- coding: utf-8 -*-

# Looking at call numbers for pageviews that have a null LCC first letter

from pymongo import Connection

# Make a connection to Mongo.
try:
    # db_conn = Connection()
    db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['library_vis']

print 'Normalizing visitor counts by overall lcc sums'
total_count = db.pageviews.find({'lcc_first_letter':None}).count()

no_item_count = 0

for count,doc in enumerate(db.pageviews.find({'lcc_first_letter':None},{'_id':True,'uniqueid':True},snapshot=True)):
	
	item = db.items.find_one({'uniqueid':doc['uniqueid']})
	if item is not None:
		print item['call-number'][:100], ' : ', 
		if 'oclcnumber' in item:
			print item['oclcnumber']
		else:
			print '..', doc['uniqueid']
	else:
		no_item_count += 1

print 'Number of no items:', no_item_count
print 'Done'

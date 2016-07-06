#!/usr/bin/python
# -*- coding: utf-8 -*-

# http://architects.dzone.com/articles/walkthrough-mongodb-mapreduce

# This script does mapreduce on the library vis mongodb page views to
# count up views according to LCC category and subcategory. Then add normalized
# visitors values to the pageview docs themselves

from pymongo import Connection
from bson.code import Code
from bson.son import SON

# Make a connection to Mongo.
try:
    db_conn = Connection()
    # db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['library_vis']

# PAGEVIEWS

# overall sums per term over all entries
m = Code("function() {"
         "  emit( this.lcc_first_letter, this.visitors);"
         "}")

m2 = Code("function() {"
         "  emit( this.lcc_category, this.visitors);"
         "}")

r = Code("function(key,values) {"
         "  var total=0;"
         "  for ( var i=0; i<values.length; i++ ) {"
         "    total += values[i];"
         "  }"
         "  return total;"
         "}")

print 'Running overall counts on lcc first letter'
db.pageviews.map_reduce(m, r, out=SON([("replace","out_lccfirst_key_total_count")]))
print 'Running overall counts on lcc detailed category'
db.pageviews.map_reduce(m2, r, out=SON([("replace","out_lcccategory_key_total_count")]))
print 'Done'

print 'Loading results of mapreduce back in for normalization'
lcc_first_counts = {}
for doc in db.out_lccfirst_key_total_count.find({}):
	lcc_first_counts[doc['_id']] = doc['value']

lcc_cat_counts = {}
for doc in db.out_lcccategory_key_total_count.find({}):
	lcc_cat_counts[doc['_id']] = doc['value']

print 'Normalizing visitor counts by overall lcc sums'
total_count = db.pageviews.find().count()

for count,doc in enumerate(db.pageviews.find({},{'_id':True,'lcc_first_letter':True,'lcc_category':True,'visitors':True},snapshot=True)):

	if ('lcc_first_letter' in doc) and ('lcc_category' in doc):
		if count%1000 == 0:
			print count, '/', total_count
	
		visitors_per_lcc_first = float(doc['visitors'])/float(lcc_first_counts[doc['lcc_first_letter']])
		visitors_per_lcc_category = float(doc['visitors'])/float(lcc_cat_counts[doc['lcc_category']])
		
		db.pageviews.update({'_id':doc['_id']},{'$set':{'visitors_per_lcc_first':visitors_per_lcc_first,'visitors_per_lcc_category':visitors_per_lcc_category}}, upsert=False, multi=False, safe=True)
	
	elif ('visitors' in doc):
	
		visitors_per_lcc_first = float(doc['visitors'])/float(lcc_first_counts[None])
		visitors_per_lcc_category = float(doc['visitors'])/float(lcc_cat_counts[None])
		
		db.pageviews.update({'_id':doc['_id']},{'$set':{'visitors_per_lcc_first':visitors_per_lcc_first,'visitors_per_lcc_category':visitors_per_lcc_category}}, upsert=False, multi=False, safe=True)
	
print 'Done'

# SEARCH TERMS

# keyed by term, compile counts per date
map_f = Code("function() {"
             "  var ts = this.timestamp;"
             "  var month = ('00' + (ts.getUTCMonth() + 1)).slice(-2);"
             "  var day = ('00' + (ts.getUTCDate() + 0)).slice(-2);"
             "  var date = ts.getUTCFullYear() + '-' + month + '-' + day;"
             "  var yy = {};"
             "  yy[date] = this.visitors;"
             "  emit(this.lcc_first_letter, yy);"
             "}")

# keyed by date, compile counts per term
map_f2 = Code("function() {"
             "  var ts = this.timestamp;"
             "  var month = ('00' + (ts.getUTCMonth() + 1)).slice(-2);"
             "  var day = ('00' + (ts.getUTCDate() + 0)).slice(-2);"
             "  var date = ts.getUTCFullYear() + '-' + month + '-' + day;"
             "  var yy = {};"
             "  yy[this.lcc_first_letter] = this.visitors;"
             "  emit(date, yy);"
             "}")


reduce_f = Code("function(key, values) {"
              "  var out = {};"
              "  function merge(a, b) {"
              "    for (var k in b) {"
              "      if (!b.hasOwnProperty(k)) {"
              "        continue;"
              "      }"
              "      a[k] = (a[k] || 0) + b[k];"
              "    }"
              "  }"
              "  for (var i=0; i < values.length; i++) {"
              "    merge(out, values[i]);"
              "  }"
              "  return out;"
              "}")

# print 'Running lcc first letter key date counts'
# # db.searches.map_reduce(map, reduce, query={'new':True}, out=SON([("reduce","out_lccfirst_key_date_counts")]))
# db.pageviews.map_reduce(map_f, reduce_f, out=SON([("replace","out_lccfirst_key_date_counts")]))
# print 'Running date key lcc first letter counts'
# # db.searches.map_reduce(map2, reduce, query={'new':True}, out=SON([("reduce","out_date_key_lccfirst_counts")]))
# db.pageviews.map_reduce(map_f2, reduce_f, out=SON([("replace","out_date_key_lccfirst_counts")]))
# print 'done'

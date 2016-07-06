#!/usr/bin/python
# -*- coding: utf-8 -*-

# http://architects.dzone.com/articles/walkthrough-mongodb-mapreduce

from pymongo import Connection

# Make a connection to Mongo.
try:
    # db_conn = Connection()
    db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['library_vis']

# overall sums per term over all entries
m = """function m () { 
	var xx=this.visitors; 
	this.term_list.forEach( 
		function(z) { 
			emit( z, {count:xx});
		} 
	); 
}"""

r = """function r (key,values) { 
	var total=0; 
	for ( var i=0; i<values.length; i++ ) { 
		total += values[i].count; 
	} 
	return {count:total}; 
}"""

print 'Running overall counts'
db.searches.map_reduce(m, r, out="out_term_key_overall_counts")

# keyed by term, compile counts per date
map = """function map () {
  var ts = this.timestamp;
  var count = this.visitors;
  // zero pad day and month to two digits
  var month = ("00" + (ts.getUTCMonth() + 1)).slice(-2);
  var day = ("00" + (ts.getUTCDate() + 0)).slice(-2);
  var date = ts.getUTCFullYear() + "-" + month + "-" + day;
  this.term_list.forEach(
  	function(z) {
  		var yy = {};
  		yy[date] = count;
  		emit(z, yy);
  	}
  );
}"""

# keyed by date, compile counts per term
map2 = """function map () {
  var ts = this.timestamp;
  var count = this.visitors;
  // zero pad day and month to two digits
  var month = ("00" + (ts.getUTCMonth() + 1)).slice(-2);
  var day = ("00" + (ts.getUTCDate() + 0)).slice(-2);
  var date = ts.getUTCFullYear() + "-" + month + "-" + day;
  this.term_list.forEach(
  	function(z) {
  		var yy = {};
  		yy[z] = count;
  		emit(date, yy);
  	}
  );
}"""

reduce = """function reduce(key, values) {
  var out = {};
  function merge(a, b) {
    for (var k in b) {
      if (!b.hasOwnProperty(k)) {
        continue;
      }
      a[k] = (a[k] || 0) + b[k];
    }
  }
  for (var i=0; i < values.length; i++) {
    merge(out, values[i]);
  }
  return out;
}"""

print 'Running term key date counts
db.searches.map_reduce(map, reduce, out="out_term_key_date_counts")
print 'Running date key term counts'
db.searches.map_reduce(map2, reduce, out="out_date_key_term_counts")
print 'done'
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

db.searches.map_reduce(m, r, out="testoutput")

# keyed by term, compile counts per date
map = """function map () {
  var ts = this.timestamp;
  var count = this.visitors;
  var date = ts.getUTCFullYear() + "-" + (ts.getUTCMonth() + 1) + "-" + ts.getUTCDate();
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
  var date = ts.getUTCFullYear() + "-" + (ts.getUTCMonth() + 1) + "-" + ts.getUTCDate();
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

db.searches.map_reduce(map, reduce, out="out_term_by_date")
db.searches.map_reduce(map2, reduce, out="out_date_by_term")
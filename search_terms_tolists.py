#!/usr/bin/python
# -*- coding: utf-8 -*-

# Build terms list for all search strings

def tokenize(termstring, singularize=False):
	
	contraction_dict = {"n't":'not', "'ll":'will'}
	stopwords = nltk.corpus.stopwords.words('english')
	
	ncends_re = re.compile(r'^[^a-zA-Z0-9]*([a-zA-Z0-9]+)[^a-zA-Z0-9]*$')
	ncall_re = re.compile(r'^[^a-zA-Z0-9]+$')
	ncany_re = re.compile(r".*?[^-'.a-zA-Z]")
	multidashes_re = re.compile(r'-{2,}')
	underscore_re = re.compile(r'_')
	
	inflect = Inflector.Inflector()
	
	term_list = []
	
	termstring = multidashes_re.sub(' ', termstring)
	termstring = underscore_re.sub(' ', termstring)
	
	# Split text into sentences using Punkt sentence tokenizer
	sentences = nltk.sent_tokenize(termstring)
	for sentence in sentences:
		# Split into words using Treebank word tokenizer
		words = nltk.word_tokenize(sentence)
		# Convert to lowercase
		for word in [w.lower() for w in words]:
			# Do contraction substitution
			if word in contraction_dict:
				word = contraction_dict[word]
			# Strip off any non-word characters from beginning and end
			word = ncends_re.sub('\g<1>', word)
			# Only accept words greater than 2 characters and not in stopwords and not all non-characters
			any_nc = ncany_re.match(word)
			if len(word) > 2 and word not in stopwords and (any_nc is None):
				# Singularize all words (should only do plural nouns...)
				if singularize:
					word = inflect.singularize(word)
				term_list.append(word)
	
	return term_list

# ---

import re
import nltk
# And, singularize with Inflector
from Inflector import Inflector
from pymongo import Connection

# Make a connection to Mongo.
try:
    # db_conn = Connection()
    db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['library_vis']
crit = {'term_list':{'$exists':False}}

total_count = db.searches.find(crit).count()

for count,doc in enumerate(db.searches.find(crit, {'_id':True,'searchkeyword':True}, snapshot=True)):
	if count%1000 == 0:
		print count, '/', total_count
	
	term_list = tokenize(doc['searchkeyword'])
	
	db.searches.update({'_id':doc['_id']},{'$set':{'term_list':term_list,'n_terms':len(term_list)}}, upsert=False, multi=False, safe=True)

#!/usr/bin/env python
# coding=UTF-8
#
# Modified from Tim Strehle's example code
# https://github.com/tistre/nltk-examples
#
# Output the 50 most-used words from a text file, using NLTK FreqDist()
# (The text file must be in UTF-8 encoding.)
#
# Usage:
#
#   ./freqdist_top_words.py input.txt
#
# Sample output:
#
# et;8
# dolorem;5
# est;4
# aut;4
# sint;4
# dolor;4
# laborum;3
# ...
#
# Requires NLTK. Official installation docs: http://www.nltk.org/install.html
#
# I installed it on my Debian box like this:
#
# sudo apt-get install python-pip
# sudo pip install -U nltk
# python
# >>> import nltk
# >>> nltk.download('stopwords')
# >>> nltk.download('punkt')
# >>> exit()

import sys
import codecs
import nltk
from nltk.corpus import stopwords
import glob
import os
import string

data_dir = '/Volumes/Data/LibraryVis/PDFs_TOC/txt_german'
data_file_ending = '_ocr_ger.txt'
out_filename = 'file_term_freqs_20170202.txt'

# NLTK's default German stopwords
default_stopwords = set(nltk.corpus.stopwords.words('german'))

# We're adding some on our own - could be done inline like this...
# custom_stopwords = set((u'–', u'dass', u'mehr'))
# ... but let's read them from a file instead (one stopword per line, UTF-8)
stopwords_file = './stopwords.txt'
custom_stopwords = set(codecs.open(stopwords_file, 'r', 'utf-8').read().splitlines())

all_stopwords = default_stopwords | custom_stopwords

# Prepare output file
outfile = codecs.open(out_filename, 'w', 'utf-8')

# Prepare to test for all punctuation
# http://stackoverflow.com/questions/21696649/filtering-out-strings-that-only-contains-digits-and-or-punctuation-python
puncs = set(string.punctuation)

# See how many files total
n_files = len(glob.glob(os.path.join( data_dir, '*' + data_file_ending )))

for ii, input_path in enumerate(glob.iglob(os.path.join( data_dir, '*' + data_file_ending ))):

    if ii%100 == 0:
        print( u"{} / {}".format(ii, n_files) )
        
	# Read file
    fp = codecs.open(input_path, 'r', 'utf-8')
    input_filename = os.path.basename( input_path )
    
    words = nltk.word_tokenize(fp.read())

    # Remove single-character tokens (mostly punctuation)
    words = [word for word in words if len(word) > 2]

    # Remove numbers
    words = [word for word in words if not all(word.isdigit() or c in puncs for c in word)]

    # Lowercase all words (default_stopwords are lowercase too)
    words = [word.lower() for word in words]

    # Stemming words seems to make matters worse, disabled
    # stemmer = nltk.stem.snowball.SnowballStemmer('german')
    # words = [stemmer.stem(word) for word in words]

    # Remove stopwords
    words = [word for word in words if word not in all_stopwords]

    # Calculate frequency distribution
    fdist = nltk.FreqDist(words)

    # Output top 20 words
    # for word, frequency in fdist.most_common(20):
    #     print(u'{}\t{}\n'.format(word, frequency))

    # Write 
    for word, frequency in fdist.most_common(20):
        outfile.write(u'{}\t{}\t{}\n'.format(input_filename, word, frequency))

outfile.close()
#!/usr/bin/env python
# coding=UTF-8

import codecs
import glob
import os
import string

data_dir = '/Volumes/Data/LibraryVis/PDFs_TOC/txt_german'
data_file_ending = '_ocr_ger.txt'
out_filename = 'docs_single_file_20170214.txt'

# Prepare output file
outfile = codecs.open(out_filename, 'w', 'utf-8')

# See how many files total
n_files = len(glob.glob(os.path.join( data_dir, '*' + data_file_ending )))

for ii, input_path in enumerate(glob.iglob(os.path.join( data_dir, '*' + data_file_ending ))):

    if ii%100 == 0:
        print( u"{} / {}".format(ii, n_files) )
        
	# Read file
    fp = codecs.open(input_path, 'r', 'utf-8')
    input_filename = os.path.basename( input_path )
    doc_string = fp.read()
    
    # Remove carriage returns and newlines
    doc_string.replace('\n', ' ')
    doc_string.replace('\r', ' ')

    # Write 
    outfile.write(u'{}\t{}\t{}\n'.format(input_filename, doc_string))

outfile.close()
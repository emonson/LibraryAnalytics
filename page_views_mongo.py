#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#			 http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reference command-line example for Google Analytics Core Reporting API v3.

This application demonstrates how to use the python client library to access
all the pieces of data returned by the Google Analytics Core Reporting API v3.

The application manages autorization by saving an OAuth2.0 token in a local
file and reusing the token for subsequent requests.

Before You Begin:

Update the client_secrets.json file

	You must update the clients_secrets.json file with a client id, client
	secret, and the redirect uri. You get these values by creating a new project
	in the Google APIs console and registering for OAuth2.0 for installed
	applications: https://code.google.com/apis/console

	Learn more about registering your analytics application here:
	http://code.google.com/apis/analytics/docs/gdata/v3/gdataAuthorization.html

Supply your TABLE_ID

	You will also need to identify from which profile to access data by
	specifying the TABLE_ID constant below. This value is of the form: ga:xxxx
	where xxxx is the profile ID. You can get the profile ID by either querying
	the Management API or by looking it up in the account settings of the
	Google Anlaytics web interface.

Sample Usage:

	$ python core_reporting_v3_reference.py

Also you can also get help on all the command-line flags the program
understands by running:

	$ python core_reporting_v3_reference.py --help
"""

__author__ = 'api.nickm@gmail.com (Nick Mihailovski)'

import sys
import re
import sample_utils
import urlparse as UP
import item_view_xml
import oclc_classify_xml
from pymongo import Connection
from datetime import datetime, timedelta

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError


# The table ID is used to identify from which Google Anlaytics profile
# to retrieve data. This ID is in the format ga:xxxx where xxxx is the
# profile ID.
TABLE_ID = 'ga:47958423'

# Pattern match for Library of Congress Category code
lcc_re = re.compile(r'([A-Z]{1,3})[0-9]{1,4}')

# Make a connection to Mongo.
try:
		db_conn = Connection()
		# db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
		print "couldn't connect: be sure that Mongo is running on localhost:27017"
		sys.exit(1)

db = db_conn['library_vis']

def main(argv):
	sample_utils.process_flags(argv)

	# Authenticate and construct service.
	service = sample_utils.initialize_service()

	# Try to make a request to the API. Print the results or handle errors.
	try:
		# Limited to seven dimensions, so excluding ga:date since searching over one date
		#   at a time, anyway...
		metrics = 'ga:visitors'
		dimensions = 'ga:pagePath,ga:hour,ga:city,ga:region,ga:country,ga:latitude,ga:longitude'
		start_date_str = '2013-04-01'
		end_date_str = '2013-04-03'
		start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
		end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
		date_delta = end_date - start_date
		delta_days = date_delta.days + 1
		one_day = timedelta(days = 1)
		
		if delta_days > 0:
			for dd in range(delta_days):
				date = start_date + dd*one_day
				date_str = date.strftime('%Y-%m-%d')
				results = get_api_query(service, date_str, metrics, dimensions).execute()
				
				# print_results(results)
				rows_to_mongo(results, date_str)
			

	except TypeError, error:
		# Handle errors in constructing a query.
		print ('There was an error in constructing your query : %s' % error)

	except HttpError, error:
		# Handle API errors.
		print ('Arg, there was an API error : %s : %s' %
					 (error.resp.status, error._get_reason()))

	except AccessTokenRefreshError:
		# Handle Auth errors.
		print ('The credentials have been revoked or expired, please re-run '
					 'the application to re-authorize')


def get_api_query(service, date_str, mets, dims):
	"""Returns a query object to retrieve data from the Core Reporting API.

	Args:
		service: The service object built by the Google API Python client library.
	"""

# All pages returned
	return service.data().ga().get(
			ids=TABLE_ID,
			start_date=date_str,
			end_date=date_str,
			metrics=mets,
			filters='ga:pagePath=@?id=DUKE',
			dimensions=dims,
			max_results='10000')

# Ranked pages by number of views
#		return service.data().ga().get(
#				ids=TABLE_ID,
#				start_date='2012-01-01',
#				end_date='2012-04-17',
#				metrics='ga:pageviews',
#				filters='ga:pagePath=@?id=DUKE',
#				dimensions='ga:pagePath',
#				sort='-ga:pageviews',
#				max_results='50')

# Search terms for item view site...
#		return service.data().ga().get(
#				ids=TABLE_ID,
#				start_date='2012-01-01',
#				end_date='2012-04-17',
#				metrics='ga:pageviews',
#				dimensions='ga:searchKeyword',
#				sort='-ga:pageviews',
#				max_results='50')


def print_results(results):
	"""Prints all the results in the Core Reporting API Response.

	Args:
		results: The response returned from the Core Reporting API.
	"""

	print_report_info(results)
	print_pagination_info(results)
	print_profile_info(results)
	print_query(results)
	print_column_headers(results)
	print_totals_for_all_results(results)
	print_rows(results)


def print_report_info(results):
	"""Prints general information about this report.

	Args:
		results: The response returned from the Core Reporting API.
	"""

	print 'Report Infos:'
	print 'Contains Sampled Data = %s' % results.get('containsSampledData')
	print 'Kind									 = %s' % results.get('kind')
	print 'ID										 = %s' % results.get('id')
	print 'Self Link						 = %s' % results.get('selfLink')
	print


def print_pagination_info(results):
	"""Prints common pagination details.

	Args:
		results: The response returned from the Core Reporting API.
	"""

	print 'Pagination Infos:'
	print 'Items per page = %s' % results.get('itemsPerPage')
	print 'Total Results	= %s' % results.get('totalResults')

	# These only have values if other result pages exist.
	if results.get('previousLink'):
		print 'Previous Link	= %s' % results.get('previousLink')
	if results.get('nextLink'):
		print 'Next Link			= %s' % results.get('nextLink')
	print


def print_profile_info(results):
	"""Prints information about the profile.

	Args:
		results: The response returned from the Core Reporting API.
	"""

	print 'Profile Infos:'
	info = results.get('profileInfo')
	print 'Account Id			 = %s' % info.get('accountId')
	print 'Web Property Id = %s' % info.get('webPropertyId')
	print 'Profile Id			 = %s' % info.get('profileId')
	print 'Table Id				 = %s' % info.get('tableId')
	print 'Profile Name		 = %s' % info.get('profileName')
	print


def print_query(results):
	"""The query returns the original report query as a dict.

	Args:
		results: The response returned from the Core Reporting API.
	"""

	print 'Query Parameters:'
	query = results.get('query')
	for key, value in query.iteritems():
		print '%s = %s' % (key, value)
	print


def print_column_headers(results):
	"""Prints the information for each column.

	The main data from the API is returned as rows of data. The column
	headers describe the names and types of each column in rows.


	Args:
		results: The response returned from the Core Reporting API.
	"""

	print 'Column Headers:'
	headers = results.get('columnHeaders')
	for header in headers:
		# Print Dimension or Metric name.
		print '\t%s name:		 = %s' % (header.get('columnType').title(),
																	header.get('name'))
		print '\tColumn Type = %s' % header.get('columnType')
		print '\tData Type	 = %s' % header.get('dataType')
		print


def print_totals_for_all_results(results):
	"""Prints the total metric value for all pages the query matched.

	Args:
		results: The response returned from the Core Reporting API.
	"""

	print 'Total Metrics For All Results:'
	print 'This query returned %s rows.' % len(results.get('rows'))
	print ('But the query matched %s total results.' %
				 results.get('totalResults'))
	print 'Here are the metric totals for the matched total results.'
	totals = results.get('totalsForAllResults')

	for metric_name, metric_total in totals.iteritems():
		print 'Metric Name	= %s' % metric_name
		print 'Metric Total = %s' % metric_total
		print


def print_rows(results):
	"""Prints all the rows of data returned by the API.

	Args:
		results: The response returned from the Core Reporting API.
	"""

	print 'Rows:'
	if results.get('rows', []):
		for row in results.get('rows'):
			print '\t'.join(row)
	else:
		print 'No Rows Found'

def return_column_headers(results):
	"""Prints the information for each column.

	The main data from the API is returned as rows of data. The column
	headers describe the names and types of each column in rows.


	Args:
		results: The response returned from the Core Reporting API.
		
	Return:
		list of strings for column header names
	"""
	
	names = []
	headers = results.get('columnHeaders')
	
	for header in headers:
		# Print Dimension or Metric name.
		names.append(header.get('name').lstrip('ga:'))

	return names

def rows_to_mongo(results, date_str):
	"""Prints all the rows of data returned by the API.

	Args:
		results: The response returned from the Core Reporting API.
		filepath: The absolute path of the output file (CSV)
	"""

	if results.get('rows', []):		
		names = return_column_headers(results)
		
		# get ID from pagePath
		pgIdx = names.index('pagePath')
		total_count = len(results.get('rows'))

		for ii,row in enumerate(results.get('rows')):
			page_view = {}
			date_hour_str = date_str + row[names.index('hour')]
			timestamp = datetime.strptime(date_hour_str, '%Y-%m-%d%H')
			for jj in range(len(row)):
				if names[jj] == 'latitude' or names[jj] == 'longitude':
					if 'loc' not in page_view:
						# spherical geospatial search and GeoJSON assume [longitude,latitude] ordering
						# and two-element list is MongoDB's recommended format for 2d locations
						page_view['loc'] = [float(row[names.index('longitude')]), float(row[names.index('latitude')])]
				elif names[jj] == 'hour':
					page_view['timestamp'] = timestamp
				elif names[jj] == 'visitors':
					page_view[names[jj]] = int(row[jj])
				else:
					page_view[names[jj]] = row[jj]
			
			page_path = row[pgIdx]
			lcc = 'unknown'
			
			parsed_query = UP.parse_qs(UP.urlparse(page_path).query)
			
			# NOTE: Exclude any pages that were requested as xml format !!!
			if ('output-format' in parsed_query) and (parsed_query['output-format'] == 'xml'):
				continue
			if ('id' not in parsed_query):
				continue
			
			id_str = parsed_query['id'][0]
			page_view['uniqueid'] = id_str
			
			
			# First check if the item info is already in our own database
			item_obj = db.items.find_one({'uniqueid':id_str},{'call-number':True,'oclcnumber':True,'lcc':True})
			
			# If not in database, need to grab info from library item view xml
			if item_obj is None:
				item_obj = item_view_xml.get_xml_object(id_str)
				# Once in a while get back an empty object from library page
				if len(item_obj) == 0:
					# NOTE: Not counting page view event if can't find page item XML !!!
					continue
				else:
					# Store item object in database for future use
					db.items.save(item_obj)
			
			if 'lcc' in item_obj:
				# lcc gets added on later (not in originally returned xml)
				lcc = item_obj['lcc']
			else:
				if 'call-number' in item_obj:
					call_number = item_obj['call-number']
				
					# Multiple call-numbers separated by '|'
					first_call_number = call_number.split('|')[0]
					lcc = first_call_number.split(' ')[0]
				
					# If call number doesn't fit the right pattern
					#		then search the OCLC Classify service and pull the LCC category from there
					lcc_match = lcc_re.match(lcc)
					if lcc_match is None:
						if 'oclcnumber' in item_obj:
							oclc_str = item_obj['oclcnumber']
							lcc = oclc_classify_xml.get_lcc_class(oclc_str)
					
							# Test one more time and blank out if not right
							if lcc is None:
								lcc = 'unknown'
							else:
								lcc_match = lcc_re.match(lcc)
								if lcc_match is None:
									lcc = 'unknown'
						else:
							lcc = 'unknown'
				
				# no call number, either
				else:
					lcc = 'unknown'
			
				# Store in db for future use
				db.items.update({'uniqueid':id_str},{'$set':{'lcc':lcc}})
			
			print date_str, ' : ', ii, '/', total_count, lcc
			page_view['lcc'] = lcc
			lcc_match = lcc_re.match(lcc)
			if lcc_match is not None:
				page_view['lcc_category'] = lcc_match.group(1)
				page_view['lcc_first_letter'] = lcc_match.group(1)[:1]
			
			# Not creating own unique _id field, so check for date + hour & page match before saving
			pv_obj = db.pageviews.find_one({'uniqueid':id_str,'timestamp':timestamp},{'_id':True})
			if pv_obj is None:
				db.pageviews.save(page_view, safe=True)
	else:
		print 'No Rows Found'


if __name__ == '__main__':
	main(sys.argv)

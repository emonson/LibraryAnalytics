"""Hello Analytics Reporting API V4."""

import argparse

from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
CLIENT_SECRETS_PATH = 'client_secrets.json' # Path to client_secrets.json file.
VIEW_ID = 'ga:13688182'


def initialize_analyticsreporting():
  """Initializes the analyticsreporting service object.

  Returns:
    analytics an authorized analyticsreporting service object.
  """
  # Parse command-line arguments.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  # Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

  # Prepare credentials, and authorize HTTP object with them.
  # If the credentials don't exist or are invalid run through the native client
  # flow. The Storage object will ensure that if successful the good
  # credentials will get written back to a file.
  storage = file.Storage('analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())

  # Build the service object.
  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

  return analytics

def get_report(analytics):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'pageSize': 10000,
          'dateRanges': [{'startDate': '2016-07-20', 'endDate': 'today'}],
          'metrics': [{'expression': 'ga:pageviews'}],
          "dimensions": [{"name": "ga:pagePath"}, {"name": "ga:pageTitle"}],
          "dimensionFilterClauses": [
            {
              "operator": "AND",
              "filters": [
                {
                  "dimensionName": "ga:pagePath",
                  "operator": "BEGINS_WITH",
                  "expressions": ["/data/"]
                },
                # For now excluding search queries which include a ? in the pagePath
                {
                  "dimensionName": "ga:pagePath",
                  "not": "true",
                  "operator": "PARTIAL",
                  "expressions": ["?"]
                }
              ]
            }
          ],
          "orderBys": [
            {"fieldName": "ga:pageviews", "sortOrder": "DESCENDING"},
            {"fieldName": "ga:pagePath"}
          ],
        }]
      }
  ).execute()


def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response"""

  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    print dimensionHeaders
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    print metricHeaders
    print
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        print header + ': ' + dimension

      for i, values in enumerate(dateRangeValues):
        # print 'Date range (' + str(i) + ')'
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print metricHeader.get('name') + ': ' + value
      print
    print "Total rows returned:", len(rows)
    
def gather_table(response):
  """Parses the Analytics Reporting API V4 response
     and creates a list of dicts which will be put into a pandas DataFrame
  """
  # NOTE: Probably not dealing with multiple "reports" properly right now...
  rows_list = []
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
      row_dict = {}
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])
      num_date_ranges = len(dateRangeValues)

      for header, dimension in zip(dimensionHeaders, dimensions):
        row_dict[header[3:]] = dimension

      for i, values in enumerate(dateRangeValues):
        # print 'Date range (' + str(i) + ')'
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          if num_date_ranges == 1:
            row_dict[metricHeader.get('name')[3:]] = value
          else:
            row_dict[metricHeader.get('name')[3:]+'_'+str(i)] = value
      
      rows_list.append(row_dict)

    return pd.DataFrame(rows_list)
    
def main():

  analytics = initialize_analyticsreporting()
  response = get_report(analytics)
  # print_response(response)
  response_table = gather_table(response)
  response_table.to_csv('page_views_DVS.csv', encoding='utf-8', index=False)

if __name__ == '__main__':
  main()

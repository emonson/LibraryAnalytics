import pymarc
import os
import pandas as pd

data_dir = '/Volumes/Data/LibraryVis'
out_file = 'TOC_800s_subject.csv'

# For our 5.8 Gb MARCXML file, this takes a long time (but < 1.5 hrs) and 32 Gb RAM...
records = pymarc.parse_xml_to_array(os.path.join(data_dir, 'TOC_2000bis2016.mrc.xml'))

titles = [r.title() for r in records]

# Not going with int() because some have things like 'c 2005'
years = [r.pubyear() for r in records]

# Only really need part of the dewey field
deweys = [r.get_fields('082')[0].get_subfields('a')[0] if '082' in r else '' for r in records]

# Getting rid of extra author info don't need right now
authors = ["|".join(r['100'].get_subfields('a','d')) if '100' in r else '' for r in records]

# MARC records
# tag 082 - Dewey Decimal Classification Number
# tag 856 - Electronic Location and Access

# TOC URLs
# e.g.
# {'856': {'ind1': u'4',
#     'ind2': u'2',
#     'subfields': [{u'm': u'V:DE-605'},
#      {u'q': u'application/pdf'},
#      {u'u': u'http://d-nb.info/95960460X/04'},
#      {u'3': u'Inhaltsverzeichnis'}]}},

# Sometimes 856 field contains more than one URL, so only want the TOC one
urls = []
for r in records:
    f = r.get_fields('856')
    url = ''
    for ff in f:
        if '3' in ff and ff.get_subfields('3')[0] == 'Inhaltsverzeichnis':
            url = ff.get_subfields('u')[0]
    urls.append(url)

# Extra information about subjects and contents in 653 sections
subjects = ["|".join([f.value() for f in r.get_fields('653')]) if '653' in r else '' for r in records]

df = pd.DataFrame({'title':titles, 'author':authors, 'year':years, 'dewey':deweys, 'url':urls, 'subject':subjects})

# Only 800s out to CSV
ee = df[df.dewey.str.startswith('8')]
ee.to_csv(out_file, index=False, encoding='utf-8')
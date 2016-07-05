# Concatenate retractions workbooks
# http://stackoverflow.com/questions/32805456/pandas-combine-multiple-excel-worksheets-on-specific-index

import pandas as pd
import os
import xlrd

data_dir = '/Users/emonson/Dropbox/People/CiaraHealy'
in_file = 'Retractions_20160629.xlsx'
out_file = 'Retractions_concat_20160705.txt'

list_dfs = []
in_path = os.path.join(data_dir, in_file)

xls = xlrd.open_workbook(in_path, on_demand=True)
total_sheets = len(xls.sheet_names())

for ii, sheet_name in enumerate(xls.sheet_names()):
    print ii+1, '/', total_sheets, ':', sheet_name
    df = pd.read_excel(in_path, sheet_name)
    df['origin_db'] = sheet_name
    list_dfs.append(df)

# Take a look at what fields are present in each database
print 'looking at fields...'
fields = {}
for df in list_dfs:
    for field in df.columns:
        if field not in fields:
            fields[field] = []
        fields[field].append(df.origin_db[0])

# print sorted([(k,len(v)) for k,v in fields.items()], key=lambda x: x[1], reverse=True)

# Make a table with ones where jounal has field
fieldsDF = pd.DataFrame( dict([(k, pd.Series( [1]*len(v), index=v )) for k,v in fields.items()]) ).transpose()
fieldsDF.fillna(value=0, inplace=True)
# print fieldsDF.sum(axis=1).sort_values(ascending=False)
fieldsDF.to_csv(os.path.join(data_dir, 'fields.txt'), sep='\t', encoding='utf-8')

print 'concatenating...'
dfs = pd.concat(list_dfs, axis=0)
print 'saving text output...'
dfs.to_csv(os.path.join(data_dir, out_file), sep='\t', encoding='utf-8')
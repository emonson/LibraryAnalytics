import os
import shutil
import xlrd
import pandas as pd

data_base_dir = '/Users/emonson/Dropbox/People/Library/ECL Stats'
orig_data_dir = 'Lilly stats'
new_data_dir = 'Lilly_date_names'

orig_data_path = os.path.join(data_base_dir, orig_data_dir)
new_data_path = os.path.join(data_base_dir, new_data_dir)
fixed_data_path = os.path.join(data_base_dir, fixed_data_dir)

paths = []
cat_idxs = []
months = {'Jan':'01', 'Feb':'02', 'Mar':'03', 
          'Apr':'04', 'May':'05', 'Jun':'06', 
          'Jul':'07', 'Aug':'08', 'Sep':'09', 
          'Oct':'10', 'Nov':'11', 'Dec':'12'}

# Place to put new renamed files with good date names
if not os.path.isdir( new_data_path ):
    os.mkdir( new_data_path )
    
# os.walk gives back a generator of tuples for each directory it walks
# ('current_dir', [directories], [files]), (...)walk = os.walk('.')
walk = os.walk( orig_data_path )
years = walk.next()[1]

# Walk through Excel files in year subdirectories
# and save a new copy which has a clean filename
for year in years:
    file_list = walk.next()[2]
    if not year.startswith('20'): 
        continue
    for filename in file_list:
        name, ext = os.path.splitext(filename)
        if filename[:3] in months and ext[:4] == '.xls':
            new_file_name = 'LillyStats_' + year + months[filename[:3]] + ext
            orig_path = os.path.join(orig_data_path, year, filename)
            dest_path = os.path.join(new_data_path, new_file_name)
            shutil.copy(orig_path, dest_path)

# Gather information about original column names
os.chdir(new_data_path)
renamed_walk = os.walk( new_data_path )
new_files = renamed_walk.next()[2]

column_names_dict = {}

for new_file in new_files:
    book = xlrd.open_workbook(os.path.abspath(new_file))
    sh = book.sheet_by_index(0)
    sh0 = sh.row_values(0)
    column_names_dict[new_file] = sh0

# orient='index' makes rows, which allows different length lists
# transpose() converts to desired columns with file names as headers and NA in extra cells
# column_names_df = pd.DataFrame.from_dict(column_names_dict, orient='index').transpose()
column_names_df = pd.DataFrame.from_dict(column_names_dict, orient='index')
# just replacing NAs with empty strings so easier to look at...
# column_names_df.fillna(value='', inplace=True)

# Sort according to filename
column_names_df.sort_index(inplace=True)

column_names_df.to_csv(os.path.join(data_base_dir, 'orig_column_names.csv'))


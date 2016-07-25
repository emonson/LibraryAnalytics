import os
import pandas as pd

# Testing concatenation on LillyStats_201008 to 201302

data_base_dir = '/Users/emonson/Dropbox/People/Library/ECL Stats'
# This directory contains only files which basically match up on columns
fixed_data_dir = 'Lilly_concat_mid'
out_file = 'LillyStats_midConcat.xlsx'

fixed_data_path = os.path.join(data_base_dir, fixed_data_dir)
column_names = ["date", "from_mail", "ILL_requests", "hold_requests", "recalls", "email", "reply_email", "delivery"]
df_list = []

walk = os.walk( fixed_data_path )
files = walk.next()[2]

for file in files:
    name, ext = os.path.splitext(file)
    if ext == '.xlsx':
        print file
        df = pd.read_excel(os.path.join(fixed_data_path, file))
        df.columns = column_names
        df_list.append(df)

df_all = pd.concat(df_list, axis=0, ignore_index=True)

# Controlling datetime output format so time won't be included
writer = pd.ExcelWriter(os.path.join(data_base_dir, out_file), datetime_format='yyyy-mm-dd')
df_all.to_excel(writer, 'Sheet1', index=False)
    
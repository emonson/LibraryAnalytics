import os
import pandas as pd

data_base_dir = '/Users/emonson/Dropbox/People/Library/ECL Stats'
new_data_dir = 'Lilly_date_names'
fixed_data_dir = 'Lilly_fixed_date'

new_data_path = os.path.join(data_base_dir, new_data_dir)
fixed_data_path = os.path.join(data_base_dir, fixed_data_dir)

# Place to put files with fixed dates
if not os.path.isdir( fixed_data_path ):
    os.mkdir( fixed_data_path )

# Fix dates of counts, which right now have correct month/day, but not correct year
# and get rid of last row, which should be "total"
# os.chdir(new_data_path)
new_walk = os.walk( new_data_path )
new_files = new_walk.next()[2]

for new_file in new_files:
    # File should end with _YYYYMM.xls
    print new_file
    name, ext = os.path.splitext(new_file)
    yeartxt = name[-6:-2]
    monthtxt = name[-2:]
    df = pd.read_excel(os.path.join(new_data_path, new_file))
    # If first column in Excel file doesn't have header/name, Pandas will make it an index
    # but this should be a date, so trying to fix
    if not df.index.is_integer():
        df.reset_index(inplace=True)
    # first column sometimes has year as header. change to 'date'
    c0 = df.columns[0]
    df.rename(columns={c0:'olddate'}, inplace=True)
    # get rid of total and blank rows
    df = df[pd.notnull(df.olddate) & (df.olddate.str.lower().str.strip().str[:5] != 'total')]
    # now fill in correct date
    sYear = pd.Series(yeartxt, index=df.index, name='year')
    sMonth = pd.Series(pd.DatetimeIndex(df.olddate).month, index=df.index, name='month')
    sDay = pd.Series(pd.DatetimeIndex(df.olddate).day, index=df.index, name='day')
    dfdate = pd.to_datetime(pd.concat([sYear, sMonth, sDay], axis=1))
    df.insert(0, 'date', pd.Series(dfdate, index=df.index))
    del df['olddate']
    # Make sure it's saving in current Excel format
    if ext == '.xls':
        new_file = new_file + 'x'
    # Controlling datetime output format so time won't be included
    writer = pd.ExcelWriter(os.path.join(fixed_data_path, new_file), datetime_format='yyyy-mm-dd')
    df.to_excel(writer, 'Sheet1', index=False)
    
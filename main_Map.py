import sys
import pandas as pd
import numpy as np
from pandas import DataFrame

###########################################################################################
# This program "transposes" or maps three years of trade data, from their original 2017 codes 
# to new 2022 codes using a correlation table and the raw datasets
# In addition, it uses a second algorithm to validate the results of the first
###########################################################################################

# Reads in correlation table
df_corr = pd.read_excel('Correlation_Table_AHTN_2017_2022.xlsx')

# Initializes mapping matrix with zeros, of dimensions = HS 2022 (rows = 11415) and
# HS 2017 codes (columns = 10813)
shape = (pd.Series(df_corr['AHTN 2022']).nunique(),
    pd.Series(df_corr['AHTN 2017']).nunique())
array_map = np.zeros(shape)

# Extracts (sorted) row and column names from correlation table and assigns to
# mapping matrix / mapping df
row_names = sorted(df_corr['AHTN 2022'].unique().tolist())
col_names = sorted(df_corr['AHTN 2017'].unique().tolist())
df_map = pd.DataFrame(data=array_map, index=row_names, columns=col_names)

# Step 1: Create the Flag column
df_corr['Flag'] = df_corr['Share'].apply(lambda x: 0 if x == 1 else 1)

# Step 2: Aggregate the AHTN 2022 values to ensure the Flag is 1 if any Flag is 1
aggregated_flags = df_corr.groupby('AHTN 2022')['Flag'].max().reset_index()

# Assigns shares from correlation table to appropriate row and column of mapping matrix
for i in range(len(df_corr)):
    row = df_corr.loc[i, 'AHTN 2022']
    col = df_corr.loc[i, 'AHTN 2017']
    df_map.loc[row, col] = df_corr.loc[i, 'Share']
    
# Check sum of weights
# print(sum(df_map.sum(axis=0))) # Test if = 10813, number of 2017 codes

# Creates an iterable dictionary of (year: i) pairs using dictionary comprehension
years = ['2019_M_Can', '2020_M_Can', '2021_M_Can']
year_indices = {year: i for i, year in enumerate(years)}

# Readies for matrix multiplication by defining numpy matrix of shares
# and initializing the trade vectors to zero
matrix_map = df_map.to_numpy()
vectors_data = np.zeros((len(col_names), len(years)))

# Reads in trade datasets, keeps useful columns and assigns 0 to missing values
datasets = ['CAM_Trade.xlsx', 'LDPR_Trade.xlsx', 'MYAN_Trade.xlsx', 'VN_Trade.xlsx']
for dataset in datasets:
    df_trade_data = pd.read_excel(dataset).sort_values('HS Code')
    columns_to_keep = ['HS Code', '2019_M_Can', '2020_M_Can', '2021_M_Can']
    df_trade_data = df_trade_data.loc[:, columns_to_keep]
    df_trade_data = df_trade_data.fillna(0)

    # Assign values to vectors based on year_indices
    for year in years:
        index = year_indices[year]
        vectors_data[:, index] = df_trade_data[year]

    # Performs matrix multiplication / mapping
    vectors_new = matrix_map.dot(vectors_data)

    # Converts vectors to dataframe with column row and column names
    cols = ['2019_M_Can', '2020_M_Can', '2021_M_Can']
    df_trade_new = pd.DataFrame(data = vectors_new, index = row_names, columns = cols)
    df_trade_new = df_trade_new.rename_axis('HS Code')

    # Checks column sums of transformed data against original data
    # test = df_trade_new[cols].sum() - df_trade_data[cols].sum()
    # if (test.sum() < 1e-6): print(dataset, ': Pass')

# The next lines of code calculate the new, 2022 edition data using a different algorithm

# Calculates "Share" column in df_corr by counting the number of each AHTN 2017 occurence, taking 
# reciprocal of that number, and broadcasting back to AHTN 2022 rows
df_corr['Share_calc'] = df_corr.groupby('AHTN 2017')['AHTN 2017'].transform('count').rdiv(1)

# Tests calculated shares ShareCalc against original Share in df_corr 
# test = df_corr['Share_calc'] - df_corr['Share']
# if (test.sum() < 1e-6): print('Shares pass')

# Merges shares with trade data
df_trade_temp = df_corr[['AHTN 2022', 'AHTN 2017', 'Share_calc']].merge(
    df_trade_data, how='left', left_on='AHTN 2017', right_on='HS Code')

# Drops 'HS Code'column
df_trade_temp = df_trade_temp.drop(columns=['HS Code'])

# Calculates new trade columns by multiplying 2017 edition trade numbers by new calculated share
df_trade_temp['2019_M'] = df_trade_temp['2019_M_Can'] * df_trade_temp['Share_calc']
df_trade_temp['2020_M'] = df_trade_temp['2020_M_Can'] * df_trade_temp['Share_calc']
df_trade_temp['2021_M'] = df_trade_temp['2021_M_Can'] * df_trade_temp['Share_calc']

# Rolls up trade numbers into 2022 edition codes and assigns to new dataframe
df_trade_calc = df_trade_temp.groupby('AHTN 2022').agg({
    '2019_M': 'sum',
    '2020_M': 'sum',
    '2021_M': 'sum'
}).reset_index()

# Drops 'HS Code'column to enable comparison of datasets
df_trade_calc = df_trade_calc.drop(columns=['AHTN 2022'])

# Checks if the dataframes are equal within a tolerance
are_equal = np.allclose(df_trade_calc.values, df_trade_new.values, atol=1e-2)
if are_equal:
    print('New and calculated data sets are equal')
else:
    print('New and calculated data sets are not equal')

# Step 4: Merge aggregated_flags with df_trade_new on their respective keys
df_trade_new_merged = df_trade_new.merge(aggregated_flags, left_on='HS Code', right_on='AHTN 2022', how='left')

# Step 5: Drop the 'AHTN 2022' column as it's no longer needed
df_trade_new_merged.drop(columns=['AHTN 2022'], inplace=True)

print(df_trade_new_merged.columns)

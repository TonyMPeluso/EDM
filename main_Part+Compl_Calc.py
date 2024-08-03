import sys
import pandas as pd
sys.path.append('RawData')

# Reads in datasets
datasets = ['CAM_Trade.xlsx', 'LDPR_Trade.xlsx', 'trade_data_Trade.xlsx', 'VN_Trade.xlsx']
for dataset in datasets:
    df_trade_data = pd.read_excel(dataset)
    df_trade_data = df_trade_data.sort_values('HS Code')

    # Removes "."from 8-digit AHTN codes, then converts to 2, 4 and 6 digits codes
    df_trade_data_split = df_trade_data['HS Code'].str.split('.')
    df_trade_data_split = pd.DataFrame(df_trade_data_split)
    df_trade_data['2dig'] = df_trade_data_split.apply(lambda row: row['HS Code'][0][:2], axis=1)
    df_trade_data['4dig'] = df_trade_data_split.apply(lambda row: row['HS Code'][0], axis=1)
    df_trade_data['6dig'] = df_trade_data_split.apply(lambda row: row['HS Code'][0] + row['HS Code'][1], axis=1)

    colsumsTot = df_trade_data.sum(axis = 0, skipna = True, numeric_only = True)

    # Reads in files of changed AHTN 6-digit codes that are completely (_compl) or
    # partially (_part) changed, and converts to integer filters for df_trade_data dataset
    pd_6Dig_compl = pd.read_excel('Cor6DigCompl.xlsx')*100
    list_6Dig_compl = pd_6Dig_compl['V2017'].astype(int).tolist()
    pd_6Dig_part = pd.read_excel('Cor6DigPart.xlsx')*100
    list_6Dig_part = pd_6Dig_part['V2017'].astype(int).tolist()

    # Converts panda colum 6dig to integer
    df_trade_data['6dig'] = df_trade_data['6dig'].astype(int)

    # Applies filters for completely and partially changed codes, and sums over filtered rows
    filter_compl = df_trade_data['6dig'].isin(list_6Dig_compl)
    df_trade_data_6dig_compl = df_trade_data[filter_compl]
    colsums_compl = df_trade_data_6dig_compl .sum(axis = 0, skipna = True, numeric_only = True)
    # print("colsums_compl\n", colsums_compl)
    # print("colsums_compl%\n", colsums_compl/colsumsTot)

    filter_part = df_trade_data['6dig'].isin(list_6Dig_part)
    df_trade_data_6dig_part = df_trade_data[filter_part]
    df_trade_data_6dig_part.to_excel("trade_dataOutput.xlsx")
    colsums_part = df_trade_data_6dig_part .sum(axis = 0, skipna = True, numeric_only = True)
    # print("colsums_part\n", colsums_part)
    # print("colsums_part%\n", colsums_part/colsumsTot)

    # Gets the 10 most important partials
    years = ['2019_M_Can', '2020_M_Can', '2021_M_Can']
    for year in years:
        df_trade_data_6dig_part_sort = df_trade_data_6dig_part.sort_values(by = year, ascending = False)
        df_top10 = df_trade_data_6dig_part_sort[['HS Code', year]].head(10)
        print(df_top10)

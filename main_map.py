import os
import pandas as pd
import numpy as np

# Set the working directory
os.chdir('/Users/anthonypeluso/VisualStudioProjects/PythonProject/EDM')

###########################################################################################
# This program "transposes" or maps three years of trade data, from their original 2017 codes 
# to new 2022 codes using a correlation table and the raw datasets.
###########################################################################################

# Function to preprocess trade data
def preprocess_trade_data(file_path, required_columns):
    """Read, filter, and preprocess trade data."""
    trade_data = pd.read_excel(file_path)
    trade_data['AHTN_code'] = trade_data['AHTN_code'].astype(str)
    trade_data = trade_data.sort_values('AHTN_code')
    filtered_data = trade_data.filter(items=required_columns)
    return filtered_data

# Function to validate columns
def validate_columns(df, required_columns):
    """Ensure required columns are present in the DataFrame."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

# Function to merge additional data
def merge_additional_data(trade_df, description_df, tariffs_df, aggregated_flags, chap_headers):
    """Merge trade data with descriptions, tariffs, flags, and chapter headers."""
    trade_df = pd.merge(trade_df, description_df, on='AHTN_code', how='left')
    trade_df = pd.merge(trade_df, tariffs_df, on='AHTN_code', how='left')
    trade_df = pd.merge(trade_df, aggregated_flags, on='AHTN_code', how='left')
    trade_df = pd.merge(trade_df, chap_headers, on=['Chap_code', 'Header_code'], how='left')
    return trade_df

# Reads in the correlation table
corr_df = pd.read_excel('Correlation_Table_AHTN_2017_2022.xlsx')

# Initializes mapping matrix with zeros
shape = (
    pd.Series(corr_df['AHTN 2022']).nunique(),
    pd.Series(corr_df['AHTN 2017']).nunique()
)
array_map = np.zeros(shape)

# Extracts row and column codes from correlation table
row_names = sorted(corr_df['AHTN 2022'].unique().tolist())
col_names = sorted(corr_df['AHTN 2017'].unique().tolist())
map_df = pd.DataFrame(data=array_map, index=row_names, columns=col_names)

# Fill mapping matrix
for i in range(len(corr_df)):
    row = corr_df.loc[i, 'AHTN 2022']
    col = corr_df.loc[i, 'AHTN 2017']
    map_df.loc[row, col] = corr_df.loc[i, 'Share']

# Create the Flag column for partially assigned commodities
corr_df['Flag'] = corr_df['Share'].apply(lambda x: 0 if x == 1 else 1)
aggregated_flags = corr_df.groupby('AHTN 2022')['Flag'].max().reset_index()
aggregated_flags = aggregated_flags.rename(columns={'AHTN 2022': 'AHTN_code'})

# Load chapter and header descriptions
chap_headers = pd.read_excel("Chap+Headers2022.xlsx", dtype={'Chap_code': str, 'Header_code': str})
chap_headers['Header_code'] = chap_headers['Header_code'].str.zfill(4)

# Specifies trade and metadata columns
trade_columns = [
    '2019_M_World', '2020_M_World', '2021_M_World',
    '2019_M_Can', '2020_M_Can', '2021_M_Can',
    '2019_M_Brun', '2020_M_Brun', '2021_M_Brun',
    '2019_M_Cam', '2020_M_Cam', '2021_M_Cam',
    '2019_M_Ind', '2020_M_Ind', '2021_M_Ind',
    '2019_M_LPDR', '2020_M_LPDR', '2021_M_LPDR',
    '2019_M_Mal', '2020_M_Mal', '2021_M_Mal',
    '2019_M_Myan', '2020_M_Myan', '2021_M_Myan',
    '2019_M_Phil', '2020_M_Phil', '2021_M_Phil',
    '2019_M_Sing', '2020_M_Sing', '2021_M_Sing',
    '2019_M_Thai', '2020_M_Thai', '2021_M_Thai',
    '2019_M_VN', '2020_M_VN', '2021_M_VN'
]
metadata_columns = ['AHTN_code', 'AHTN_desc']
required_columns = metadata_columns + trade_columns

# Prepare the matrix for mapping
matrix_map = map_df.to_numpy()
print("Shape of matrix_map:", matrix_map.shape)

# Process each dataset
datasets = ['CAM_Trade.xlsx', 'LPDR_Trade.xlsx', 'MYAN_Trade.xlsx', 'VN_Trade.xlsx']
for dataset in datasets:
    dataset_name = dataset.split('_')[0]

    # Preprocess the trade data
    trade_data_df = preprocess_trade_data(dataset, required_columns)

    # Validate columns
    validate_columns(trade_data_df, required_columns)

    # Prepare trade vectors for multiplication
    vectors_data = trade_data_df[trade_columns].fillna(0).to_numpy()
    vectors_new = matrix_map.dot(vectors_data)

    # Create a DataFrame for the transformed data
    trade_new_df = pd.DataFrame(data=vectors_new, index=row_names, columns=trade_columns)
    trade_new_df = trade_new_df.rename_axis('AHTN_code').reset_index()

    # Save AHTN descriptions
    AHTN_description = trade_data_df[['AHTN_code', 'AHTN_desc']]

    # Add chapter and header codes to the transformed data
    trade_new_df['Chap_code'] = trade_new_df['AHTN_code'].str[:2].str.zfill(2)
    trade_new_df['Header_code'] = trade_new_df['AHTN_code'].str[:4].str.zfill(4)

    # Load tariffs
    excel_file = f"{dataset_name}_2023CodesClean.xlsx"
    tariffs_df = pd.read_excel(excel_file, dtype={"AHTN_code": str})
    tariffs_df = tariffs_df[['AHTN_code', 'MFN_2023', 'MFN_2019']]

    # Merge additional data
    trade_merged_df = merge_additional_data(trade_new_df, AHTN_description, tariffs_df, aggregated_flags, chap_headers)

    # Specify final column order
    complete_cols = ['AHTN_code', 'MFN_2023', 'MFN_2019', 'Chap_desc', 'Header_desc', 'AHTN_desc',
                      *trade_columns, 'Flag']

    # Ensure column order
    trade_merged_df = trade_merged_df[complete_cols]

    # Save the merged DataFrame
    output_filename = f"{dataset_name}_Merged.xlsx"
    trade_merged_df.to_excel(output_filename, index=False)
    print(f"Saved merged data for {dataset_name}: {output_filename}")

    # Compare new and test methods
    trade_test_df = corr_df[['AHTN 2022', 'AHTN 2017', 'Share']].merge(
        trade_data_df, how='left', left_on='AHTN 2017', right_on='AHTN_code'
    )

    for year in trade_columns:
        trade_test_df[year] = trade_test_df[year] * trade_test_df['Share']

    trade_test_df = trade_test_df.groupby('AHTN 2022').agg({year: 'sum' for year in trade_columns}).reset_index()
    trade_test_df = trade_test_df.rename(columns={'AHTN 2022': 'AHTN_code'})

    # Compare the two methods
    values_new = trade_new_df[trade_columns].values
    values_test = trade_test_df[trade_columns].values
    are_equal = np.allclose(values_new, values_test, atol=1e-2)

    if are_equal:
        print(f"{dataset_name}: New and test methods are equal.")
    else:
        print(f"{dataset_name}: Differences found between new and test methods.")
        differences = np.abs(values_new - values_test)
        print(f"Maximum difference: {differences.max()}")

        # Save differences to Excel
        diff_df = pd.DataFrame(data=differences, index=row_names, columns=trade_columns)
        diff_output_filename = f"{dataset_name}_Differences.xlsx"
        with pd.ExcelWriter(diff_output_filename) as writer:
            trade_new_df.to_excel(writer, sheet_name='Trade_New', index=False)
            trade_test_df.to_excel(writer, sheet_name='Trade_Test', index=False)
            diff_df.to_excel(writer, sheet_name='Differences', index=True)
        print(f"Saved trade data and differences to {diff_output_filename}")

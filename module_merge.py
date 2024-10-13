import pandas as pd

def merge_data(trade_new_dict, code_files, aggregated_flags):
    """
    Merges each trade DataFrame with its corresponding code table based on 'AHTN_code',
    retains specified columns, filters rows with non-zero values in columns of interest, 
    adds a new column, and includes a flag column from the aggregated_flags DataFrame.

    Parameters:
    trade_new_dict (dict): Dictionary containing trade DataFrames (e.g., {'CAM_Can': df_CAM_Can, 'CAM_World': df_CAM_World, ...}).
    code_files (dict): Dictionary containing the paths to the Excel files for each code table.
    aggregated_flags (DataFrame): DataFrame containing 'AHTN_code' and 'Flag' columns for merging.
    
    Returns:
    dict: A dictionary containing filtered and merged DataFrames.
    """
    # Define columns to keep for both Can and World data after merging
    columns_to_keep_Can = ['AHTN_code', 'MCT Rate', 'AHTN_desc', '2019_M_Can', '2020_M_Can', '2021_M_Can']
    columns_to_keep_World = ['AHTN_code', 'MCT Rate', 'AHTN_desc', '2019_M_World', '2020_M_World', '2021_M_World']

    # Define columns of interest for filtering non-zero rows for both datasets
    columns_of_interest_Can = ['2019_M_Can', '2020_M_Can', '2021_M_Can']
    columns_of_interest_World = ['2019_M_World', '2020_M_World', '2021_M_World']

    # Initialize an empty dictionary to store the merged DataFrames
    trade_merged_dict = {}

    # Loop through each key in the trade dictionary
    for country_key, trade_df in trade_new_dict.items():
        # Determine if we're processing 'Can' or 'World' data
        if country_key.endswith('_Can'):
            columns_to_keep = columns_to_keep_Can
            columns_of_interest = columns_of_interest_Can
        elif country_key.endswith('_World'):
            columns_to_keep = columns_to_keep_World
            columns_of_interest = columns_of_interest_World
        else:
            print(f"Unexpected key format: {country_key}. Skipping.")
            continue
        
        # Extract the base country code (e.g., 'CAM') from the key
        country_code = country_key.split('_')[0]

        # Get the corresponding code file path from the code_files dictionary
        code_file = code_files.get(country_code)
        
        if not code_file:
            print(f"No code file found for {country_code}. Skipping.")
            continue

        # Read the code table into a DataFrame
        codes_df = pd.read_excel(code_file)

        # Merge the trade DataFrame with the code table on 'AHTN_code'
        merged_df = pd.merge(trade_df, codes_df, on='AHTN_code', how='left')

        # Merge the aggregated_flags DataFrame to include the flag column
        merged_df = pd.merge(merged_df, aggregated_flags[['AHTN_code', 'Flag']], on='AHTN_code', how='left')

        # Retain only the specified columns
        merged_df = merged_df[columns_to_keep + ['Flag']]

        # Condition: Retain rows where at least one of the specified columns is greater than 0
        condition = merged_df[columns_of_interest].gt(0).any(axis=1)
        df_filtered = merged_df[condition]

        # Store the filtered DataFrame in the trade_merged_dict using the full key (e.g., 'CAM_Can', 'CAM_World')
        trade_merged_dict[country_key] = df_filtered

        print(f"Merged, filtered, and processed data for {country_key}.")

    return trade_merged_dict

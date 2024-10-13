import pandas as pd

def output_data(trade_merged_dict, chap_headers_file):
    """
    Processes each country's DataFrame from trade_merged_dict, merges with chap_headers_df,
    saves the separate outputs (_Can, _World), and also produces a combined output.

    Parameters:
    trade_merged_dict (dict): Dictionary containing DataFrames with merged trade data.
    chap_headers_file (str): Path to the 'Chap+Headers2022.xlsx' file.

    Returns:
    None
    """
    # Load the chapter headers data
    chap_headers_df = pd.read_excel(chap_headers_file, dtype={'Header_code': str})
    chap_headers_df['Header_code'] = chap_headers_df['Header_code'].str.zfill(4)

    # Loop through each key in the trade_merged_dict
    for country_key, merged_df in trade_merged_dict.items():
        print(f"Processing data for {country_key}...")

        # Determine if we're processing 'Can' or 'World' data and adjust columns accordingly
        if '_Can' in country_key:
            output_columns = ['AHTN_code', 'MCT Rate', 'Chap_desc', 'Header_desc', 'AHTN_desc',
                              '2019_M_Can', '2020_M_Can', '2021_M_Can', 'Flag']
            base_country_code = country_key.split('_')[0]
            output_file = f"{base_country_code}_Can_Output.xlsx"
        elif '_World' in country_key:
            output_columns = ['AHTN_code', 'MCT Rate', 'Chap_desc', 'Header_desc', 'AHTN_desc',
                              '2019_M_World', '2020_M_World', '2021_M_World', 'Flag']
            base_country_code = country_key.split('_')[0]
            output_file = f"{base_country_code}_World_Output.xlsx"
        else:
            print(f"Unexpected key format: {country_key}. Skipping.")
            continue

        # Create a new column for the first 4 digits in the AHTN_code
        merged_df['Header_code'] = merged_df['AHTN_code'].str[:4].str.zfill(4)

        # Merge with the chapter headers DataFrame on 'Header_code'
        merged_with_headers = pd.merge(merged_df, chap_headers_df, on='Header_code', how='left')

        # Filter the columns to include only the required ones
        try:
            filtered_output = merged_with_headers[output_columns]
        except KeyError as e:
            print(f"KeyError while filtering columns for {country_key}: {e}")
            continue

        # Save the filtered output to an Excel file for _Can or _World
        filtered_output.to_excel(output_file, index=False)
        print(f"Output saved to {output_file}.")

    # Now produce the combined output for both _Can and _World data
    for base_country_code in set(key.split('_')[0] for key in trade_merged_dict if '_Can' in key):
        can_key = f"{base_country_code}_Can"
        world_key = f"{base_country_code}_World"

        can_df = trade_merged_dict.get(can_key)
        world_df = trade_merged_dict.get(world_key)

        if can_df is None or world_df is None:
            print(f"Missing data for {can_key} or {world_key}. Skipping combined output.")
            continue

        # Merge the 'Can' and 'World' DataFrames on 'AHTN_code'
        combined_df = pd.merge(
            can_df,
            world_df[['AHTN_code', '2019_M_World', '2020_M_World', '2021_M_World']],
            on='AHTN_code',
            how='left'
        )

        # Add the 'Header_code' for merging with the chapter headers
        combined_df['Header_code'] = combined_df['AHTN_code'].str[:4].str.zfill(4)
        combined_with_headers = pd.merge(combined_df, chap_headers_df, on='Header_code', how='left')

        # Define the output columns for the combined version
        combined_output_columns = [
            'AHTN_code', 'MCT Rate', 'AHTN_desc', '2019_M_Can', '2019_M_World',
            '2020_M_Can', '2020_M_World', '2021_M_Can', '2021_M_World'
        ]

        # Filter the columns for the combined output
        try:
            filtered_combined_output = combined_with_headers[combined_output_columns]
        except KeyError as e:
            print(f"KeyError while filtering combined columns for {base_country_code}: {e}")
            continue

        # Define the output file for the combined data
        combined_output_file = f"{base_country_code}_Combined_Output.xlsx"
        filtered_combined_output.to_excel(combined_output_file, index=False)
        print(f"Combined output saved to {combined_output_file}.")

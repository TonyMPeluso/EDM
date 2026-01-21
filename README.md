This repository contains Python code to "transpose" historical international merchandise trade data, from a 2017 edition of 
commodity code to a 2022 edition of the commodity code, as part of the Expert Deployment Mechanism for Trade & Development (EDM)

The main program, "main_map.py" uses historical (2019-2021) trade data for four ASEAN countries, together with a correlation table, all available separately.

More specifically, it reads in 
- A correlation table “Correlation_Table_AHTN_2017_2022.xlsx” having columns “AHTN 2017” and “AHTN 2022", that map the AHTN 2017 edition of commodity codes into the AHTN 2022 edition of commodity codes
- The data sets for the various countries contained in the Excel files “CAM_Trade.xlsx”, “LPDR_Trade.xlsx”, “MYAN_Trade.xlsx, and “VN_Trade.xlsx”, which have imports for the years 2019-2021 from each fellow ASEAN economy as well as from the World and Canada. This is the trade data being transposed.
- The Excel file “Chap+Headers2022.xlsx”, which contains the descriptions of each AHTN commodity, used for the final output file.

Two methods are used to transpose the data: one using matrix multiplication based on transforming the correlation table into a mapping matrix, the other using column manipulations. Both methods are used to cross-validate one another.

The other program, "main_Part+Compl_Calc.py", calculates column sums for historical imports whose 2017 AHTN commodity codes have been partially or completely reassigned to new 2022 commodity codes. This program helps to identify "problem" commodities, whose reassignment from a single AHTN 2017 commodity to multiply AHTN 2022 commodity codes presents a quality issue in interpreting the final table.

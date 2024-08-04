This repository contains Python code to "transpose" historical international merchandise trade data, from a 2017 edition of 
commodity code to a 2022 edition of the commodity code, as part of the Expert Deployment Mechanism for Trade & Development (EDM)

It uses historical (2019-2021) trade data for four countries, together with a correlation table, all available separately.

One file, main_Part+Compl_Calc.py, calculates column sums for commodities that have been either partially or completely  
reassigned to new 2021 commodity codes.

The other, main_Map, performs the actual transpositon and outputs the data to a template

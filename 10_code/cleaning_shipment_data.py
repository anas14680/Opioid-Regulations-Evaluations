# %%
import os

os.cpu_count()
os.chdir("/Users/mohammadanas/Desktop/Duke MIDS/Fall 2021/Practising Data Science/")
## SETTING UP A DASK Cluster
from dask.distributed import Client

client = Client(n_workers=6)
client


# %%
import dask.dataframe as dd
## Loading dataset with the right datatypes

shipment_data = dd.read_csv(
    "/Users/mohammadanas/Desktop/arcos_all_washpost.tsv",
    delimiter="\t",
    dtype={
        "REPORTER_ADDL_CO_INFO": str,
        "REPORTER_ADDRESS2": str,
        "ACTION_INDICATOR": str,
        "BUYER_ADDL_CO_INFO": str,
        "NDC_NO": str,
        "UNIT": str,
        "ORDER_FORM_NO": str,
        "BUYER_ZIP": float,
        "DRUG_CODE": float,
        "REPORTER_ZIP": float,
        "TRANSACTION_DATE": float,
        "TRANSACTION_ID": float,
        "BUYER_ADDRESS2": str,
    },
)


# %%
# filter out only relevant columns

shipment_data_rel = shipment_data.loc[
    :,
    [
        "REPORTER_STATE",
        "REPORTER_COUNTY",
        "BUYER_COUNTY",
        "BUYER_STATE",
        "TRANSACTION_DATE",
        "DOSAGE_UNIT",
        "MME_Conversion_Factor",
        "CALC_BASE_WT_IN_GM",
        "QUANTITY",
        "UNIT",
        "DRUG_NAME",
        "dos_str",
    ],
]
### Make it store this data to cache memory
shipment_data_rel.persist()


# %%
# check to see if there are any null values in the the two columns we'll use for our MME calculation
missingvalues = shipment_data_rel[shipment_data_rel["CALC_BASE_WT_IN_GM"].isna()]
missing_weight = missingvalues.compute()
missing_weight


# %%
# check for missing values in the MME Conversion Factor
Missing_MME_conversion = shipment_data_rel[
    shipment_data_rel["MME_Conversion_Factor"].isna()
]
missing_conversion = Missing_MME_conversion.compute()
missing_conversion

# no nulls in either, good to move on to the calculation


# %%
# check to see if there are any null values in county
missing_buyer_county = shipment_data_rel[shipment_data_rel["BUYER_COUNTY"].isna()]
# 2057 rows missing county
missing_county = missing_buyer_county.compute()
missing_county


# %%
# create a new column to change the date format and extract year
shipment_data_rel["DATE"] = dd.to_datetime(
    shipment_data_rel["TRANSACTION_DATE"], format="%m%d%Y"
)

shipment_data_rel["Year"] = shipment_data_rel["DATE"].dt.year


# convert weight in grams to milligrams
shipment_data_rel["CALC_BASE_WT_IN_MG"] = shipment_data_rel["CALC_BASE_WT_IN_GM"] * 1000

# calculation for Morphine Milligram Equivalent (MME)
shipment_data_rel["MME"] = (
    shipment_data_rel["CALC_BASE_WT_IN_MG"] * shipment_data_rel["MME_Conversion_Factor"]
)

# store to the cache memory
shipment_data_rel.persist()


# We remove Missing Buyer County columns
ship_no_na = shipment_data_rel[~shipment_data_rel["BUYER_COUNTY"].isna()]


ship_no_na.persist()
# group the data by state, county and year
shipment_grouped = (
    ship_no_na.groupby(["BUYER_STATE", "BUYER_COUNTY", "Year"])["MME"]
    .agg("sum")
    .reset_index()
)

shipment_data_cleaned = shipment_grouped.compute()


# %%
## We remove the state Alaska and store it to local folder
import pandas as pd

cleaned_shipment_final = pd.DataFrame(shipment_data_cleaned)
cleaned_shipment_final = cleaned_shipment_final[
    cleaned_shipment_final["BUYER_STATE"] != "AK"
]
cleaned_shipment_final.to_csv(
    "/Users/mohammadanas/Desktop/Duke MIDS/Fall 2021/Practising Data Science/pds2021-opioids-team-2-ids720/\
    20_intermediate_files/shipment_data_cleaned.csv",
    encoding="utf-8",
)
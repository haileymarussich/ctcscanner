# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 13:29:20 2022

@author: haile
"""

# Imports
from __future__ import print_function
import os.path
import os

import pandas as pd
pd.options.display.float_format = "{:,.2f}".format
from pandas.tseries.offsets import DateOffset
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
    )

import datetime
from datetime import timedelta

import streamlit as st
from streamlit_option_menu import option_menu
from st_aggrid import AgGrid

import numpy as np
import matplotlib.pyplot as plt

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account



# Streamlit config
st.set_page_config(layout="wide")

# Navigation
sidebar = st.sidebar.header('Navigation')
with st.sidebar:
    selected = option_menu(
        menu_title=None,
        options = ['Customer Search', 
                   'Aggregate Volumes', 
                   'Shared Wallet Scanner',
                   'OFAC Matches',
                   'Transaction Log', 'Customer Database', 'All Data'])
    


# Creating Google Sheets scopes
scope = ['https://www.googleapis.com/auth/spreadsheets']
service_account_file = 'keys.json'

credentials = None
credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scope)



# The ID and ranges of a sample spreadsheet
spreadsheet_id = '13xsnIBC0ftpS2rLKXgSbpMUztVadheV8AROnY1fiVFw'

tx_log_range = "TX_Log"
customer_database_range = "Customer_Database"
SAR_log_range = "SAR_Log"
BL_range = "BL_Addresses"



# Create the service and call it (TX_Log)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()
tx_result = sheet.values().get(spreadsheetId=spreadsheet_id, range=tx_log_range).execute()
tx_values = tx_result.get('values', [])

tx_rows = sheet.values().get(spreadsheetId=spreadsheet_id, range=tx_log_range).execute()
tx_data = tx_rows.get('values')
tx_log_df = pd.DataFrame(tx_data[1:], columns=tx_data[0])

# Datetime (TX_Log)
tx_log_df['TX_Date'] = pd.to_datetime(tx_log_df['TX_Date'])
tx_log_df['TX_Date'] = tx_log_df['TX_Date'].apply(lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))



# Create the service and call it (Customer_Database)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()
customer_result = sheet.values().get(spreadsheetId=spreadsheet_id, range=customer_database_range).execute()
customer_values = customer_result.get('values', [])

customer_rows = sheet.values().get(spreadsheetId=spreadsheet_id, range=customer_database_range).execute()
customer_data = customer_rows.get('values')
customer_database_df = pd.DataFrame(customer_data[1:], columns=customer_data[0])

# Datetime (Customer_Database)
customer_database_df['Date_Approved'] = pd.to_datetime(customer_database_df['Date_Approved']).dt.date
customer_database_df['Last_Review'] = pd.to_datetime(customer_database_df['Last_Review']).dt.date
customer_database_df['DOB'] = pd.to_datetime(customer_database_df['DOB'])



# Create the service and call it (SAR_Log)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()
SAR_result = sheet.values().get(spreadsheetId=spreadsheet_id, range=SAR_log_range).execute()
SAR_values = SAR_result.get('values', [])

SAR_rows = sheet.values().get(spreadsheetId=spreadsheet_id, range=SAR_log_range).execute()
SAR_data = SAR_rows.get('values')
SAR_log = pd.DataFrame(SAR_data[1:], columns=SAR_data[0])



# Create the service and call it (BL_Addresses)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()
BL_result = sheet.values().get(spreadsheetId=spreadsheet_id, range=BL_range).execute()
BL_values = BL_result.get('values', [])

BL_rows = sheet.values().get(spreadsheetId=spreadsheet_id, range=BL_range).execute()
BL_data = BL_rows.get('values')
BL_addresses = pd.DataFrame(BL_data[1:], columns=BL_data[0])



# Check DF info
print(customer_database_df.dtypes)
print(customer_database_df.shape)

print(tx_log_df.dtypes)
print(tx_log_df.shape)

print(SAR_log.dtypes)
print(SAR_log.shape)

print(BL_addresses.dtypes)
print(BL_addresses.shape)



# CONCAT SHEETS
concat_df1 = pd.merge(tx_log_df, customer_database_df, on = "ID", how = "outer", indicator="indicator_column")
concat_df = pd.merge(concat_df1, SAR_log, on = "SAR_ID", how = "outer", indicator="indicator_column2")

concat_df.columns = concat_df.columns.str.replace(' ', '')

concat_df["Control"] = pd.to_numeric(concat_df["Control"], errors="coerce")
concat_df = concat_df.sort_values(by='Control', ascending=True)

concat_df = concat_df.replace('', np.nan, regex=True)


# Date last transacted (Concat_Df)
concat_df['Last_TX'] = concat_df.groupby('ID_x')['TX_Date'].transform('max')
concat_df['Last_TX'] = pd.to_datetime(concat_df['Last_TX']).dt.date


# Aggregate tX totals (Concat_DF)
concat_df["Trade_Value"] = concat_df["Trade_Value"].str.replace(',','')
concat_df["Trade_Value"] = pd.to_numeric(concat_df["Trade_Value"], errors="coerce")
concat_df['Agg_Volume'] = concat_df.groupby('ID_x')['Trade_Value'].transform(sum)


# Shared wallets (Concat_DF) and new DF
shared_wallets_df = concat_df.groupby(['Address']).agg(
        Shared_Count = ('ID_x', 'nunique'), Shared_Identities = ('ID_x', 'unique'), Shared_Total = ('Trade_Value', 'sum'), Wallet_Last_TX = ('TX_Date', 'max'))
    
shared_wallets_df = shared_wallets_df.loc[(shared_wallets_df['Shared_Count'] > 1)]

shared_wallets_df['Wallet_Last_TX'] = pd.to_datetime(shared_wallets_df['Wallet_Last_TX']).dt.date
shared_wallets_df = shared_wallets_df.sort_values(by='Wallet_Last_TX', ascending=False)

concat_df = pd.merge(concat_df, shared_wallets_df, on = "Address", how = "outer")
concat_df['Wallet_Last_TX'] = pd.to_datetime(concat_df['Wallet_Last_TX']).dt.date


# Duplicate phone numbers (Concat_DF)
#duplicate_phone_df = concat_df.groupby(['Phone']).agg(
#        Count = ('ID_x', 'nunique'), Identities = ('ID_x', 'unique'), Phone_Shared_Total = ('Trade_Value', 'sum'), Phone_Last_TX = ('TX_Date', 'max'))
#
#duplicate_phone_df = duplicate_phone_df.loc[(duplicate_phone_df['Count'] > 1)]


# High-risk customers (Concat_DF)
concat_df['Age'] = (datetime.datetime.today()
            - pd.to_datetime(concat_df['DOB'])).astype('timedelta64[Y]')
concat_df.loc[(concat_df['Age'] > 60), 'Risk_Rating'] = "FEEVA"
concat_df['DOB'] = pd.to_datetime(concat_df['DOB']).dt.date

concat_df["Agg_Volume"] = pd.to_numeric(concat_df["Agg_Volume"])
concat_df['Percentile'] = concat_df.Agg_Volume.rank(pct = True)
concat_df.loc[concat_df['Percentile'] > .9, 'Risk_Rating'] = "High Volume"

concat_df.loc[concat_df['Company_Type'] == "Financial Institution", 'Risk_Rating'] = "Financial Institution"

# Screening result (Concat_DF)
#list_addresses = BL_addresses['Address'].tolist()
#bl_addresses_list = BL_addresses['Address'].unique()  
#concat_df['Address_Match'] = np.where(concat_df['Address'].isin(bl_addresses_list), "Yes", "No")

concat_df['Address'].fillna("Missing", inplace=True)
concat_df['Address_Match'] = concat_df.Address.isin(BL_addresses.Address)
concat_df.loc[concat_df['Address_Match'] == True, 'Risk_Rating'] = "Blacklisted Address"

concat_df.loc[concat_df['Shared_Count'] > 1, 'Risk_Rating'] = "Wallet Sharing"
high_risk = concat_df.Risk_Rating.unique()


# Add "dormant" calculation to Status column: > than 6 months with no volume (Concat_DF)
concat_df.loc[concat_df['Last_TX'] + DateOffset(months=6) < datetime.datetime.today(), 'Status'] = "Dormant"
status_options = concat_df.Risk_Rating.unique()


# Dateindex (Concat_DF)
dateindex_concat = concat_df.set_index('TX_Date') 


# Start and end date (Concat_DF)
min_start_date = dateindex_concat.index.min()
max_end_date = dateindex_concat.index.max()


# Review needed (Concat_DF)          
concat_df.loc[(concat_df['Risk_Rating'].isin(high_risk)) &
              (concat_df['Date_Approved'] + DateOffset(months=6) < max_end_date) & 
              (concat_df['Last_Review'] + DateOffset(months=6) < max_end_date), 
              'Review_Needed'] = 'Yes'

concat_df.loc[(concat_df['Status'] == "Dormant"), 
              'Review_Needed'] = 'Yes'
#concat_df['Review_Needed'].fillna(" ", inplace=True)


# Statements needed (Concat_DF)
concat_df.loc[(concat_df['Risk_Rating'].isin(high_risk)) &
              (concat_df['Age'] > 60) & 
              (concat_df['Agg_Volume'] > 50000) &
              (concat_df['Statements_Collected'] == "No"),
              'Statements_Needed'] = 'Yes'

concat_df.loc[(concat_df['Risk_Rating'].isin(high_risk)) &
              (concat_df['Age'] < 60) & 
              (concat_df['Agg_Volume'] > 100000) &
              (concat_df['Statements_Collected'] == "No"),
              'Statements_Needed'] = 'Yes'

concat_df['TX_Date'] = pd.to_datetime(concat_df['TX_Date']).dt.date



# Check DF info
print(concat_df.dtypes)
print(concat_df.shape)



# TRANSACTION LOG
if selected == 'Transaction Log':
# Tile and file names
    st.title('Transaction Log')
 
# Filter
    tx_log_df['TX_Date'] = pd.to_datetime(tx_log_df['TX_Date']).dt.date
    tx_log_df = tx_log_df.sort_values(['TX_Date', 'Control'], ascending=[True, True])
    tx_log_df.reset_index()
    tx_log_df.set_index('Control', inplace=True)
        
    st.write(tx_log_df.shape) 
    st.dataframe(customer_database_df)
    #AgGrid(tx_log_df, key = 'txs', editable = True, fit_columns_on_grid_load = True)
        
# Download as a csv
    @st.cache
    def convert_df_to_csv(df):
        return df.to_csv().encode('utf-8')
    st.download_button(
        label = 'Download as CSV',
        data=convert_df_to_csv(tx_log_df),
        file_name='transactions.csv',
        mime='text/csv',
        )   
        
        
                
# CUSTOMER DATABASE
if selected == 'Customer Database':
# Tile and file names
    st.title('Customer Database')

# Filter
    customer_database_df['DOB'] = pd.to_datetime(customer_database_df['DOB']).dt.date
    customer_database_df = customer_database_df.sort_values('Date_Approved', ascending=True)
    customer_database_df.set_index('ID', inplace=True)
            
    st.write(customer_database_df.shape)
    st.dataframe(customer_database_df)
    #AgGrid(customer_database_df, key = 'customers', editable = True, fit_columns_on_grid_load = True)
               
# Download as a csv
    @st.cache
    def convert_df_to_csv(df):
        return df.to_csv().encode('utf-8')
    st.download_button(
        label = 'Download as CSV',
        data=convert_df_to_csv(customer_database_df),
        file_name='all_customers',
        mime='text/csv',
        )
        

        
# AGGREGATE TOTALS
if selected == 'Aggregate Volumes':
# Tile and file names
    st.title('Aggregate Volumes')
        
# Frame
    agg_from_concat = concat_df[['Agg_Volume', 'Percentile', 'ID_x', 'Name_y', 'Company_Name', 'Last_TX', 'Status', 'Risk_Rating',
            'Statements_Needed', 'Statements_Collected',  'Review_Needed', 'Last_Review', 'Notes_y']]
                         
    agg_from_concat.drop_duplicates(subset=['ID_x'], inplace=True)
    agg_from_concat.set_index('ID_x', inplace=True)
        
# Print
    st.write(agg_from_concat.shape)  
    st.dataframe(agg_from_concat)

# Download as a csv
    @st.cache
    def convert_df_to_csv(df):
        return df.to_csv().encode('utf-8')
    st.download_button(
        label = 'Download as CSV',
        data=convert_df_to_csv(agg_from_concat),
        file_name='agg_volumes.csv',
        mime='text/csv',
        )
   
        
# SHARED WALLET
if selected == 'Shared Wallet Scanner':
# Tile and file names
    st.title('Shared Wallet Scanner')
        
# Print
    st.write(shared_wallets_df.shape)
    st.dataframe(shared_wallets_df)

# Download as a csv
    @st.cache
    def convert_df_to_csv(df):
        return df.to_csv().encode('utf-8')
    st.download_button(
        label = 'Download as CSV',
        data=convert_df_to_csv(shared_wallets_df),
        file_name='shared_wallets.csv',
        mime='text/csv',
        )



# OFAC MATCHES
if selected == 'OFAC Matches':
# Tile and file names
    st.title('OFAC Matches')
    
# Frame
    ofac_from_concat = concat_df[['Address', 'Address_Match', 'ID_x', 'Name_y', 'Company_Name', 'Last_TX', 'Status', 'Risk_Rating',
                                 'Notes_y']]
    ofac_from_concat.sort_values('Last_TX', ascending=True, inplace=True)
    ofac_from_concat.set_index('Address', inplace=True)
    
# Filter
    ofac_from_concat = ofac_from_concat.loc[(ofac_from_concat['Address_Match'] == True)]
    
    BL_addresses.drop_duplicates()
    address_list = BL_addresses.set_index('Address').to_dict()['Note']
    
# Print
    st.write(ofac_from_concat.shape)
    st.dataframe(ofac_from_concat)
    
# Download as a csv
    @st.cache
    def convert_df_to_csv(df):
        return df.to_csv().encode('utf-8')
    st.download_button(
        label = 'Download as CSV',
        data=convert_df_to_csv(ofac_from_concat),
        file_name='ofac_wallets.csv',
        mime='text/csv',
        )
    
# Show BL
    show_bl = st.checkbox("Show Blacklist")
    if show_bl:
        st.write(address_list)
    


# All
if selected == 'All Data':
    
# Tile and file names
    st.title('All')
    concat_df.drop(['indicator_column', 'indicator_column2'], axis=1, inplace=True)
    concat_df = concat_df.sort_values(by='Control', ascending=True)
    concat_df.set_index('Control', inplace=True)
          
# Define filter
    def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds a UI on top of a dataframe to let viewers filter columns

        Args:
        df (pd.DataFrame): Original dataframe

        Returns:
        pd.DataFrame: Filtered dataframe
        """
        modify = st.checkbox("Add filters", key = "modify")

        if not modify:
            return df
        df = df.copy()

        # Try to convert datetimes into a standard format (datetime, no timezone)
        for col in df.columns:
            if is_object_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception:
                    pass

            if is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        modification_container = st.container()

        with modification_container:
            to_filter_columns = st.multiselect("Filter On", df.columns, key = "to_filter_columns")
            for column in to_filter_columns:
                left, right = st.columns((1, 20))
                # Treat columns with < 10 unique values as categorical
                if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                    user_cat_input = right.multiselect(f"Values for {column}",
                                                           df[column].unique(),
                                                           default=list(df[column].unique()),
                                                           )
                    df = df[df[column].isin(user_cat_input)]
                elif is_numeric_dtype(df[column]):
                    _min = float(df[column].min())
                    _max = float(df[column].max())
                    step = (_max - _min) / 100
                    user_num_input = right.slider(f"Values for {column}",
                                                      min_value=_min,
                                                      max_value=_max,
                                                      value=(_min, _max),
                                                      step=step,
                                                      )
                    
                    df = df[df[column].between(*user_num_input)]
                elif is_datetime64_any_dtype(df[column]):
                    user_date_input = right.date_input(f"Values for {column}",
                                                           value=(
                                                               df[column].min(),
                                                               df[column].max(),
                                                               ),
                                                           )
                    if len(user_date_input) == 2:
                        user_date_input = tuple(map(pd.to_datetime, user_date_input))
                        start_date, end_date = user_date_input
                        df = df.loc[df[column].between(start_date, end_date)]
                else:
                    user_text_input = right.text_input(f"Substring or regex in {column}",
                                                           )
                    if user_text_input:
                        df = df[df[column].astype(str).str.contains(user_text_input)]


            check2 = st.checkbox("Remove Columns", key = "check2")

            if not check2:
                return df
            df = df.copy()

            modification_container2 = st.container()
            columns = list(df.columns.unique())

            with modification_container2:
                to_remove_columns = st.multiselect("Remove Columns", columns, default = columns, key = "to_remove_columns")
                df = df[to_remove_columns]

            return df
    
# Print
    df = concat_df
    df1 = filter_dataframe(df)
    st.write(df1.shape)
    st.dataframe(df1)

# Download as a csv
    @st.cache
    def convert_df_to_csv(df):
        return df.to_csv().encode('utf-8')
    st.download_button(
        label = 'Download as CSV',
        data=convert_df_to_csv(df1),
        file_name='custom_data_filters.csv',
        mime='text/csv',
        )               
            
 
    
# CUSTOMER SEARCH
if selected == 'Customer Search':
    
# Tile and file names
    st.title('Customer Search')
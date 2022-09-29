# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 13:29:20 2022

@author: haile
"""

# IMPORTS
from __future__ import print_function
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,)
import datetime
import streamlit as st
import numpy as np
import matplotlib.pyplot as plt
import gspread
import pickle
from pathlib import Path
import streamlit_authenticator as stauth
from streamlit_option_menu import option_menu

# GOOGLE API
SPREADSHEET_ID = "13xsnIBC0ftpS2rLKXgSbpMUztVadheV8AROnY1fiVFw"
GSHEET_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]

cred_file = "service_account.json"
gc = gspread.service_account(cred_file)

database = gc.open("Database")

sheet_names = ["TX_Log", "Customer_Database", "SAR_Log", 'BL_Addresses', 'TX_Log2']
list_wksts = database.worksheets()
tx_log_df1 = pd.DataFrame(database.worksheet("TX_Log").get_all_records())
customer_database_df = pd.DataFrame(database.worksheet("Customer_Database").get_all_records())
SAR_log = pd.DataFrame(database.worksheet("SAR_Log").get_all_records())
BL_addresses = pd.DataFrame(database.worksheet("BL_Addresses").get_all_records())
tx_log_df2 = pd.DataFrame(database.worksheet("TX_Log2").get_all_records())

# ST CONFIG
st.set_page_config(layout="wide")

# AUTHENTICATION
names = ["Glenn Hay-Roe", "Jesse Parsons", "Hailey Marussich"]
usernames = ["ghayroe", "jparsons", "hmarussich"]

file_path = Path(__file__).parent / "hashed_pw.pk1"
with file_path.open("rb") as file:
    hashed_passwords = pickle.load(file)
    
authenticator = stauth.Authenticate(names, usernames, hashed_passwords, "ctc_dashboard", "abcdef", cookie_expiry_days = 30)
name, authentication_status, username = authenticator.login("Login", "main")

if authentication_status == False:
    st.error("Username/password is incorrect.")
if authentication_status == None:
    st.warning("Please enter your username and password.")
if authentication_status:
    
# NAVIGATION
    st.sidebar.title(f"Welcome {name}")
    with st.sidebar:
        selected = option_menu(
            menu_title=None,
            options = ['Customer Search',
                       'Agg. Volumes', 
                       'Shared Wallets',
                       'Blacklisted Addresses',
                       'Transactions', 'Customer Database', 'All'])
    authenticator.logout("Logout", "sidebar")
    
# CONCAT AND CLEAN -- TX_LOG
    tx_log_df = pd.concat([tx_log_df1, tx_log_df2], ignore_index=True)
    tx_log_df.columns = tx_log_df.columns.str.strip()
    tx_log_df = tx_log_df.replace(["^\s*$"], np.nan, regex=True)
    
# DTYPES -- TX_LOG
    tx_log_df["Control"] = pd.to_numeric(tx_log_df["Control"], downcast='integer', errors="coerce")
    tx_log_df["Amount_Received"] = pd.to_numeric(tx_log_df["Amount_Received"], errors="coerce")
    tx_log_df["Amount_Sent"] = pd.to_numeric(tx_log_df["Amount_Sent"], errors="coerce")
    tx_log_df["Exchange_Rate"] = pd.to_numeric(tx_log_df["Exchange_Rate"], errors="coerce")
    tx_log_df["Trade_Value"] = pd.to_numeric(tx_log_df["Trade_Value"], errors="coerce")
    tx_log_df['Asset_Received'] = tx_log_df.Asset_Received.astype('category')
    tx_log_df['Received_At'] = tx_log_df.Received_At.astype('category')
    tx_log_df['Received'] = tx_log_df.Received.astype('category')
    tx_log_df['Asset_Sent'] = tx_log_df.Asset_Sent.astype('category')
    tx_log_df['Sent'] = tx_log_df.Sent.astype('category')
    tx_log_df['Inventory'] = tx_log_df.Inventory.astype('category')
    tx_log_df['TX_Date'] = pd.to_datetime(tx_log_df['TX_Date'])
    
# CLEAN -- CUSTOMER_DATABASE
    customer_database_df = customer_database_df.replace(["^\s*$"], np.nan, regex=True)
    
# DTYPES -- CUSTOMER_DATABASE
    customer_database_df['Company_Type'] = customer_database_df.Company_Type.astype('category')
    customer_database_df['State'] = customer_database_df.State.astype('category')
    customer_database_df['Statements_Collected'] = customer_database_df.Statements_Collected.astype('category')
    customer_database_df['Phone'] = customer_database_df['Phone'].values.astype(str)
    customer_database_df['Date_Approved'] = pd.to_datetime(customer_database_df['Date_Approved'])
    customer_database_df['Last_Review'] = pd.to_datetime(customer_database_df['Last_Review'])
    customer_database_df['DOB'] = pd.to_datetime(customer_database_df['DOB'])
    
# CLEAN -- SAR_LOG 
    SAR_log = SAR_log.replace(["^\s*$"], np.nan, regex=True)
    SAR_log = SAR_log.replace(np.nan, 0)
    
# DTYPES -- SAR_LOG
    SAR_log['SAR_Type'] = SAR_log.SAR_Type.astype('category')
    SAR_log["SAR_TX_Total"] = pd.to_numeric(SAR_log["SAR_TX_Total"], errors="coerce")
    SAR_log['BL'] = SAR_log.BL.astype('category')
    
# CLEAN -- BL_ADDRESSES 
    BL_addresses = BL_addresses.replace(["^\s*$"], np.nan, regex=True)
    
# CONCAT ALL DATA -- CONCAT_DF
    concat_df1 = pd.merge(tx_log_df, customer_database_df, on = "ID", how = "outer", indicator = "indicator_column")
    concat_df = pd.merge(concat_df1, SAR_log, on = "SAR_ID", how = "outer", indicator = "indicator_column2")
    
# CLEAN -- CONCAT_DF
    concat_df.columns = concat_df.columns.str.replace(' ', '')
    concat_df = concat_df.sort_values(by='Control', ascending=True)
     
# LAST_TX -- CONCAT_DF
    concat_df['Last_TX'] = concat_df.groupby('ID')['TX_Date'].transform('max')
    concat_df['Last_TX'] = pd.to_datetime(concat_df['Last_TX'])
    
# AGG_VOLUME -- CONCAT_DF
    concat_df["Trade_Value"] = concat_df.Trade_Value.astype(np.float64)
    concat_df['Agg_Volume'] = concat_df.groupby('ID')['Trade_Value'].transform(sum)
    
# AVG_VOLUME -- CONCAT_DF
    concat_df['Avg_Volume'] = concat_df.groupby('ID')['Trade_Value'].transform('mean')
    
# TX_COUNT -- CONCAT_DF
    concat_df['TX_Count'] = concat_df.groupby('ID')['Trade_Value'].transform('count')
    
# SHARED_WALLETS -- CONCAT_DF AND SHARED_WALLETS_DF
    shared_wallets_df = concat_df.groupby(['Address']).agg(
            Shared_Count = ('ID', 'nunique'),
            Shared_Identities = ('ID', 'unique'),
            Shared_Total = ('Trade_Value', 'sum'),
            Wallet_Last_TX = ('TX_Date', 'max'),
            Wallet_Notes_S = ('ID', 'unique'))
    shared_wallets_df = shared_wallets_df.loc[(shared_wallets_df['Shared_Count'] > 1)]
    
    shared_wallets_df['Wallet_Last_TX'] = pd.to_datetime(shared_wallets_df['Wallet_Last_TX']).dt.date
    shared_wallets_df = shared_wallets_df.sort_values(by='Wallet_Last_TX', ascending=False)
    
    concat_df = pd.merge(concat_df, shared_wallets_df, on = "Address", how = "outer")
    concat_df['Wallet_Last_TX'] = pd.to_datetime(concat_df['Wallet_Last_TX']).dt.date
    
# RISK_RATING -- CONCAT_DF
    concat_df['Age'] = (datetime.datetime.today() - pd.to_datetime(concat_df['DOB'])).astype('timedelta64[Y]')
    concat_df.loc[(concat_df['Age'] > 60), 'Risk_Rating'] = "High Risk"
    
    concat_df['Percentile'] = concat_df.Agg_Volume.rank(pct = True)
    
    concat_df.loc[(concat_df['Age'] > 60), 'Is_FEEVA'] = "Yes"
    concat_df['DOB'] = pd.to_datetime(concat_df['DOB']).dt.date
    
    concat_df.loc[concat_df['Percentile'] > .9, 'Risk_Rating'] = "High Risk"
    concat_df.loc[concat_df['Percentile'] > .9, 'Is_High_Volume'] = "Yes"
    
    concat_df.loc[concat_df['Company_Type'] == "Financial Institution", 'Risk_Rating'] = "High Risk"
    concat_df.loc[concat_df['Company_Type'] == "Financial Institution", 'Is_Financial_Inst'] = "Yes"
    
    concat_df['Address'].fillna("Missing", inplace=True)
    concat_df['Is_BL_Wallet'] = concat_df.Address.isin(BL_addresses.Address)
    concat_df.loc[concat_df['Is_BL_Wallet'] == True, 'Risk_Rating'] = "High Risk"
    concat_df.loc[concat_df['Is_BL_Wallet'] == True, 'Is_BL_Wallet'] = "Yes"
    
    concat_df.loc[concat_df['Shared_Count'] > 1, 'Risk_Rating'] = "High Risk"
    concat_df.loc[concat_df['Shared_Count'] > 1, 'Is_Shared_Wallet'] = "Yes"
    
    concat_df['Risk_Rating'] = concat_df.Risk_Rating.astype('category')
    concat_df['Is_FEEVA'] = concat_df.Is_FEEVA.astype('category')
    concat_df['Is_High_Volume'] = concat_df.Is_High_Volume.astype('category')
    concat_df['Is_Financial_Inst'] = concat_df.Is_Financial_Inst.astype('category')
    concat_df['Is_BL_Wallet'] = concat_df.Is_BL_Wallet.astype('category')
    concat_df['Is_Shared_Wallet'] = concat_df.Is_Shared_Wallet.astype('category')
    
# "DORMANT" STATUS -- CONCAT_DF
    concat_df['Months'] = 6
    concat_df['Last_Tx_Plus_6M'] = (concat_df['Last_TX'] + concat_df['Months'].values.astype("timedelta64[M]"))
    concat_df.loc[(concat_df['Last_Tx_Plus_6M'] < datetime.datetime.today(), 'Status')] = "Dormant"
    
    status_options = concat_df.Risk_Rating.unique()
    concat_df['Status'] = concat_df.Status.astype('category')
    
# DATEINDEX_CONCAT
    dateindex_concat = concat_df.set_index('TX_Date') 
# START AND END DATE
    min_start_date = dateindex_concat.index.min()
    max_end_date = dateindex_concat.index.max()
    
# REVIEW NEEDED -- CONCAT_DF 
    concat_df.loc[(concat_df['Risk_Rating'] == "High Risk") &
                  (concat_df['Date_Approved'] + concat_df['Months'].values.astype("timedelta64[M]") < max_end_date) & 
                  (concat_df['Last_Review'] + concat_df['Months'].values.astype("timedelta64[M]") < max_end_date), 
                  'Review_Needed'] = 'Yes'
    concat_df.loc[(concat_df['Status'] == "Dormant"), 'Review_Needed'] = 'Yes'
    concat_df['Review_Needed'] = concat_df.Review_Needed.astype('category')
    
# STATEMENTS_NEEDED -- CONCAT_DF
    concat_df.loc[(concat_df['Risk_Rating'] == "High Risk") &
                  (concat_df['Age'] > 60) & 
                  (concat_df['Agg_Volume'] > 50000) &
                  (concat_df['Statements_Collected'] == "No"),
                  'Statements_Needed'] = 'Yes'
    concat_df.loc[(concat_df['Risk_Rating'] == "High Risk") &
                  (concat_df['Age'] < 60) & 
                  (concat_df['Agg_Volume'] > 100000) &
                  (concat_df['Statements_Collected'] == "No"),
                  'Statements_Needed'] = 'Yes'
    
    concat_df['Statements_Needed'] = concat_df.Statements_Needed.astype('category')
    concat_df['TX_Date'] = pd.to_datetime(concat_df['TX_Date']).dt.date
    
    concat_df = concat_df.replace(["^\s*$"], np.nan, regex=True)
    
# PAGES -------------------------------------------------------------------------------------------------------------
        
# TRANSACTIONS -- PAGE
    if selected == 'Transactions':
# TILE AND FILE NAMES
        st.title('Transactions')
        
    #    control_new = int(tx_log_df['Control'].tail(1)+1)
    #    form = st.form(key="tx_input")
    #    with form:
    #        right, left = st.columns((1, 2))
    #        right.write(f"Control: {control_new}")
    #        date = left.date_input("Transaction Date:")
    #        cust_id = right.text_input("Customer ID:")
    #        cust_name = left.text_input("Customer Name:")
    #        comp_name = right.text_input("Company Name:")
    #        amount_rec = left.text_input("Amount Received:")
    #        asset_rec = right.text_input("Asset Received:")
    #        rec_at = left.text_input("Received At:")
    #        rec = right.selectbox("Received?", ["Yes", "No", " "])
    #        amount_sent = left.text_input("Amount Sent:")
    #        asset_rec = right.text_input("Asset Sent:")
    #        wallet = left.text_input("Wallet:")
    #        sent = right.selectbox("Sent?", ["Yes", "No", " "])
    #        exch_rate = left.text_input("Exchange Rate:")
    #        trade_value = right.text_input("Trade Value:")
    #        inventory = left.selectbox("Inventory:", ["I", " "])
    #        notes = right.text_input("Transaction Notes")
    #        submitted = st.form_submit_button(label="Submit")
    
# FILTER
        tx_log_df['TX_Date'] = pd.to_datetime(tx_log_df['TX_Date']).dt.date
        tx_log_df = tx_log_df.sort_values(['TX_Date', 'Control'], ascending=[True, True])
        tx_log_df.reset_index()
        tx_log_df.set_index('Control', inplace=True)
# PRINT
        st.write(tx_log_df.shape)
        test = tx_log_df.astype(str)
        st.dataframe(test)
        #AgGrid(tx_log_df, key = 'txs', editable = True, fit_columns_on_grid_load = True)
# DOWNLOAD
        @st.cache
        def convert_df_to_csv(df):
            return df.to_csv().encode('utf-8')
        st.download_button(
            label = 'Download as CSV',
            data=convert_df_to_csv(tx_log_df),
            file_name='transactions.csv',
            mime='text/csv',
            )
                    
# CUSTOMER DATABASE -- PAGE
    if selected == 'Customer Database':
# TILE AND FILE NAMES
        st.title('Customer Database')
        
    #    form = st.form(key="cust_input")
    #    with form:
    #        right, left = st.columns((1, 2))
    #        cust_name = right.text_input("Customer Name:")
    #        cust_id = left.text_input("Customer ID:")
    #        comp_name = right.text_input("Company Name:")
    #        comp_type = right.selectbox("Company Type", ["Entity", "Financial Institution", " "])
    #        state = left.text_input("State:")
    #        DOB = right.date_input("Date of Birth:")
    #        phone = left.text_input("Phone:")
    #        email = right.selectbox("Email")
    #        purpose = left.text_input("Purpose")
    #        source = right.text_input("Source")
    #        occupation = left.text_input("Occupation")
    #        ref = right.text_input("Referral")
    #        statements = left.selectbox("Statements Collected?", ["Yes", "No", " "])
    #        SAR_ID
    #        status = right.selectbox("Staus:", ["Pending Approval", "Under Review", "Approved", "SAR", " "])
    #        trade_value = left.date_input("Date Approved:")
    #        check = left.checkbox("Check if No Date")
    #        last_review = right.date_input("Last Review:")
    #        notes = left.text_input("Transaction Notes")
    #        submitted = st.form_submit_button(label="Submit")
        
# FILTER
        customer_database_df['DOB'] = pd.to_datetime(customer_database_df['DOB']).dt.date
        customer_database_df = customer_database_df.sort_values('Date_Approved', ascending=False)
        #customer_database_df.reset_index()
        customer_database_df.set_index('Date_Approved', inplace=True)
        
# PRINT
        st.write(customer_database_df.shape)
        test = customer_database_df.astype(str)
        st.dataframe(test)
        #AgGrid(customer_database_df, key = 'customers', editable = True, fit_columns_on_grid_load = True)          
# DOWNLOAD
        @st.cache
        def convert_df_to_csv(df):
            return df.to_csv().encode('utf-8')
        st.download_button(
            label = 'Download as CSV',
            data=convert_df_to_csv(customer_database_df),
            file_name='all_customers',
            mime='text/csv',
            )
                     
# AGGREGATE VOLUME -- PAGE
    if selected == 'Agg. Volumes':
# TILE AND FILE NAMES
        st.title('Agg. Volumes')        
# FRAME
        agg_from_concat = concat_df[['Agg_Volume', 'Percentile', 'Avg_Volume', 'ID', 'Name_C', 'Company_Name', 'Last_TX', 'Status', 'Risk_Rating',
                                     'Is_FEEVA', 'Is_High_Volume', 'Is_Financial_Inst', 'Is_BL_Wallet', 'Is_Shared_Wallet',
                                     'Statements_Needed', 'Review_Needed', 'Cust_Notes']]
# FILTER                         
        agg_from_concat.drop_duplicates(subset=['ID'], inplace=True)
        agg_from_concat.reset_index()
        agg_from_concat.set_index('Percentile', inplace=True)
        agg_from_concat = agg_from_concat.sort_values('Agg_Volume', ascending=False)
# PRINT
        st.write(agg_from_concat.shape)
        test = agg_from_concat.astype(str)
        st.dataframe(test)
# DOWNLOAD
        @st.cache
        def convert_df_to_csv(df):
            return df.to_csv().encode('utf-8')
        st.download_button(
            label = 'Download as CSV',
            data=convert_df_to_csv(agg_from_concat),
            file_name='agg_volumes.csv',
            mime='text/csv',
            )
       
# SHARED WALLETS -- PAGE
    if selected == 'Shared Wallets':
# TILE AND FILE NAMES
        st.title('Shared Wallets')       
# PRINT
        st.write(shared_wallets_df.shape)
        test = shared_wallets_df.astype("str")
        st.dataframe(test)
# DOWNLOAD
        @st.cache
        def convert_df_to_csv(df):
            return df.to_csv().encode('utf-8')
        st.download_button(
            label = 'Download as CSV',
            data=convert_df_to_csv(shared_wallets_df),
            file_name='shared_wallets.csv',
            mime='text/csv',
            )
    
# BLACKLISTED ADDRESSES
    if selected == 'Blacklisted Addresses':
# TILE AND FILE NAMES
        st.title('Blacklisted Addresses')   
# FRAME
        ofac_from_concat = concat_df[['Address', 'Is_BL_Wallet', 'ID', 'Name_C', 'Company_Name', 'Last_TX', 'Status', 'Cust_Notes']]
        ofac_from_concat.sort_values('Last_TX', ascending=False, inplace=True)
        ofac_from_concat.set_index('Address', inplace=True)    
# FILTER
        ofac_from_concat = ofac_from_concat.loc[(ofac_from_concat['Is_BL_Wallet'] == 'Yes')]
        
# PRINT
        st.write(ofac_from_concat.shape)
        test = ofac_from_concat.astype(str)
        st.dataframe(test) 
# DOWNLOAD
        @st.cache
        def convert_df_to_csv(df):
            return df.to_csv().encode('utf-8')
        st.download_button(
            label = 'Download as CSV',
            data=convert_df_to_csv(ofac_from_concat),
            file_name='ofac_wallets.csv',
            mime='text/csv',
            )    
# ADDRESS SEARCH
        BL_addresses.drop_duplicates()
        address_dict = BL_addresses.set_index('Address').to_dict()['Description']
    
        st.subheader("Screen for Matches")
        user_text_input = st.text_input("Enter a Wallet Address:", help("Note: check for spaces."))
        if user_text_input:
            dict_value = address_dict.get(user_text_input, 'No Key')
            if dict_value == 'No Key':
                st.success('No matches found for key: [ {0} ]'.format(user_text_input))
            else:
                st.error('key: [ {0} ] matches value: [ {1} ]'.format(user_text_input, dict_value))
# SHOW BL
        with st.expander("Show All Blacklisted Addresses"):
            BL_addresses.drop_duplicates()
            address_dict = BL_addresses.set_index('Address').to_dict()['Description']
            for key, val in address_dict.items():
                st.markdown(f'{key} : **{val}**')     
        
# ALL -- PAGE
    if selected == 'All':      
# TITLE AND FILE NAMES
        st.title('All')
        concat_df.drop(['indicator_column', 'indicator_column2', 'Months'], axis=1, inplace=True)
        concat_df = concat_df.sort_values(by='Control', ascending=True)
        concat_df.set_index('Control', inplace=True)           
# DEFINE DF
        def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:

            display = st.radio("Display", ("All Columns", "User Selected Columns"), key = "display")
            if display == "All Columns":
                df = df
            
            if display == "User Selected Columns":
                column_container = st.container()
                with column_container:
                        columns = list(df.columns.unique())
                        to_remove_columns = st.multiselect("Columns", columns, default = columns, key = "to_remove_columns")
                        df = df[to_remove_columns]
            
            modify = st.checkbox("Add Filters", key = "modify")
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
                columns2 = list(df.columns.unique())
                columns2.remove("Shared_Identities")
                columns2.remove("Wallet_Notes_S")
                to_filter_columns = st.multiselect("Filter By", columns2, key = "to_filter_columns") 
                for column in to_filter_columns:
                    left, right = st.columns((1, 16))
                    
                    # Treat columns with < 10 unique values as categorical
                    if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                        user_cat_input = right.multiselect(f"Categories in {column}",
                                                               df[column].unique(),
                                                               default=list(df[column].unique()))
                        df = df[df[column].isin(user_cat_input)] 
                        
                    elif is_numeric_dtype(df[column]):
                        _min = float(df[column].min())
                        _max = float(df[column].max())
                        step = ((_max - _min) / 100)
                        user_num_input = right.slider(f"Values for {column}",
                                                          min_value=_min,
                                                          max_value=_max,
                                                          value=(_min, _max),
                                                          step=step)
                        df = df[df[column].between(*user_num_input)]
                        
                    elif is_datetime64_any_dtype(df[column]):
                        user_date_input = right.date_input(f"Filter for {column}",
                                                               value=(
                                                                   df[column].min(),
                                                                   df[column].max()))
                        if len(user_date_input) == 2:
                            user_date_input = tuple(map(pd.to_datetime, user_date_input))
                            start_date, end_date = user_date_input
                            df = df.loc[df[column].between(start_date, end_date)]
                            
                    else:
                        user_text_input = right.text_input(f"Substring in {column}")
                        if user_text_input:
                            df = df[df[column].astype(str).str.contains(user_text_input)]
    
                return df
# PRINT
        df = concat_df
        df1 = filter_dataframe(df)
        for col in df1.columns:
            if is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col]).dt.date
            if is_numeric_dtype(df[col]):
                df[col] = df[col].round(2)
            
        st.write(df1.shape)
        test = df1.astype(str)
        st.dataframe(test)
# DOWNLOAD
        @st.cache
        def convert_df_to_csv(df):
            return df.to_csv().encode('utf-8')
        st.download_button(
            label = 'Download as CSV',
            data=convert_df_to_csv(df1),
            file_name='custom_data_filters.csv',
            mime='text/csv',
            )               
                
# CUSTOMER SEARCH -- PAGE
    if selected == 'Customer Search':     
# TITLE AND FILE NAMES
        st.title('Customer Search')   
# FILTER
        no_duplicates_df = concat_df.drop_duplicates(subset='ID', keep='last')
        customer_option = no_duplicates_df.Name_C.unique()
        
        select = st.selectbox("Select Customer", customer_option)
        submit = st.button("Submit")
        if submit:
            customerdata = no_duplicates_df[no_duplicates_df["Name_C"].astype(str).str.contains(select)]
            original_dict = customerdata.to_dict(orient="list")
    
            st.markdown("## Profile for {} ##".format(select))
            cust_list = ['Name_C', 'Phone', 'Email','State', 'DOB', 'Age', 'Referral', 'Company_Name',
                         'Company_Type', 'Cust_Notes', 'Date_Approved', 'Status', 'Purpose', 'Source', 
                         'Occupation', 'Last_Review', 'Statements_Collected']
            
            reordered_cust = {k: original_dict[k] for k in cust_list}
            for key, val in reordered_cust.items():
                st.markdown(f'**{key}** : {val}')
            st.text("")
                    
            st.markdown("### Risk Report ###")
            risk_list = ['Risk_Rating', 'Review_Needed', 'Statements_Needed', 'Is_FEEVA', 
                             'Is_High_Volume', 'Is_Financial_Inst',  'Is_BL_Wallet', 'Is_Shared_Wallet',
                             'Shared_Count', 'Shared_Count', 'Shared_Total', 'Wallet_Last_TX', 'Wallet_Notes_S']
            
            reordered_dict2 = {k: original_dict[k] for k in risk_list}
            for key, val in reordered_dict2.items():
                st.markdown(f'**{key}** : {val}')
            st.text("")   
                
            st.markdown("### SAR ###")      
            with st.expander("Show Details"):
                sar_list = ['SAR_ID', 'SAR_Type', 'Alert_Date', 'Prompt', 'SAR_TX_Total', 
                             'Date_Filed', 'BL', 'SAR_Notes']
            
                reordered_dict3 = {k: original_dict[k] for k in sar_list}
                for key, val in reordered_dict3.items():
                    st.markdown(f'**{key}** : {val}')            
            st.text("")
            
            st.markdown("### Transactions ###")
            tx_list = ['Percentile', 'Agg_Volume', 'Avg_Volume', 'TX_Count', 'Last_TX']
            
            reordered_dict4 = {k: original_dict[k] for k in tx_list}
            for key, val in reordered_dict4.items():
                st.markdown(f'**{key}** : {val}')
        
            with st.expander("Show Transactions"):
                customet_tx_frame = concat_df[['Control', 'TX_Date', 'ID', 'Name_T', 'Name_C', 'Amount_Received', 'Asset_Received', 'Received_At', 
                                  'Received', 'Amount_Sent', 'Asset_Sent', 'Address', 'Sent', 'Exchange_Rate', 'Trade_Value',
                                  'Inventory', 'TX_Notes', 'Wallet_Notes']]
                customer_txs = customet_tx_frame[customet_tx_frame["Name_C"].astype(str).str.contains(select)]
                customer_txs = customer_txs.sort_values(by=['Control', 'TX_Date'], ascending=True)
                customer_txs.dropna(subset = ['Control'], inplace=True)
                customer_txs.set_index('Control', inplace=True)
                st.write(customer_txs.shape)
                test = customer_txs.astype(str)
                st.dataframe(test)       
# DOWNLOAD
                @st.cache
                def convert_df_to_csv(df):
                    return df.to_csv().encode('utf-8')
                st.download_button(
                    label = 'Download as CSV',
                    data=convert_df_to_csv(customer_txs),
                    file_name='customer_transactions.csv',
                    mime='text/csv',
                    )
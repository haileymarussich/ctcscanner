# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 16:09:24 2022

@author: haile
"""






for f in filters:
    if (dfnew[f].dtype == 'object'):
    choices = concat_df[f].unique()
    
    dfnew = dfnew[dfnew[f].isin(options)]
    
    
if (concat_df[f].dtype == 'datetime64[ns]'):
        min_start_date = dfnew[f].min()
        min_start_date = dfnew[f].min()
        start_date_input = st.date_input('Start Date', min_start_date, min_value = min_start_date, max_value = max_end_date)
        end_date_input = st.date_input('End Date', max_end_date, min_value = min_start_date, max_value = max_end_date)


        start_date_input_fixed = pd.to_datetime(start_date_input) - datetime.timedelta(days=1)
        end_date_input_fixed = pd.to_datetime(end_date_input) + datetime.timedelta(days=1)

        dfnew = dfnew.copy()
        dfnew = dfnew.loc[(dfnew[f] >= start_date_input_fixed) & 
                                                (dfnew[f] <= end_date_input_fixed)]
        dfnew[f] = pd.to_datetime(dfnew[f]).dt.date()

        #dfnew = dfnew.sort_values([f], ascending=True)
        # dfnew.set_index(f, inplace=True)


if (concat_df[f].dtype == 'float64'):
        min_value = concat_df[f].min()
        max_value = concat_df[f].max()
        values = st.slider('Select a range of values', min_value, max_value, (min_value, max_value))

        dfnew = dfnew.loc[(dfnew[f] >= min_value)
                & (dfnew[f] <= max_value)]
        #dfnew = filtered_buys.sort_values(by='tx_total', ascending=False)






# CONCAT SHEETS
concat_on_id = tx_log_df.sort_values('ID').duplicated('Date', keep='last')
customer_database_df.merge(tx_log_df[~concat_on_id], on='ID', how='left')

pd.merge(tx_log_df, customer_database_df, how="right", on="ID")

# Date last transacted (Customer_Database)
customer_database_df['Date_Last Transacted'] = customer_database_df.groupby(['ID', df.TransactionDate.dt.year])['TransactionDate'].transform(max)

# "Dormant" Calculation: GTG for 6 months after approval with no volume (Customer_Database)
min_start_date = dateindex_tx_log.index.min()
max_end_date = dateindex_tx_log.index.max()

tx_log_df.loc[(tx_log_df['Date_Approved'] + DateOffset(months=6) < max_end_date) & 
                         (customer_database_df['Last_Review'] + DateOffset(months=6) < max_end_date), 
                         'Review_Due'] = 'Needs Review' 

customer_database_df.loc[(customer_database_df['Date_Approved'] + DateOffset(months=6) < max_end_date) & 
                         (customer_database_df['Last_Review'] + DateOffset(months=6) < max_end_date), 
                         'Review_Due'] = 'Needs Review'  

customer_database_df.loc[(df['first_name'] + DateOffset(months=3)) & (df['first_name'] != 'Emma'), 'name_match'] = 'Mismatch'

df['date_column'] + DateOffset(months=3)

customer_database_df.loc[customer_database_df["Dormant"]
customer_database_df.loc[(customer_database_df['Approved_Date'] 'Harry')
                         & (df['B']=='George') & (df['C']>'2019'),'Status'] = 'Dormant'     



    if not values:
        st.error('ERROR: No data found.')
    else:
        rows = sheet.values().get(spreadsheetId=spreadsheet_id, range=tx_log_range).execute()
        tx_data = rows.get('values')
        st.success("COMPLETE: Data retrieved.")
        
        raw_check = st.checkbox("Show Data")
        if raw_check:  
            tx_log_df = pd.DataFrame(tx_data[1:], columns=tx_data[0])
            st.write(tx_log_df.shape)
            AgGrid(tx_log_df, key = 'txs', editable = True, fit_columns_on_grid_load = True)

# NAVIGATION
sidebar = st.sidebar.header('Navigation')
with st.sidebar:
    selected = option_menu(
        menu_title=None,
        options = ['Raw Data Upload', 'Customer Agg. Volumes', 'CTR Scanner', 'Shared Wallet Scanner']
    )

        
# FILE UPLOADER
uploaded_files = st.sidebar.file_uploader("Upload Multiple CSV Files", accept_multiple_files=True)

df = pd.DataFrame()
li = []

if uploaded_files is not None:
    for uploaded_file in uploaded_files:
        df = pd.read_csv(uploaded_file, sep=";", encoding='ISO-8859-1')
        li.append(df)      

# merge files into one
if len(li) == 0:
	st.error("Error: No data to analyze. Please upload at least one CSV.")
else:
    st.sidebar.success("Success: Data uploaded.")

if len(li) > 0:
    frame = pd.concat(li, axis=0, ignore_index=True)

# Server Time pd.to_datetime
    frame['Server Time'] = pd.to_datetime(frame['Server Time'], format = '%Y-%m-%d %H:%M:%S')

# sort by Server Time
    frame = frame.sort_values(by = 'Server Time', ascending = True)

# drop duplicates
    frame = frame.drop_duplicates()

# drop columns and create main_df
    main_df = frame[['Server Time', 'Type', 'Cash Amount', 'Crypto Amount', 'Terminal SN', 'Local Transaction Id', 'Remote Transaction Id',
                       'Destination Address', 'Identity', 'Identity First Name', 'Identity Last Name']].copy()

# make lowercase and remove spaces from column names
    main_df.columns = main_df.columns.str.replace(' ', '_')
    main_df.columns = map(str.lower, main_df.columns)

# new df with server_time not as index
    main_noindex_df = main_df.sort_values(by='server_time')
    main_noindex_df['server_time'] = pd.to_datetime(main_noindex_df['server_time'], format = '%Y-%m-%d %H:%M:%S')

# set server_time as index
    main_df.set_index('server_time', inplace=True)

# sort by ascending time (index)
    main_df = main_df.sort_index()


# RAW DATA PAGE
if len(li) > 0 and selected == 'Raw Data Upload':

# title and file names
    st.title('Raw Data Upload')    
    
# checkbox
    raw_check = st.checkbox("Show Unfiltered Data")
    if raw_check:  
        
# main_df print   
        st.subheader('All Transactions')
        st.write(main_noindex_df.shape)
        AgGrid(main_noindex_df, key = 'raw1', editable = True)

# date range select = start_date_input and end_date_input
    min_start_date = main_df.index.min()
    max_end_date = main_df.index.max()
    start_date_input = st.date_input('Start Date', min_start_date, min_value = min_start_date, max_value = max_end_date)
    end_date_input = st.date_input('End Date', max_end_date, min_value = min_start_date, max_value = max_end_date)

# validation
    if start_date_input < end_date_input:
        st.success('Start Date: `%s`\n\nEnd Date: `%s`' % (start_date_input, end_date_input))
    else:
        st.error('Error: End date must fall after start date.')

# pd.Timestamp inputs
    start_date_input_fixed = pd.to_datetime(start_date_input) - datetime.timedelta(days=1)
    end_date_input_fixed = pd.to_datetime(end_date_input) + datetime.timedelta(days=1)

# submit filtering and print new df = time_filtered_df
    submit_button = st.button('Filter')
    
    if submit_button:
        time_filtered_df = main_noindex_df.copy()
        time_filtered_df = time_filtered_df.loc[(time_filtered_df['server_time'] >= start_date_input_fixed) & 
                                                (time_filtered_df['server_time'] <= end_date_input_fixed)]
        
        st.subheader('Filtered Transactions')
        st.write(time_filtered_df.shape)
        AgGrid(time_filtered_df, key = 'raw2', editable = True)
        
# download as a csv
        @st.cache
        def convert_df_to_csv(df):
            return df.to_csv().encode('utf-8')

        st.download_button(
            label = 'Download as CSV',
            data=convert_df_to_csv(time_filtered_df),
            file_name='filtered_transactions.csv',
            mime='text/csv',
        )
    
    
# CUSTOME AGG. VOLUMES PAGE
if len(li) > 0 and selected == 'Customer Agg. Volumes':
    st.title('Customer Agg. Volumes')
    
# customer buys
    customer_buys = main_noindex_df[main_noindex_df.type=="BUY"].groupby(['identity', 'identity_first_name', 'identity_last_name'], as_index=False).agg(
        tx_total = ('cash_amount', 'sum'), last_transacted = ('server_time', 'max'))

# customer buys format   = customer_buys_df  
    customer_buys_df = customer_buys.copy()
    customer_buys_df.set_index('identity', inplace=True)
    customer_buys_df['last_transacted'] = pd.to_datetime(customer_buys_df['last_transacted']).dt.date
    customer_buys_df = customer_buys_df.sort_values(by='last_transacted', ascending=False)
    
# customer sells
    customer_sells = main_noindex_df[main_noindex_df.type=="SELL"].groupby(['identity', 'identity_first_name', 'identity_last_name'], as_index=False).agg(
        tx_total = ('cash_amount', 'sum'), last_transacted = ('server_time', 'max'))

# customer sells format = customer_sells_df 
    customer_sells_df = customer_sells.copy()
    customer_sells_df.set_index('identity', inplace=True)
    customer_sells_df['last_transacted'] = pd.to_datetime(customer_sells_df['last_transacted']).dt.date
    customer_sells_df = customer_sells_df.sort_values(by='last_transacted', ascending=False)
    
 # checkbox
    agg_check = st.checkbox("Show Unfiltered Data")
    if agg_check:     
    
# unfiltered dfs print
    # col1,col2 = st.columns([2,2])
                                  
        st.subheader('Customer Buys')
        st.write(customer_buys_df.shape)
    
        buys_aggrid1 = customer_buys_df.reset_index()
        AgGrid(buys_aggrid1, key = 'buys1', editable = True, fit_columns_on_grid_load = True)
    
    # col2.subheader('Customer Sells')
    # col2.write(customer_sells_df.shape)
    
    # sells_aggrid1 = customer_sells_df.reset_index()
    # col2.AgGrid(sells_aggrid1, key = 'sells1', editable = True)
    
# min date last_transacted select = last_transacted_date_input
    min_start_date = main_df.index.min()
    max_end_date = main_df.index.max()
    last_transacted_date_input = st.date_input('Min Date Last Transacted', max_end_date, min_value = min_start_date, max_value = max_end_date)
    
# pd.Timestamp inputs
    last_transacted_date_input = pd.to_datetime(last_transacted_date_input)
    
# min tx_total select = last_transacted_amount_input
    last_transacted_amount_input = st.slider('Min Transaction Total', min_value = 0, max_value = 50000, value = 25000)

# submit filtering and print new df = filtered_buys and filtered_sells
    agg_total_submit_button = st.button('Filter')
    if agg_total_submit_button:
        filtered_buys = customer_buys_df.loc[(customer_buys_df['last_transacted'] >= last_transacted_date_input)
                                   & (customer_buys_df['tx_total'] >= last_transacted_amount_input)]
        filtered_buys = filtered_buys.sort_values(by='tx_total', ascending=False)
        
        filtered_sells = customer_sells_df.loc[(customer_sells_df['last_transacted'] >= last_transacted_date_input)
                                   & (customer_sells_df['tx_total'] >= last_transacted_amount_input)]
        filtered_sells = customer_sells_df.sort_values(by='tx_total', ascending=False)
        
        st.subheader('Filtered Buys')
        st.write(filtered_buys.shape)
        
        buys_aggrid2 = filtered_buys.reset_index()
        AgGrid(buys_aggrid2, key = 'buys2', editable = True, fit_columns_on_grid_load = True)
    
        # col2.subheader('Filtered Sells')
        # col2.write(filtered_sells.shape)

        # sells_aggrid2 = filtered_sells.reset_index()
        # col2.AgGrid(sells_aggrid2, key = 'sells2', editable = True)


# CTR SCANNER PAGE
if len(li) > 0 and selected == 'CTR Scanner':
    st.title('CTR Scanner')
    
    ctrs = main_noindex_df.groupby(['identity', 'identity_first_name', 'identity_last_name', pd.Grouper(freq='1D', key = 'server_time')]).agg(
        tx_total = ('cash_amount', 'sum'))

# ctrs format = ctr_df   
    ctr_df = ctrs.copy()
    ctr_df.reset_index(inplace=True)
    ctr_df['server_time'] = pd.to_datetime(ctr_df['server_time']).dt.date
    ctr_df.rename({'server_time': 'date'}, axis = "columns", inplace = True)
    ctr_df = ctr_df.sort_values(by='date', ascending=False)    

 # checkbox
    ctr_check = st.checkbox("Show Unfiltered Data")
    if ctr_check:  

# ctr_df print
        st.subheader('All Totals')
        st.write(ctr_df.shape)

        AgGrid(ctr_df, key = 'ctrs1', editable = True, fit_columns_on_grid_load = True)
    
# date range select = ctr_start_date_input and ctr_end_date_input
    min_start_date = main_df.index.min()
    max_end_date = main_df.index.max()
    ctr_start_date_input = st.date_input('Start Date', min_start_date, min_value = min_start_date, max_value = max_end_date)
    ctr_end_date_input = st.date_input('End Date', max_end_date, min_value = min_start_date, max_value = max_end_date)

# validation
    if ctr_start_date_input < ctr_end_date_input:
        st.success('Start Date: `%s`\n\nEnd Date: `%s`' % (ctr_start_date_input, ctr_end_date_input))
    else:
        st.error('Error: End date must fall after start date.')

# pd.Timestamp inputs
    ctr_start_date_input = pd.to_datetime(ctr_start_date_input)
    ctr_end_date_input = pd.to_datetime(ctr_end_date_input)
    
# tx_total slider
    amount_select = st.slider('Transaction Total', min_value = 2000, max_value = 20000, value = [10001, 20000])
    amount_slider_max = amount_select[1]
    amount_slider_min = amount_select[0]
    
# submit filtering and print new df = filtered_ctrs
    ctr_submit_button = st.button('Filter')
    
    if ctr_submit_button:
        filtered_ctrs = ctr_df.loc[(ctr_df['date'] >= ctr_start_date_input) & (ctr_df['date'] <= ctr_end_date_input) 
                                   & (ctr_df['tx_total'] <= amount_slider_max) & (ctr_df['tx_total'] >= amount_slider_min)]
        filtered_ctrs = filtered_ctrs.sort_values(by='date', ascending=False)
        
        st.subheader('Filtered Totals')
        st.write(filtered_ctrs.shape)

        AgGrid(filtered_ctrs, key = 'ctrs2', editable = True, fit_columns_on_grid_load = True)
        
# download as a csv
        @st.cache
        def convert_df_to_csv(df):
            return df.to_csv().encode('utf-8')

        st.download_button(
            label = 'Download as CSV',
            data=convert_df_to_csv(filtered_ctrs),
            file_name='filtered_ctrs.csv',
            mime='text/csv',
        )


# SHARED WALLER SCANNER PAGE
if len(li) > 0 and selected == 'Shared Wallet Scanner':
    st.title('Shared Wallet Scanner')

# sw_main_noindex copy of main_noindex_df
    sw_main_noindex_copy = main_noindex_df[['destination_address', 'identity', 'cash_amount', 'server_time']].copy()
    
# shared_wallets
    shared_wallets = sw_main_noindex_copy.groupby(['destination_address']).agg(
        count = ('identity', 'nunique'), identities = ('identity', 'unique'), shared_total = ('cash_amount', 'sum'), last_transacted = ('server_time', 'max'))
    
# remove single identity counts
    shared_wallets = shared_wallets.loc[(shared_wallets['count'] > 1)]

# shared_wallets format = shared_wallets_df                                                                    
    shared_wallets_df = shared_wallets.copy()
    shared_wallets_df['last_transacted'] = pd.to_datetime(shared_wallets_df['last_transacted']).dt.date
    shared_wallets_df = shared_wallets_df.sort_values(by='last_transacted', ascending=False)

 # checkbox
    shared_check = st.checkbox("Show Unfiltered Data")
    if shared_check:
        
# shared_wallets_df print
        st.subheader('Shared Wallets')
        st.write(shared_wallets_df.shape)

        sw_aggrid1 = shared_wallets_df.reset_index()
        AgGrid(sw_aggrid1, key = 'sw1', editable = True, fit_columns_on_grid_load = True)
       
# min date last_transacted select = shared_wallet_date input
    min_start_date = main_df.index.min()
    max_end_date = main_df.index.max()
    shared_wallet_date_input = st.date_input('Min Date Last Transacted', max_end_date, min_value = min_start_date, max_value = max_end_date)
    
# pd.Timestamp inputs
    shared_wallet_date_input = pd.to_datetime(shared_wallet_date_input)
    
# min shared_total select
    shared_wallet_amount_input = st.slider('Min Transaction Total', min_value = 0, max_value = 20000, value = 2000)
    
# submit filtering and print new df = filtered_shared_wallets
    shared_wallets_submit_button = st.button('Filter')
    if shared_wallets_submit_button:
        filtered_shared_wallets = shared_wallets_df.loc[(shared_wallets_df['last_transacted'] >= shared_wallet_date_input)
                                   & (shared_wallets_df['shared_total'] >= shared_wallet_amount_input)]
        filtered_shared_wallets = filtered_shared_wallets.sort_values(by='shared_total', ascending=False)
        
        st.subheader('Filtered Shared Wallets')
        st.write(filtered_shared_wallets.shape)

        sw_aggrid2 = filtered_shared_wallets.reset_index()
        AgGrid(sw_aggrid2, key = 'sw2', editable = True, fit_columns_on_grid_load = True)
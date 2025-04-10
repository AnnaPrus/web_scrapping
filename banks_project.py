# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd 
from datetime import datetime 
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
import requests

log_file = "code_log.txt"
URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = "./exchange_rate.csv"
table_name = "Largest_banks"
output_csv_path = "./banks_transformed.csv"

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    # List to hold the extracted data
    extracted_data = []
    
    for row in rows[1:]:  # Skip header row
        cols = row.find_all('td')
        if len(cols) >= 2:  # Make sure the row has enough columns
            name = cols[1].text.strip()
            mc_usd_billion = float(cols[2].text.strip().replace(',', '').replace('$', '').strip())  # Market Cap in Billion USD
            extracted_data.append([name, mc_usd_billion])

    # Create a DataFrame 
    columns = table_attribs 
    df = pd.DataFrame(extracted_data, columns=columns)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    # Transform Market Cap from USD to other currencies
    if 'MC_USD_Billion' in df.columns:
        df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
        df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
        df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

        log_progress("Transformation complete. New columns: MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion.")
    else:
        log_progress("MC_USD_Billion column not found in the DataFrame.")
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress(f"Data saved to CSV file: {output_path}")

def load_to_db(df, conn, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress(f"Data loaded into table: {table_name}")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    result = sql_connection.execute(query_statement).fetchall()
    print(result)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
df1 = extract(URL, table_attribs)
df_transformed = transform(df1, csv_path)
load_to_csv(df_transformed, output_csv_path) 

sql_connection = 'Banks.db'
conn = sqlite3.connect(sql_connection)
load_to_db(df_transformed, conn,table_name)  # Load data to SQLite database

''' 
# Running a sample query to test
''' 
log_progress('Print the contents of the entire table.')
run_query("SELECT * FROM Largest_banks",conn)

log_progress('Print the average market capitalization of all the banks in Billion USD.')
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks",conn)

log_progress('Print only the names of the top 5 banks.')
run_query("SELECT Name from Largest_banks LIMIT 5",conn)

conn.close()

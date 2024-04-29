import requests 
import pandas as pd     
import numpy as np                      
from bs4 import BeautifulSoup
import sqlite3 
from datetime import datetime 

url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['Name', 'MC_USD_Billion']

csv_path = 'largest_banks_data.csv'
database_name = 'Banks.db' 
table_name = 'largest_banks' 


def log_process(message):
    timestamp_format = '%Y-%h-%d-%H:$M:$S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')

# TASK 1: DATA EXTRACTION
def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {'Name': col[1].find_all('a')[-1]['title'], 'MC_USD_Billion': col[2].contents[0][:-2]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df        

# TASK 2: TRANSFORM 
def transform(df):
    money = df['MC_USD_Billion'].tolist()
    money = [float(x) for x in money]
    rate = pd.read_csv("exchange_rate.csv")
    euro = [np.round(x*float(rate.iloc[0, 1]), 2) for x in money]
    gbp = [np.round(x*float(rate.iloc[1,1]), 2) for x in money]
    inr = [np.round(x*float(rate.iloc[2, 1]), 2) for x in money]
    df['MC_EUR_Billion'] = euro
    df['MC_GBP_Billion'] = gbp
    df['MC_INR_Billion'] = inr 
    return df 
 
# TASK 3: LOAD TRANSFORMED INFO 
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

# TASK 4: LOAD TO DATABASE 
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# TASK 5: RUN QUERY
def run_query(statement, sql_connection):
    print(statement)
    query_output = pd.read_sql(statement, sql_connection)
    print(query_output)

log_process('preliminaries complete. Iniating ETL process')

df = extract(url, table_attribs)

log_process('Data extraction complete. Iniating transformation process')

df = transform(df) 

log_process('data transformation complete. Iniating loading process')

load_to_csv(df, csv_path) 

log_process("Data saved to CSV file")

sql_connection = sqlite3.connect(database_name)

log_process("SQL connection initiated")

load_to_db(df, sql_connection, table_name)

log_process("Data loaded to Database as table. Running the query")

statement = f"SELECT * FROM {table_name}"
run_query(statement, sql_connection)

statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(statement, sql_connection)

statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(statement, sql_connection)

log_process("Process complete")

sql_connection.close()

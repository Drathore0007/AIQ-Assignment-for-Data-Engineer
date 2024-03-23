import pandas as pd
import pyodbc
from sqlalchemy import create_engine

# Extract data from source (e.g., CSV file)
data = pd.read_csv('data.csv')

# Transform data if needed
# For example, you can clean, filter, or manipulate the data here

# Load data into SQL Server
server = 'your_server'
database = 'your_database'
username = 'Dharm'
password = 'Admin@123'

conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}')

data.to_sql('table_name', engine, if_exists='replace', index=False)

print('Data loaded successfully to SQL Server')


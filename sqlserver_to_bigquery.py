from pydoc import describe
import pyodbc
import pandas as pd
import numpy as np
from google.oauth2 import service_account

cnxn_str = ("Driver={SQL Server Native Client 11.0};"
            "Server=DESKTOP-IC62LU3;"
            "Database=AdventureWorksDW2019;"
            "Trusted_Connection=yes;")
cnxn = pyodbc.connect(cnxn_str)

cursor = cnxn.cursor()
def extract_from_sql_server():
    try:
        sql = "select t.name as table_name from sys.tables t where t.name in ('DimProductCategory')"
        cursor.execute(sql)
        y = cursor.fetchall()
        table_list = list()
        for i in range(len(y)):
            y[i] = str(y[i])
            y[i] = y[i][2:]
            y[i] = y[i][:-4]
            table_list.append(y[i])
        return table_list
    except Exception as e:
        print("Data extraction error " + str(e))

def load_to_bigquery():
    credentials = service_account.Credentials.from_service_account_file('weather-data-357710-e40c1a3a03e1.json')
    projectID = "weather-data-357710"
    dataset_ref = "Migration_Test"
    for v in (extract_from_sql_server()):
        rows_imported = 0
        extract_sql = f'select * from {v}'
        rows = cursor.execute(extract_sql).fetchall()
        df = pd.DataFrame().from_records(rows)
        df.columns = pd.DataFrame(np.matrix(cursor.description))[0]
        # if v == 'DimSalesTerritory':
        #     df.drop
        print(df.info())
        print(f'importing {len(df)} rows for table {v}')
        df.to_gbq(destination_table=f'{dataset_ref}.src_{v}', project_id=projectID, credentials=credentials, if_exists="replace")

print(extract_from_sql_server())
load_to_bigquery()
print("Done")


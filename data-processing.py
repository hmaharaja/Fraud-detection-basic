"""
Retrieve the masked fraud data from the Azure SQL database and use data duplication to increase the number of fraud samples,
the output a new dataset csv to feed into the Azure ML model.
"""
import sqlalchemy as sal
import pandas as pd
import pyodbc
import urllib
import getpass
import random

def connect_to_db(uid, pwd):
    params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=hackitout.database.windows.net,1433;DATABASE=transaction_data;UID=%s;PWD=%s" % (uid, pwd))
    engine = sal.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    conn = engine.connect()
    return conn, engine


def resample(raw_df):
    new = []
    non_fraud = 0
    for i, row in raw_df.iterrows():
        if non_fraud < 10001 and row["Class"].strip() == "'0'": # only keep ~10000 non-fraud examples randomly scattered in the dataset
            randnum = random.random()
            if randnum >= 0.5:
                new.append(row)
                non_fraud +=1
        
        elif row["Class"].strip() == "'1'": # we want ~1000 fraud samples so the model can at least overfit to those instead of guessing
            randnum = random.randint(2,5)

            while randnum != 0:
                new.append(row)
                randnum -=1

    data_df = pd.DataFrame(new, columns=raw_df.columns)
    data_df = data_df.sample(frac=1).reset_index(drop=True)
    return data_df
    

if __name__ == "__main__":
    uid = input()
    pwd = getpass.getpass()

    conn, engine = connect_to_db(uid, pwd)

    dataset = sal.Table('dataset2', sal.MetaData(), autoload=True, autoload_with=engine)
    columns = dataset.columns.keys()
    table_df = pd.read_sql_table('dataset2', con=engine, columns=columns)
    new_dataset_df = resample(table_df)
    
    export_path = "dataset.csv"
    new_dataset_df.to_csv(export_path, index=False)

    conn.close()
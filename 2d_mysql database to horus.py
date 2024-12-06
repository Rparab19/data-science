import pandas as pd import sqlite3 as sq
sInputFileName='D:/Dinesh/DS/db_horus/utility.db' sInputTable='Country_Code'
conn = sq.connect(sInputFileName) sSQL='select * FROM ' + sInputTable + ';' InputData=pd.read_sql_query(sSQL, conn)
print('Input Data Values ===================================')
print(InputData) print('=====================================================')
ProcessData=InputData
ProcessData.drop('ISO-2-CODE', axis=1,inplace=True) ProcessData.drop('ISO-3-Code', axis=1,inplace=True) ProcessData.rename(columns={'Country': 'CountryName'}, inplace=True) ProcessData.rename(columns={'ISO-M49': 'CountryNumber'}, inplace=True) ProcessData.set_index('CountryNumber', inplace=True)
ProcessData.sort_values('CountryName', axis=0, ascending=False, inplace=True) print('Process Data Values =================================') print(ProcessData) print('=====================================================')
OutputData=ProcessData sOutputFileName='D:/Dinesh/DS/db_horus/HORUSDB-CSV-Country.csv' OutputData.to_csv(sOutputFileName, index = False)
print('Database to HORUS - Done')

import sys import os
import sqlite3 as sq import pandas as pd Base = 'D:/Dinesh/DS/5/'
print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sInputFileName='csv/Retrieve_Profit_And_Loss.csv' sDataBaseDir=Base + '/' + '/02-Assess/SQLite'
if not os.path.exists(sDataBaseDir): os.makedirs(sDataBaseDir)
sDatabaseName=sDataBaseDir + '/clark.db' conn = sq.connect(sDatabaseName)
### Import Financial Data sFileName=Base + '/'+ '/' + sInputFileName
print('################################')
print('Loading :',sFileName) print('################################')
FinancialRawData=pd.read_csv(sFileName,header=0,low_memory=False,  encoding="latin-1")
FinancialData=FinancialRawData
print('Loaded Company :',FinancialData.columns.values) print('################################') print('################')
 
sTable='Assess-Financials'
print('Storing :',sDatabaseName,' Table:',sTable) FinancialData.to_sql(sTable, conn, if_exists="replace") print('################')
print(FinancialData.head()) print('################################')
print('Rows : ',FinancialData.shape[0]) print('################################')
print('### Done!! ############################################')

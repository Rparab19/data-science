import sys import os
import pandas as pd Base = 'D:/Dinesh/DS/5'
print('################################')
print('Working Base :',Base) print('################################')

InputFileName='Retrieve_All_Countries.csv' OutputFileName='Assess_All_Warehouse.csv' sFileDir=Base + '/5g'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)
sFileName=Base + '/' + InputFileName print('###########')
print('Loading :',sFileName)

Warehouse=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") sColumns={'X1' : 'Country',
'X2' : 'PostCode',
'X3' : 'PlaceName',
'X4' : 'AreaName',
'X5' : 'AreaCode',
'X10' : 'Latitude',
 
'X11' : 'Longitude'} Warehouse.rename(columns=sColumns,inplace=True)
WarehouseGood=Warehouse sFileName=sFileDir + '/' + OutputFileName
WarehouseGood.to_csv(sFileName, index = False)
print('### Done!! ############################################')

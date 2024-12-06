import sys import os
import pandas as pd Base = 'D:/Dinesh/DS/4/'
sFileName = os.path.join(Base, 'ret/IP_DATA_ALL.csv') print('Loading :', sFileName)

try:
IP_DATA_ALL = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin- 1")
print("File loaded successfully.") except Exception as e:
print("Error loading file:", e) sys.exit(1)
sFileDir = os.path.join(Base, 'updated/') if not os.path.exists(sFileDir):
try:
os.makedirs(sFileDir) print("Directory created:", sFileDir)
except Exception as e:
print("Error creating directory:", e) sys.exit(1)
 
print('Rows:', IP_DATA_ALL.shape[0]) print('Columns:', IP_DATA_ALL.shape[1])

print('### Raw Data Set #####################################')
for i in range(len(IP_DATA_ALL.columns)): print(IP_DATA_ALL.columns[i], type(IP_DATA_ALL.columns[i]))

print('### Fixed Data Set ###################################') IP_DATA_ALL_FIX = IP_DATA_ALL.copy()
for i in range(len(IP_DATA_ALL.columns)): cNameOld = IP_DATA_ALL.columns[i] cNameNew = cNameOld.strip().replace(" ", ".")
IP_DATA_ALL_FIX.columns.values[i] = cNameNew print(IP_DATA_ALL_FIX.columns[i], type(IP_DATA_ALL_FIX.columns[i]))

print('Fixed Data Set with ID')
IP_DATA_ALL_with_ID = IP_DATA_ALL_FIX.copy() IP_DATA_ALL_with_ID.index.name = 'RowID'
sFileName2 = os.path.join(sFileDir, 'Retrieve_IP_DATA.csv') try:
IP_DATA_ALL_with_ID.to_csv(sFileName2, index=True, encoding="latin-1") print("File saved successfully:", sFileName2)
except Exception as e: print("Error saving file:", e) sys.exit(1)
print('### Done!! ############################################')

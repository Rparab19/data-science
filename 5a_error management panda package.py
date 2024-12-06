i.	 Drop the Columns Where All Elements Are Missing Values
Code:
import sys import os
import pandas as pd Base='D:/Dinesh/DS/5/' sInputFileName='Good-or-Bad.csv' sOutputFileName='Good-or-Bad-01.csv' sFileDir=Base + '/5aa'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)
Company = 'YourCompanyName' # Define your company name variable sFileName=Base + sInputFileName
print('Loading :',sFileName) RawData=pd.read_csv(sFileName,header=0) print('################################')
print('## Raw Data Values') print('################################')
print(RawData) print('################################')
print('## Data Profile') print('################################')
print('Rows :',RawData.shape[0]) print('Columns :',RawData.shape[1]) print('################################')
sFileName=sFileDir + '/' + sInputFileName RawData.to_csv(sFileName, index = False)
 
TestData=RawData.dropna(axis=1, how='all') print('## Test Data Values') print('################################')
print(TestData) print('################################')
print('## Data Profile') print('################################')
print('Rows :',TestData.shape[0]) print('Columns :',TestData.shape[1]) sFileName=sFileDir + '/' + sOutputFileName TestData.to_csv(sFileName, index = False) print('### Done!! #####################')
Output:

 
 


ii.	 Drop the Columns Where Any of the Elements Is Missing Values
Code:
import sys import os
import pandas as pd Base='D:/Dinesh/DS/5/' sInputFileName='Good-or-Bad.csv' sOutputFileName='Good-or-Bad-01.csv'
 
sFileDir=Base + '/5ab'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)
Company = 'YourCompanyName' # Define your company name variable sFileName=Base + sInputFileName
print('Loading :',sFileName) RawData=pd.read_csv(sFileName,header=0) print('################################')
print('## Raw Data Values') print('################################')
print(RawData) print('################################')
print('## Data Profile') print('################################')
print('Rows :',RawData.shape[0]) print('Columns :',RawData.shape[1]) sFileName=sFileDir + '/' + sInputFileName RawData.to_csv(sFileName, index = False) TestData=RawData.dropna(axis=1, how='any') print('################################')
print('## Test Data Values') print('################################')
print(TestData) print('################################')
print('## Data Profile') print('################################')
print('Rows :',TestData.shape[0]) print('Columns :',TestData.shape[1]) sFileName=sFileDir + '/' + sOutputFileName TestData.to_csv(sFileName, index = False) print('### Done!! #####################')
 

Output:



 
 



iii.	 Keep Only the Rows That Contain a Maximum of Two Missing Values
Code:
import sys import os
import pandas as pd
 
Base='D:/Dinesh/DS/5/' sInputFileName='Good-or-Bad.csv' sOutputFileName='Good-or-Bad-01.csv' sFileDir=Base + '/5ac'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)
Company = 'YourCompanyName' # Define your company name variable sFileName=Base + sInputFileName
print('Loading :',sFileName) RawData=pd.read_csv(sFileName,header=0) print('################################')
print('## Raw Data Values') print('################################')
print(RawData) print('################################')
print('## Data Profile') print('################################')
print('Rows :',RawData.shape[0]) print('Columns :',RawData.shape[1]) print('################################')
sFileName=sFileDir + '/' + sInputFileName RawData.to_csv(sFileName, index = False) TestData=RawData.dropna(thresh=2) print('################################')
print('## Test Data Values') print('################################')
print(TestData) print('################################')
print('## Data Profile') print('################################')
print('Rows :',TestData.shape[0])
 
print('Columns :',TestData.shape[1]) print('################################')
sFileName=sFileDir + '/' + sOutputFileName TestData.to_csv(sFileName, index = False) print('################################') print('### Done!! #####################') print('################################')

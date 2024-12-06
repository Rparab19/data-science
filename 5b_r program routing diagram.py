Assess-Network-Routing-Company
Code:
import sys import os
import pandas as pd pd.options.mode.chained_assignment = None Base='D:/Dinesh/DS/5/' print('################################')
print('Working Base :',Base, ' using Windows') print('################################')
sInputFileName1='/Retrieve_Country_Code.csv' sInputFileName2='/Retrieve_Router_Location.csv' sInputFileName3='/Retrieve_IP_DATA.csv' sOutputFileName='Assess-Network-Routing-Company.csv' sFileName=Base + 'csv' + sInputFileName1 print('################################')
print('Loading :',sFileName) print('################################')
CountryData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") print('Loaded Country:',CountryData.columns.values) print('################################') print('################################')
print('Changed :',CountryData.columns.values) CountryData.rename(columns={'Country': 'Country_Name'}, inplace=True) CountryData.rename(columns={'ISO-2-CODE': 'Country_Code'}, inplace=True) CountryData.drop('ISO-M49', axis=1, inplace=True)
CountryData.drop('ISO-3-Code', axis=1, inplace=True)
 
CountryData.drop('RowID', axis=1, inplace=True) print('To :',CountryData.columns.values) print('################################')
sFileName=Base + 'csv' + sInputFileName2 print('################################')
print('Loading :',sFileName) print('################################')
CompanyData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin1") print('Loaded Company :',CompanyData.columns.values) print('################################') print('################################')
print('Changed :',CompanyData.columns.values) CompanyData.rename(columns={'Country': 'Country_Code'}, inplace=True) print('To :',CompanyData.columns.values) print('################################')
sFileName=Base + 'csv' + sInputFileName3 print('################################')
print('Loading :',sFileName) print('################################')
CustomerRawData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1")
print('################################')
print('Loaded Customer :',CustomerRawData.columns.values) print('################################')
CustomerData=CustomerRawData.dropna(axis=0, how='any') print('################################')
print('Remove Blank Country Code')
print('Reduce Rows from', CustomerRawData.shape[0],' to ', CustomerData.shape[0]) print('################################') print('################################')
print('Changed :',CustomerData.columns.values)
 
CustomerData.rename(columns={'Country': 'Country_Code'}, inplace=True) print('To :',CustomerData.columns.values) print('################################') print('################################')
print('Merge Company and Country Data') print('################################')
CompanyNetworkData=pd.merge( CompanyData,
CountryData, how='inner', on='Country_Code'
) print('################################')
print('Change ',CompanyNetworkData.columns.values) for i in CompanyNetworkData.columns.values:
j='Company_'+i CompanyNetworkData.rename(columns={i: j}, inplace=True) print('To ', CompanyNetworkData.columns.values) print('################################')
sFileName=Base + sOutputFileName print('################################')
print('Storing :', sFileName) print('################################')
CompanyNetworkData.to_csv(sFileName, index = False, encoding="latin-1") print('### Done!! #####################')
 
Output:



 
Assess-Network-Routing-Customer
Code:
import sys import os
import pandas as pd pd.options.mode.chained_assignment = None Base='D:/Dinesh/DS/5/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sInputFileName=Base + 'Assess-Network-Routing-Company.csv' sFileName=sInputFileName print('################################')
print('Loading :',sFileName) print('################################')
CustomerData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") print('Loaded Country:',CustomerData.columns.values) print('################################')
print(CustomerData.head()) print('################################') print('### Done!! #####################') print('################################')
 
Output:





Assess-Network-Routing-Node
Code:
import sys import os
import pandas as pd


pd.options.mode.chained_assignment = None Base='D:/Dinesh/DS/5/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sInputFileName= 'Retrieve_IP_DATA.csv'

sOutputFileName='Assess-Network-Routing-Node.csv' sFileName=Base + '/csv/' + sInputFileName print('################################')
 
print('Loading :',sFileName) print('################################')
IPData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") print('Loaded IP :', IPData.columns.values) print('################################') print('################################')
print('Changed :',IPData.columns.values) IPData.drop('RowID', axis=1, inplace=True)

IPData.rename(columns={'Country': 'Country_Code'}, inplace=True) IPData.rename(columns={'Place.Name': 'Place_Name'}, inplace=True) IPData.rename(columns={'Post.Code': 'Post_Code'}, inplace=True) IPData.rename(columns={'First.IP.Number': 'First_IP_Number'}, inplace=True) IPData.rename(columns={'Last.IP.Number': 'Last_IP_Number'}, inplace=True) print('To :',IPData.columns.values)

print('################################') print('################################')
print('Change ',IPData.columns.values)


for i in IPData.columns.values: j='Node_'+i
IPData.rename(columns={i: j}, inplace=True)

print('To ', IPData.columns.values) print('################################')
sFileDir=Base + '5bc'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)
sFileName=sFileDir + '/' + sOutputFileName print('################################')
 
print('Storing :', sFileName) print('################################')
IPData.to_csv(sFileName, index = False, encoding="latin-1") print('################################')
print('### Done!! #####################') print('################################')



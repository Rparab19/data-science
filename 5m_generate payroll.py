################################################################
import sys import os
import sqlite3 as sq import pandas as pd
################################################################
Base = 'D:/Dinesh/DS/5/'
print('Working Base :',Base, ' using ', sys.platform) ################################################################
sInputFileName1='csv/Retrieve-Data_female-names.csv' sInputFileName2='csv/Retrieve-Data_male-names.csv' sInputFileName3='csv/Retrieve-Data_last-names.csv' sOutputFileName1='Assess-Staff.csv' sOutputFileName2='Assess-Customers.csv'
################################################################
sDataBaseDir=Base + '/02-Assess/SQLite' if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir) ################################################################
sDatabaseName=sDataBaseDir + '/clark.db' conn = sq.connect(sDatabaseName)
################################################################
### Import Female Data ################################################################
sFileName=Base + '/' + sInputFileName1 print('Loading :',sFileName)
 
print(sFileName)
FemaleRawData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") FemaleRawData.rename(columns={'NameValues' : 'FirstName'},inplace=True) FemaleRawData.drop_duplicates(subset=None, keep='first', inplace=True) FemaleData=FemaleRawData.sample(100)
sTable='Assess_FemaleName'
print('Storing :',sDatabaseName,' Table:',sTable) FemaleData.to_sql(sTable, conn, if_exists="replace") print('Rows : ',FemaleData.shape[0], ' records') print('################################')
################################################################
### Import Male Data ################################################################
sFileName=Base + '/' + sInputFileName2 print('################################')
print('Loading :',sFileName) print('################################')
MaleRawData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") MaleRawData.rename(columns={'NameValues' : 'FirstName'},inplace=True) MaleRawData.drop_duplicates(subset=None, keep='first', inplace=True) MaleData=MaleRawData.sample(100)
sTable='Assess_MaleName'
print('Storing :',sDatabaseName,' Table:',sTable) MaleData.to_sql(sTable, conn, if_exists="replace") print('################')
print('Rows : ',MaleData.shape[0], ' records') print('################################') ################################################################
### Import Surname Data ################################################################
sFileName=Base + '/'+ sInputFileName3
 
print('Loading :',sFileName) SurnameRawData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") SurnameRawData.rename(columns={'NameValues' : 'LastName'},inplace=True) SurnameRawData.drop_duplicates(subset=None, keep='first', inplace=True) SurnameData=SurnameRawData.sample(200)
sTable='Assess_Surname'
print('Storing :',sDatabaseName,' Table:',sTable) SurnameData.to_sql(sTable, conn, if_exists="replace") print('Rows : ',SurnameData.shape[0], ' records') print('################')
sTable='Assess_FemaleName & Assess_MaleName' print('Loading :',sDatabaseName,' Table:',sTable) sSQL="select distinct"
sSQL=sSQL+ " A.FirstName," sSQL=sSQL+ " 'Female' as Gender" sSQL=sSQL+ " from"
sSQL=sSQL+ " Assess_FemaleName as A" sSQL=sSQL+ " UNION"
sSQL=sSQL+ " select distinct" sSQL=sSQL+ " A.FirstName," sSQL=sSQL+ " 'Male' as Gender" sSQL=sSQL+ " from"
sSQL=sSQL+ " Assess_MaleName as A;" FirstNameData=pd.read_sql_query(sSQL, conn) print('################')
sTable='Assess_FirstName'
print('Storing :',sDatabaseName,' Table:',sTable) FirstNameData.to_sql(sTable, conn, if_exists="replace") sTable='Assess_FirstName x2 & Assess_Surname' print('Loading :',sDatabaseName,' Table:',sTable) sSQL="select distinct"
 
sSQL=sSQL+ " A.FirstName,"
sSQL=sSQL+ " B.FirstName AS SecondName," sSQL=sSQL+ " C.LastName,"
sSQL=sSQL+ " A.Gender" sSQL=sSQL+ " from"
sSQL=sSQL+ " Assess_FirstName as A" sSQL=sSQL+ " ,"
sSQL=sSQL+ " Assess_FirstName as B" sSQL=sSQL+ " ,"
sSQL=sSQL+ " Assess_Surname as C" sSQL=sSQL+ " WHERE"
sSQL=sSQL+ " A.Gender = B.Gender" sSQL=sSQL+ " AND"
sSQL=sSQL+ " A.FirstName <> B.FirstName;" PeopleRawData=pd.read_sql_query(sSQL, conn) People1Data=PeopleRawData.sample(10000)

sTable='Assess_FirstName & Assess_Surname' print('Loading :',sDatabaseName,' Table:',sTable) sSQL="select distinct"
sSQL=sSQL+ " A.FirstName," sSQL=sSQL+ " '' AS SecondName," sSQL=sSQL+ " B.LastName," sSQL=sSQL+ " A.Gender" sSQL=sSQL+ " from"
sSQL=sSQL+ " Assess_FirstName as A" sSQL=sSQL+ " ,"
sSQL=sSQL+ " Assess_Surname as B;" PeopleRawData=pd.read_sql_query(sSQL, conn) People2Data=PeopleRawData.sample(10000)
PeopleData = pd.concat([People1Data, People2Data], ignore_index=True)
 
print(PeopleData) #################################################################
sTable='Assess_People'
print('Storing :',sDatabaseName,' Table:',sTable) PeopleData.to_sql(sTable, conn, if_exists="replace")
################################################################
sFileDir=Base + '/' + '/02-Assess/01-EDS/02-Python' if not os.path.exists(sFileDir):
os.makedirs(sFileDir) sOutputFileName = sTable+'.csv'
sFileName=sFileDir + '/' + sOutputFileName print('Storing :', sFileName) PeopleData.to_csv(sFileName, index = False)
print('### Done!! ############################################')

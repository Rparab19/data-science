Transform Superstep
Code:
import sys import os
from datetime import datetime from pytz import timezone import pandas as pd
import sqlite3 as sq import uuid
 
Practical: 7 Transforming data
 

pd.options.mode.chained_assignment = None Base = 'D:/Dinesh/DS/7/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
InputFileName='csv/VehicleData.csv' sDataBaseDir=Base + '/' + '/04-Transform/SQLite' if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir) sDatabaseName=sDataBaseDir + '/Vermeulen.db' conn1 = sq.connect(sDatabaseName) sDataVaultDir=Base + '/88-DV'
if not os.path.exists(sDataVaultDir): os.makedirs(sDataVaultDir)
sDatabaseName=sDataVaultDir + '/datavault.db' conn2 = sq.connect(sDatabaseName) sDataWarehouseDir=Base + '/99-DW'
if not os.path.exists(sDataWarehouseDir):
 
os.makedirs(sDataWarehouseDir) sDatabaseName=sDataWarehouseDir + '/datawarehouse.db' conn3 = sq.connect(sDatabaseName) print('\n#################################')
print('Time Category') print('UTC Time')
BirthDateUTC = datetime(1960,12,20,10,15,0) BirthDateZoneUTC=BirthDateUTC.replace(tzinfo=timezone('UTC')) BirthDateZoneStr=BirthDateZoneUTC.strftime("%Y-%m-%d %H:%M:%S") BirthDateZoneUTCStr=BirthDateZoneUTC.strftime("%Y-%m-%d %H:%M:%S (%Z) (%z)") print(BirthDateZoneUTCStr)
print('#################################')
print('Birth Date in Reykjavik :') BirthZone = 'Atlantic/Reykjavik'
BirthDate = BirthDateZoneUTC.astimezone(timezone(BirthZone)) BirthDateStr=BirthDate.strftime("%Y-%m-%d %H:%M:%S (%Z) (%z)") BirthDateLocal=BirthDate.strftime("%Y-%m-%d %H:%M:%S") print(BirthDateStr)
print('#################################') ################################################################
IDZoneNumber=str(uuid.uuid4()) sDateTimeKey=BirthDateZoneStr.replace(' ','-').replace(':','-') TimeLine=[('ZoneBaseKey', ['UTC']),
('IDNumber', [IDZoneNumber]), ('DateTimeKey', [sDateTimeKey]), ('UTCDateTimeValue', [BirthDateZoneUTC]), ('Zone', [BirthZone]),
('DateTimeValue', [BirthDateStr])] TimeFrame = pd.DataFrame(dict(TimeLine))

TimeHub=TimeFrame[['IDNumber','ZoneBaseKey','DateTimeKey','DateTimeValue']]
 
TimeHubIndex=TimeHub.set_index(['IDNumber'],inplace=False) sTable = 'Hub-Time-Gunnarsson' print('\n#################################')
print('Storing :',sDatabaseName,'\n Table:',sTable) print('\n#################################')
TimeHubIndex.to_sql(sTable, conn2, if_exists="replace") sTable = 'Dim-Time-Gunnarsson' TimeHubIndex.to_sql(sTable, conn3, if_exists="replace")

TimeSatellite=TimeFrame[['IDNumber','DateTimeKey','Zone','DateTimeValue']] TimeSatelliteIndex=TimeSatellite.set_index(['IDNumber'],inplace=False) BirthZoneFix=BirthZone.replace(' ','-').replace('/','-')
sTable = 'Satellite-Time-' + BirthZoneFix + '-Gunnarsson' print('\n#################################')
print('Storing :',sDatabaseName,'\n Table:',sTable) print('\n#################################')
TimeSatelliteIndex.to_sql(sTable, conn2, if_exists="replace") sTable = 'Dim-Time-' + BirthZoneFix + '-Gunnarsson' TimeSatelliteIndex.to_sql(sTable, conn3, if_exists="replace")

print('\n#################################')
print('Person Category') FirstName = 'Guðmundur' LastName = 'Gunnarsson'
print('Name:',FirstName,LastName) print('Birth Date:',BirthDateLocal) print('Birth Zone:',BirthZone)
print('UTC Birth Date:',BirthDateZoneStr) print('#################################')

IDPersonNumber=str(uuid.uuid4())
 
PersonLine=[('IDNumber', [IDPersonNumber]), ('FirstName', [FirstName]),
('LastName', [LastName]),
('Zone', ['UTC']),
('DateTimeValue', [BirthDateZoneStr])] PersonFrame = pd.DataFrame(dict(PersonLine)) TimeHub=PersonFrame
TimeHubIndex=TimeHub.set_index(['IDNumber'],inplace=False) sTable = 'Hub-Person-Gunnarsson' print('\n#################################')
print('Storing :',sDatabaseName,'\n Table:',sTable) print('\n#################################')
TimeHubIndex.to_sql(sTable, conn2, if_exists="replace") sTable = 'Dim-Person-Gunnarsson' TimeHubIndex.to_sql(sTable, conn3, if_exists="replace")

Output:

 
Gunnarsson-Sun-Model
Code:
import sys import os
from datetime import datetime from pytz import timezone import pandas as pd
import sqlite3 as sq import uuid

pd.options.mode.chained_assignment = None Base = 'D:/Dinesh/DS/7/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sDataBaseDir=Base + '/' + '/04-Transform/SQLite' if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir) sDatabaseName=sDataBaseDir + '/Vermeulen.db' conn1 = sq.connect(sDatabaseName) sDataWarehousetDir=Base + '/99-DW'
if not os.path.exists(sDataWarehousetDir): os.makedirs(sDataWarehousetDir)
sDatabaseName=sDataWarehousetDir + '/datawarehouse.db' conn2 = sq.connect(sDatabaseName)
print('Time Dimension') BirthZone = 'Atlantic/Reykjavik'

BirthDateUTC = datetime(1960,12,20,10,15,0) BirthDateZoneUTC=BirthDateUTC.replace(tzinfo=timezone('UTC')) BirthDateZoneStr=BirthDateZoneUTC.strftime("%Y-%m-%d %H:%M:%S")
 
BirthDateZoneUTCStr=BirthDateZoneUTC.strftime("%Y-%m-%d %H:%M:%S (%Z) (%z)") BirthDate = BirthDateZoneUTC.astimezone(timezone(BirthZone)) BirthDateStr=BirthDate.strftime("%Y-%m-%d %H:%M:%S (%Z) (%z)") BirthDateLocal=BirthDate.strftime("%Y-%m-%d %H:%M:%S") IDTimeNumber=str(uuid.uuid4())

TimeLine=[('TimeID', [IDTimeNumber]), ('UTCDate', [BirthDateZoneStr]), ('LocalTime', [BirthDateLocal]), ('TimeZone', [BirthZone])]
TimeFrame = pd.DataFrame(dict(TimeLine)) DimTime=TimeFrame DimTimeIndex=DimTime.set_index(['TimeID'],inplace=False) sTable = 'Dim-Time'

print('Storing :',sDatabaseName,'\n Table:',sTable) DimTimeIndex.to_sql(sTable, conn1, if_exists="replace") DimTimeIndex.to_sql(sTable, conn2, if_exists="replace") print('Dimension Person')
FirstName = 'Guðmundur' LastName = 'Gunnarsson' IDPersonNumber=str(uuid.uuid4())

PersonLine=[('PersonID', [IDPersonNumber]), ('FirstName', [FirstName]),
('LastName', [LastName]),
('Zone', ['UTC']),
('DateTimeValue', [BirthDateZoneStr])] PersonFrame = pd.DataFrame(dict(PersonLine)) DimPerson=PersonFrame
DimPersonIndex=DimPerson.set_index(['PersonID'],inplace=False)
 
sTable = 'Dim-Person'
print('Storing :',sDatabaseName,'\n Table:',sTable) DimPersonIndex.to_sql(sTable, conn1, if_exists="replace") DimPersonIndex.to_sql(sTable, conn2, if_exists="replace") print('Fact - Person - time') IDFactNumber=str(uuid.uuid4()) PersonTimeLine=[('IDNumber', [IDFactNumber]),
('IDPersonNumber', [IDPersonNumber]), ('IDTimeNumber', [IDTimeNumber])]
PersonTimeFrame = pd.DataFrame(dict(PersonTimeLine)) FctPersonTime=PersonTimeFrame FctPersonTimeIndex=FctPersonTime.set_index(['IDNumber'],inplace=False) sTable = 'Fact-Person-Time'
print('Storing :',sDatabaseName,'\n Table:',sTable) FctPersonTimeIndex.to_sql(sTable, conn1, if_exists="replace") FctPersonTimeIndex.to_sql(sTable, conn2, if_exists="replace")



Output:



 
Building a Data Warehouse
Code:
import sys import os
from datetime import datetime from pytz import timezone import pandas as pd
import sqlite3 as sq import uuid
pd.options.mode.chained_assignment = None Base = 'D:/Dinesh/DS/7/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sDatabaseName='01-Vermeulen/04-Transform/SQLite/Vermeulen.db' conn1 = sq.connect(sDatabaseName)
sDatabaseName= 'csv/datavault.db' conn2 = sq.connect(sDatabaseName) sDatabaseName= 'csv/datawarehouse.db' conn3 = sq.connect(sDatabaseName)
sSQL=" SELECT DateTimeValue FROM [Hub-Time];" DateDataRaw=pd.read_sql_query(sSQL, conn2) DateData=DateDataRaw.head(1000)
print(DateData) print('\n#################################')
print('Time Dimension') print('\n#################################')
t=0 mt=DateData.shape[0] for i in range(mt):
BirthZone = ('Atlantic/Reykjavik','Europe/London','UCT')
 
for j in range(len(BirthZone)): t+=1
print(t,mt*3)
BirthDateUTC = datetime.strptime(DateData['DateTimeValue'][i],"%Y-%m-%d
%H:%M:%S")
BirthDateZoneUTC=BirthDateUTC.replace(tzinfo=timezone('UTC')) BirthDateZoneStr=BirthDateZoneUTC.strftime("%Y-%m-%d %H:%M:%S") BirthDateZoneUTCStr=BirthDateZoneUTC.strftime("%Y-%m-%d %H:%M:%S (%Z)
(%z)")
BirthDate = BirthDateZoneUTC.astimezone(timezone(BirthZone[j])) BirthDateStr=BirthDate.strftime("%Y-%m-%d %H:%M:%S (%Z) (%z)") BirthDateLocal=BirthDate.strftime("%Y-%m-%d %H:%M:%S") IDTimeNumber=str(uuid.uuid4())
TimeLine=[('TimeID', [str(IDTimeNumber)]), ('UTCDate', [str(BirthDateZoneStr)]), ('LocalTime', [str(BirthDateLocal)]), ('TimeZone', [str(BirthZone)])]
if t==1:
TimeFrame = pd.DataFrame(dict(TimeLine)) else:
TimeRow = pd.DataFrame(dict(TimeLine))
TimeFrame = pd.concat([TimeFrame, TimeRow], ignore_index=True) DimTime=TimeFrame DimTimeIndex=DimTime.set_index(['TimeID'],inplace=False)
sTable = 'Dim-Time' print('\n#################################')
print('Storing :',sDatabaseName,'\n Table:',sTable) print('\n#################################')
DimTimeIndex.to_sql(sTable, conn1, if_exists="replace") DimTimeIndex.to_sql(sTable, conn3, if_exists="replace") sSQL=" SELECT " + \
 
" FirstName," + \
" SecondName," + \ " LastName," + \
" BirthDateKey " + \
" FROM [Hub-Person];" PersonDataRaw=pd.read_sql_query(sSQL, conn2) PersonData=PersonDataRaw.head(1000) print('\n#################################')
print('Dimension Person') print('\n#################################')
t=0 mt=DateData.shape[0] for i in range(mt):
t+=1
print(t,mt)
FirstName = str(PersonData["FirstName"]) SecondName = str(PersonData["SecondName"]) if len(SecondName) > 0:
SecondName=""
LastName = str(PersonData["LastName"]) BirthDateKey = str(PersonData["BirthDateKey"]) IDPersonNumber=str(uuid.uuid4()) PersonLine=[('PersonID', [str(IDPersonNumber)]),
('FirstName', [FirstName]), ('SecondName', [SecondName]), ('LastName', [LastName]), ('Zone', [str('UTC')]),
('BirthDate', [BirthDateKey])]
if t==1:
PersonFrame = pd.DataFrame.from_items(PersonLine) else:
 
PersonRow = pd.DataFrame.from_items(PersonLine) PersonFrame = PersonFrame.append(PersonRow)
DimPerson=PersonFrame print(DimPerson)
DimPersonIndex=DimPerson.set_index(['PersonID'],inplace=False) sTable = 'Dim-Person' print('\n#################################')
print('Storing :',sDatabaseName,'\n Table:',sTable) print('\n#################################')
DimPersonIndex.to_sql(sTable, conn1, if_exists="replace") DimPersonIndex.to_sql(sTable, conn3, if_exists="replace")



Output:



 
Simple Linear Regression
Code:
import sys import os
import pandas as pd import sqlite3 as sq
import matplotlib.pyplot as plt import numpy as np
from sklearn import datasets
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score Base = 'D:/Dinesh/DS/7/'

print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')

sDatabaseName = 'csv/Vermeulen.db' conn1 = sq.connect(sDatabaseName)

sDatabaseName = 'csv/datavault.db' conn2 = sq.connect(sDatabaseName) sDataWarehouseDir = Base + '/99-DW'
if not os.path.exists(sDataWarehouseDir): os.makedirs(sDataWarehouseDir)
sDatabaseName = sDataWarehouseDir + '/datawarehouse.db' conn3 = sq.connect(sDatabaseName)
t = 0

tMax = ((300 - 100) / 10) * ((300 - 30) / 5)
PersonFrame = pd.DataFrame(columns=['PersonID', 'Height', 'Weight', 'bmi', 'Indicator'])
 
for heightSelect in range(100, 300, 10):
for weightSelect in range(30, 300, 5):
height = round(heightSelect / 100, 3) weight = int(weightSelect)
bmi = weight / (height * height)


# Determine the BMI category if bmi <= 18.5:
BMI_Result = 1
elif 18.5 < bmi < 25:
BMI_Result = 2
elif 25 < bmi < 30:
BMI_Result = 3 elif bmi > 30:
BMI_Result = 4 else:
BMI_Result = 0


PersonLine = pd.DataFrame([{ 'PersonID': str(t),
'Height': height, 'Weight': weight, 'bmi': bmi,
'Indicator': BMI_Result
}])


PersonLine = PersonLine[PersonFrame.columns] PersonLine = PersonLine.dropna(axis=1, how='all')
PersonFrame = pd.concat([PersonFrame, PersonLine], ignore_index=True) t += 1
 
print('Row:', t, 'of', tMax)


DimPerson = PersonFrame
DimPersonIndex = DimPerson.set_index(['PersonID'], inplace=False) sTable = 'Transform-BMI' print('\n#################################')
print('Storing :', sDatabaseName, '\n Table:', sTable) print('\n#################################')
DimPersonIndex.to_sql(sTable, conn1, if_exists="replace") sTable = 'Person-Satellite-BMI' print('\n#################################')
print('Storing :', sDatabaseName, '\n Table:', sTable) print('\n#################################')
DimPersonIndex.to_sql(sTable, conn2, if_exists="replace") sTable = 'Dim-BMI' print('\n#################################')
print('Storing :', sDatabaseName, '\n Table:', sTable) print('\n#################################')
DimPersonIndex.to_sql(sTable, conn3, if_exists="replace") fig = plt.figure()
PlotPerson = DimPerson[DimPerson['Indicator'] == 1] # Underweight x = PlotPerson['Height']
y = PlotPerson['Weight']
plt.plot(x, y, ".", label="Underweight")


PlotPerson = DimPerson[DimPerson['Indicator'] == 2] # Normal weight x = PlotPerson['Height']
y = PlotPerson['Weight']
plt.plot(x, y, "o", label="Normal weight")


PlotPerson = DimPerson[DimPerson['Indicator'] == 3] # Overweight
 
x = PlotPerson['Height'] y = PlotPerson['Weight']
plt.plot(x, y, "+", label="Overweight")


PlotPerson = DimPerson[DimPerson['Indicator'] == 4] # Obese x = PlotPerson['Height']
y = PlotPerson['Weight'] plt.plot(x, y, "^", label="Obese")

plt.axis('tight') plt.title("BMI Curve") plt.xlabel("Height (meters)") plt.ylabel("Weight (kg)") plt.legend()
plt.show()


diabetes = datasets.load_diabetes()


diabetes_X = diabetes.data[:, np.newaxis, 2] diabetes_X_train = diabetes_X[:-30] diabetes_X_test = diabetes_X[-50:] diabetes_y_train = diabetes.target[:-30] diabetes_y_test = diabetes.target[-50:]
regr = LinearRegression() regr.fit(diabetes_X_train, diabetes_y_train) diabetes_y_pred = regr.predict(diabetes_X_test)

print('Coefficients: \n', regr.coef_)
print("Mean squared error: %.2f" % mean_squared_error(diabetes_y_test, diabetes_y_pred)) print('Variance score: %.2f' % r2_score(diabetes_y_test, diabetes_y_pred))
 
plt.figure(figsize=(8, 6))
plt.scatter(diabetes_X_test, diabetes_y_test, color='black') # Test data plt.plot(diabetes_X_test, diabetes_y_pred, color='blue', linewidth=3) # Regression line plt.xticks(())
plt.yticks(())
plt.title("Diabetes Progression Prediction") plt.xlabel("BMI")
plt.ylabel("Disease Progression") plt.show()


Output:

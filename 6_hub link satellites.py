Process Time
Code:
import sys import os
 
Practical: 6
Build the time hub, links and satellites
 
from datetime import datetime, timedelta from pytz import timezone, all_timezones import pandas as pd
import sqlite3 as sq
from pandas.io import sql import uuid

# Disable chained assignment warnings pd.options.mode.chained_assignment = None

# Base directory
Base = 'D:/Dinesh/DS/6/' print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')

# Input file name
InputFileName = 'csv/VehicleData.csv'


# Create directories if they don't exist
sDataBaseDir = os.path.join(Base, '03-Process', 'SQLite') if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir)
print("Created database directory:", sDataBaseDir)
 
# Database file path
sDatabaseName = os.path.join(sDataBaseDir, 'Hillman.db') conn1 = sq.connect(sDatabaseName)

# Directory for the second database (datavault.db) sDataVaultDir = os.path.join(Base, '88-DV')
if not os.path.exists(sDataVaultDir): os.makedirs(sDataVaultDir)
print("Created data vault directory:", sDataVaultDir)


# Database file path for second database
sDatabaseName = os.path.join(sDataVaultDir, 'datavault.db') print("Database Path for conn2:", sDatabaseName)

# Try to connect to the second database try:
conn2 = sq.connect(sDatabaseName)
print("Database connection to datavault.db successful!") except sq.OperationalError as e:
print(f"Error connecting to database: {e}")


# Generate datetime range
base = datetime(2018, 1, 1, 0, 0, 0)
numUnits = 1 * 365 * 24 # 1 years worth of hourly data

# Create list of datetime objects
date_list = [base - timedelta(hours=x) for x in range(0, numUnits)]


# Initialize variables for DataFrame creation # Initialize variables for DataFrame creation
 
t = 0
TimeFrame = pd.DataFrame()


# Loop to create the time data for i in date_list:
now_utc = i.replace(tzinfo=timezone('UTC'))
sDateTime = now_utc.strftime("%Y-%m-%d %H:%M:%S") sDateTimeKey = sDateTime.replace(' ', '-').replace(':', '-')
t += 1
IDNumber = str(uuid.uuid4())


# Construct the dictionary for the DataFrame TimeLineDict = {
'ZoneBaseKey': ['UTC'], 'IDNumber': [IDNumber], 'nDateTimeValue': [now_utc], 'DateTimeValue': [sDateTime], 'DateTimeKey': [sDateTimeKey]
}


# Create the DataFrame
TimeRow = pd.DataFrame(TimeLineDict)


# Concatenate the new row to the main TimeFrame DataFrame if t == 1:
TimeFrame = TimeRow else:
TimeFrame = pd.concat([TimeFrame, TimeRow], ignore_index=True)

# Set TimeHub DataFrame and its index
TimeHub = TimeFrame[['IDNumber', 'ZoneBaseKey', 'DateTimeKey', 'DateTimeValue']]
 
TimeHubIndex = TimeHub.set_index(['IDNumber'], inplace=False)


# Store TimeHub data in SQLite databases sTable = 'Process-Time'
print(f'Storing : {sDatabaseName} Table: {sTable}') TimeHubIndex.to_sql(sTable, conn1, if_exists="replace")

sTable = 'Hub-Time'
print(f'Storing : {sDatabaseName} Table: {sTable}') TimeHubIndex.to_sql(sTable, conn2, if_exists="replace")

# Time zone conversion active_timezones = all_timezones z = 0

# Loop through time zones for zone in active_timezones:
t = 0
for j in range(TimeFrame.shape[0]):
now_date = TimeFrame['nDateTimeValue'][j] DateTimeKey = TimeFrame['DateTimeKey'][j] now_utc = now_date.replace(tzinfo=timezone('UTC'))
sDateTime = now_utc.strftime("%Y-%m-%d %H:%M:%S") now_zone = now_utc.astimezone(timezone(zone))
sZoneDateTime = now_zone.strftime("%Y-%m-%d %H:%M:%S") print(sZoneDateTime)
t += 1
z += 1
IDZoneNumber = str(uuid.uuid4())


# Construct the dictionary for the DataFrame
 
TimeZoneLineDict = { 'ZoneBaseKey': ['UTC'], 'IDZoneNumber': [IDZoneNumber], 'DateTimeKey': [DateTimeKey], 'UTCDateTimeValue': [sDateTime], 'Zone': [zone],
'DateTimeValue': [sZoneDateTime]
}


# Create DataFrame from dictionary
TimeZoneRow = pd.DataFrame(TimeZoneLineDict)


# Append the row to the DataFrame if t == 1:
TimeZoneFrame = TimeZoneRow else:
TimeZoneFrame = pd.concat([TimeZoneFrame, TimeZoneRow], ignore_index=True)


# Set index for TimeZoneFrame
TimeZoneFrameIndex = TimeZoneFrame.set_index(['IDZoneNumber'], inplace=False)


# Clean up zone names for table names sZone = zone.replace('/', '-').replace(' ', '')

# Store time zone data in SQLite databases sTable = 'Process-Time-' + sZone
print(f'Storing : {sDatabaseName} Table: {sTable}') TimeZoneFrameIndex.to_sql(sTable, conn1, if_exists="replace")

sTable = 'Satellite-Time-' + sZone
print(f'Storing : {sDatabaseName} Table: {sTable}')
 
TimeZoneFrameIndex.to_sql(sTable, conn2, if_exists="replace")



# Vacuum the databases to optimize print('################')
print('Vacuum Databases') sSQL = "VACUUM;"
sql.execute(sSQL, conn1) sql.execute(sSQL, conn2) print('################')

# Final message
print('### Done!! ############################################')




Golden Nominal
Code:
import sys import os
import sqlite3 as sq import pandas as pd
from pandas.io import sql
from datetime import datetime, timedelta from pytz import timezone, all_timezones from random import randint
import uuid


Base = 'D:/Dinesh/DS/6/' print('################################')
print('Working Base :', Base, ' using ', sys.platform)
 
print('################################')


sInputFileName = 'csv/Assess_People.csv'


sDataBaseDir = Base + '/' + '/03-Process/SQLite' if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir)


sDatabaseName = sDataBaseDir + '/clark.db' conn1 = sq.connect(sDatabaseName)

sDataVaultDir = Base + '/88-DV'
if not os.path.exists(sDataVaultDir): os.makedirs(sDataVaultDir)

sDatabaseName = sDataVaultDir + '/datavault.db' conn2 = sq.connect(sDatabaseName)

### Import Female Data
sFileName = Base + '/' + '/' + sInputFileName print('################################')
print('Loading :', sFileName) print('################################')
print(sFileName)
RawData = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin-1") RawData.drop_duplicates(subset=None, keep='first', inplace=True)

start_date = datetime(1900, 1, 1, 0, 0, 0)
start_date_utc = start_date.replace(tzinfo=timezone('UTC')) HoursBirth = 100 * 365 * 24
 
RawData['BirthDateUTC'] = RawData.apply(lambda row: (start_date_utc + timedelta(hours=randint(0, HoursBirth)))
, axis=1)
zonemax = len(all_timezones) - 1 RawData['TimeZone'] = RawData.apply(lambda row:
(all_timezones[randint(0, zonemax)])
, axis=1)
RawData['BirthDateISO'] = RawData.apply(lambda row: row["BirthDateUTC"].astimezone(timezone(row['TimeZone']))
, axis=1)
RawData['BirthDateKey'] = RawData.apply(lambda row: row["BirthDateUTC"].strftime("%Y-%m-%d %H:%M:%S")
, axis=1)
RawData['BirthDate'] = RawData.apply(lambda row: row["BirthDateISO"].strftime("%Y-%m-%d %H:%M:%S")
, axis=1)
RawData['PersonID'] = RawData.apply(lambda row: str(uuid.uuid4())
, axis=1)


Data = RawData.copy() Data.drop('BirthDateUTC', axis=1, inplace=True) Data.drop('BirthDateISO', axis=1, inplace=True) indexed_data = Data.set_index(['PersonID'])

print('################################') print('################')
sTable = 'Process_Person'
print('Storing :', sDatabaseName, ' Table:', sTable) indexed_data.to_sql(sTable, conn1, if_exists="replace") print('################')
 
PersonHubRaw = Data[['PersonID', 'FirstName', 'SecondName', 'LastName', 'BirthDateKey']].copy()
PersonHubRaw['PersonHubID'] = RawData.apply(lambda row: str(uuid.uuid4())
, axis=1)
PersonHub = PersonHubRaw.drop_duplicates(subset=None,
keep='first', inplace=False)
indexed_PersonHub = PersonHub.set_index(['PersonHubID']) sTable = 'Hub-Person'
print('Storing :', sDatabaseName, ' Table:', sTable) indexed_PersonHub.to_sql(sTable, conn2, if_exists="replace")

PersonSatelliteGenderRaw = Data[['PersonID', 'FirstName', 'SecondName', 'LastName', 'BirthDateKey', 'Gender']].copy()
PersonSatelliteGenderRaw['PersonSatelliteID'] = RawData.apply(lambda row: str(uuid.uuid4())
, axis=1)
PersonSatelliteGender = PersonSatelliteGenderRaw.drop_duplicates(subset=None,
keep='first', inplace=False)
indexed_PersonSatelliteGender = PersonSatelliteGender.set_index(['PersonSatelliteID']) sTable = 'Satellite-Person-Gender'
print('Storing :', sDatabaseName, ' Table:', sTable) indexed_PersonSatelliteGender.to_sql(sTable, conn2, if_exists="replace")

PersonSatelliteBirthdayRaw = Data[['PersonID', 'FirstName', 'SecondName', 'LastName', 'BirthDateKey', 'TimeZone', 'BirthDate']].copy()
PersonSatelliteBirthdayRaw['PersonSatelliteID'] = RawData.apply(lambda row: str(uuid.uuid4())
 
, axis=1)
PersonSatelliteBirthday = PersonSatelliteBirthdayRaw.drop_duplicates(subset=None,
keep='first', inplace=False)
indexed_PersonSatelliteBirthday = PersonSatelliteBirthday.set_index(['PersonSatelliteID']) sTable = 'Satellite-Person-Names'
print('Storing :', sDatabaseName, ' Table:', sTable) indexed_PersonSatelliteBirthday.to_sql(sTable, conn2, if_exists="replace")

sFileDir = Base + '/' + '/03-Process/01-EDS/02-Python' if not os.path.exists(sFileDir):
os.makedirs(sFileDir)


sOutputFileName = sTable + '.csv'
sFileName = sFileDir + '/' + sOutputFileName print('################################')
print('Storing :', sFileName) print('################################')
RawData.to_csv(sFileName, index=False) print('################################')

print('################')
print('Vacuum Databases') sSQL = "VACUUM;"
conn1.execute(sSQL) conn2.execute(sSQL) print('################')
print('### Done!! ############################################')
 
Output:






Classification of vehicles
Code:
import sys import os
import pandas as pd import sqlite3 as sq import uuid
pd.options.mode.chained_assignment = None Base = 'D:/Dinesh/DS/6/' print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')
InputFileName = 'csv/VehicleData.csv' sDataBaseDir = Base + '/03-Process/SQLite' if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir)
 
sDataVaultDir = Base + '/88-DV'
if not os.path.exists(sDataVaultDir): os.makedirs(sDataVaultDir)
sDatabaseName = sDataBaseDir + '/Hillman.db' conn1 = sq.connect(sDatabaseName) sDatabaseName = sDataVaultDir + '/datavault.db' conn2 = sq.connect(sDatabaseName)
sFileName = Base + '/' + InputFileName print('###########')
print('Loading :', sFileName)
VehicleRaw = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin-1") sTable = 'Process_Vehicles'
print('Storing :', sDatabaseName, ' Table:', sTable) VehicleRaw.to_sql(sTable, conn1, if_exists="replace") VehicleRawKey = VehicleRaw[['Make', 'Model']].copy() VehicleKey = VehicleRawKey.drop_duplicates() VehicleKey['ObjectKey'] = VehicleKey.apply(lambda row:
str('(' + str(row['Make']).strip().replace(' ', '-').replace('/', '-').lower() +
')-(' + (str(row['Model']).strip().replace(' ', '-').lower()) + ')'
), axis=1) VehicleKey['ObjectType'] = 'vehicle'
VehicleKey['ObjectUUID'] = VehicleKey.apply(lambda row: str(uuid.uuid4()), axis=1) VehicleHub = VehicleKey[['ObjectType', 'ObjectKey', 'ObjectUUID']].copy() VehicleHub.index.name = 'ObjectHubID'
sTable = 'Hub-Object-Vehicle'
print('Storing :', sDatabaseName, ' Table:', sTable) VehicleHub.to_sql(sTable, conn2, if_exists="replace")
VehicleSatellite = VehicleKey[['ObjectType', 'ObjectKey', 'ObjectUUID', 'Make', 'Model']].copy()
VehicleSatellite.index.name = 'ObjectSatelliteID'
 
sTable = 'Satellite-Object-Make-Model' print('Storing :', sDatabaseName, ' Table:', sTable)
VehicleSatellite.to_sql(sTable, conn2, if_exists="replace") sView = 'Dim-Object'
print('Storing :', sDatabaseName, ' View:', sView) cursor1 = conn1.cursor()
cursor2 = conn2.cursor() sSQL = """
CREATE VIEW IF NOT EXISTS [Dim-Object] AS SELECT DISTINCT
H.ObjectType,
H.ObjectKey AS VehicleKey, TRIM(S.Make) AS VehicleMake, TRIM(S.Model) AS VehicleModel
FROM
[Hub-Object-Vehicle] AS H JOIN
[Satellite-Object-Make-Model] AS S ON
H.ObjectType = S.ObjectType AND
H.ObjectUUID = S.ObjectUUID;""" cursor2.execute(sSQL)
conn2.commit() # Query View sSQL = """
SELECT DISTINCT
VehicleMake, VehicleModel
FROM
[Dim-Object]
 
ORDER BY
VehicleMake, VehicleModel; """ DimObjectData = pd.read_sql_query(sSQL, conn2) DimObjectData.index.name = 'ObjectDimID'
DimObjectData.sort_values(['VehicleMake', 'VehicleModel'], inplace=True, ascending=True) print('################')
print(DimObjectData) print('################')
print('Vacuum Databases') cursor1.execute("VACUUM;") cursor2.execute("VACUUM;") conn1.commit() conn2.commit() print('################')
cursor1.close() cursor2.close()


Output:

 
Human-Environment Interaction
Code:
import sys import os
import pandas as pd import sqlite3 as sq import uuid
Base = 'D:/Dinesh/DS/6/' print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')

InputAssessGraphName = 'csv/Assess_All_Animals.gml' EDSAssessDir = '02-Assess/01-EDS'
InputAssessDir = EDSAssessDir + '/02-Python'


sFileAssessDir = Base + '/' + InputAssessDir if not os.path.exists(sFileAssessDir):
os.makedirs(sFileAssessDir)
sDataBaseDir = Base + '/' + '/03-Process/SQLite' if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir)
sDatabaseName = sDataBaseDir + '/Vermeulen.db' conn1 = sq.connect(sDatabaseName) sDataVaultDir = Base + '/88-DV'
if not os.path.exists(sDataVaultDir): os.makedirs(sDataVaultDir)
sDatabaseName = sDataVaultDir + '/datavault.db' conn2 = sq.connect(sDatabaseName)
t = 0
tMax = 360 * 180
 
for Longitude in range(-180, 180, 10):
for Latitude in range(-90, 90, 10):
t += 1
IDNumber = str(uuid.uuid4())
LocationName = 'L' + format(round(Longitude, 3) * 1000, '+07d') + \ '-' + format(round(Latitude, 3) * 1000, '+07d')
print('Create:', t, ' of ', tMax, ':', LocationName)


LocationLine = { 'ObjectBaseKey': ['GPS'], 'IDNumber': [IDNumber], 'LocationNumber': [str(t)],
'LocationName': [LocationName], 'Longitude': [Longitude], 'Latitude': [Latitude]
}


if t == 1:
LocationFrame = pd.DataFrame(LocationLine) else:
LocationRow = pd.DataFrame(LocationLine)
LocationFrame = pd.concat([LocationFrame, LocationRow], ignore_index=True) LocationHubIndex = LocationFrame.set_index(['IDNumber'], inplace=False)
sTable = 'Process-Location'
print('Storing :', sDatabaseName, ' Table:', sTable) LocationHubIndex.to_sql(sTable, conn1, if_exists="replace") sTable = 'Hub-Location'
print('Storing :', sDatabaseName, ' Table:', sTable) LocationHubIndex.to_sql(sTable, conn2, if_exists="replace") print('################')
print('Vacuum Databases')
 
cursor1 = conn1.cursor() cursor1.execute("VACUUM;")

conn1.commit() cursor1.close()

cursor2 = conn2.cursor() cursor2.execute("VACUUM;")

conn2.commit() cursor2.close() print('################')
print('### Done!! ############################################')


Output:



 
Forecasting
Code:
import sys import os
import sqlite3 as sq import quandl import pandas as pd
from requests.exceptions import HTTPError quandl.ApiConfig.api_key = 'dfgdfgdfg' Base = 'D:/Dinesh/DS/6/'
print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')

sInputFileName = 'csv/VKHCG_Shares.csv' sOutputFileName = 'Shares.csv'
sDataBaseDir = os.path.join(Base, '03-Process', 'SQLite') os.makedirs(sDataBaseDir, exist_ok=True)

sFileDir1 = os.path.join(Base, '01-Retrieve', '01-EDS', '02-Python') os.makedirs(sFileDir1, exist_ok=True)

sFileDir2 = os.path.join(Base, '02-Assess', '01-EDS', '02-Python') os.makedirs(sFileDir2, exist_ok=True)

sFileDir3 = os.path.join(Base, '03-Process', '01-EDS', '02-Python') os.makedirs(sFileDir3, exist_ok=True)
sDatabaseName = os.path.join(sDataBaseDir, 'clark.db') conn = sq.connect(sDatabaseName)
sFileName = os.path.join(Base, sInputFileName) print('################################')
 
print('Loading :', sFileName) print('################################')

RawData = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin-1") RawData.drop_duplicates(subset=None, keep='first', inplace=True)
print('Rows :', RawData.shape[0]) print('Columns:', RawData.shape[1]) print('################')
for file_dir, prefix in zip([sFileDir1, sFileDir2, sFileDir3], ['Retrieve', 'Assess', 'Process']): sFileName = os.path.join(file_dir, f'{prefix}_{sOutputFileName}') print(f'################################\nStoring :
{sFileName}\n################################')
RawData.to_csv(sFileName, index=False) nShares = RawData.shape[0]
for sShare in range(nShares):
sShareName = str(RawData['Shares'][sShare]) try:
ShareData = quandl.get(sShareName) UnitsOwn = RawData['Units'][sShare] ShareData['UnitsOwn'] = UnitsOwn ShareData['ShareCode'] = sShareName
print(f'################\nShare : {sShareName}\nRows  :
{ShareData.shape[0]}\nColumns: {ShareData.shape[1]}\n################') sTable = str(RawData['sTable'][sShare]) print(f'################\nStoring : {sDatabaseName} Table:
{sTable}\n################')
ShareData.to_sql(sTable, conn, if_exists="replace")
for file_dir, prefix in zip([sFileDir1, sFileDir2, sFileDir3], ['Retrieve', 'Assess', 'Process']): sOutputFileName = sTable.replace("/", "-") + '.csv'
sFileName = os.path.join(file_dir, f'{prefix}_{sOutputFileName}') print(f'################################\nStoring :
 
{sFileName}\n################################')
ShareData.to_csv(sFileName, index=False)


except HTTPError as e:
print(f"HTTP error occurred while fetching data for {sShareName}: {e}") except Exception as e:
print(f"Other error occurred for {sShareName}: {e}")


print('### Done!! ############################################')






Output:

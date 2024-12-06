import sys import os
import pandas as pd import networkx as nx
from geopy.distance import vincenty import sqlite3 as sq
from pandas.io import sql if sys.platform == 'linux':
Base = os.path.expanduser('~') + '/VKHCG' else:
Base = 'C:/VKHCG' print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')
Company = '03-Hillman'
InputDir = '01-Retrieve/01-EDS/01-R' InputFileName = 'Retrieve_All_Countries.csv' EDSDir = '02-Assess/01-EDS'
OutputDir = EDSDir + '/02-Python' OutputFileName = 'Assess_Best_Logistics.gml' sFileDir = Base + '/' + Company + '/' + EDSDir if not os.path.exists(sFileDir):
os.makedirs(sFileDir)
 
sFileDir = Base + '/' + Company + '/' + OutputDir if not os.path.exists(sFileDir):
os.makedirs(sFileDir)
sDataBaseDir = Base + '/' + Company + '/02-Assess/SQLite' if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir)
sDatabaseName = sDataBaseDir + '/Hillman.db' conn = sq.connect(sDatabaseName)
sFileName = Base + '/' + Company + '/' + InputDir + '/' + InputFileName print('###########')
print('Loading :', sFileName)
Warehouse = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin-1") sColumns = {
'X1': 'Country',
'X2': 'PostCode',
'X3': 'PlaceName',
'X4': 'AreaName',
'X5': 'AreaCode',
'X10': 'Latitude', 'X11': 'Longitude'
}
Warehouse.rename(columns=sColumns, inplace=True) WarehouseGood = Warehouse #print(WarehouseGood.head())
RoutePointsCountry = pd.DataFrame(WarehouseGood.groupby(['Country'])[['Latitude', 'Longitude']].mean())
print('################')
sTable = 'Assess_RoutePointsCountry' print('Storing :', sDatabaseName, ' Table:', sTable)
RoutePointsCountry.to_sql(sTable, conn, if_exists="replace") print('################')
 
RoutePointsPostCode = pd.DataFrame(WarehouseGood.groupby(['Country', 'PostCode'])[['Latitude', 'Longitude']].mean())
print('################')
sTable = 'Assess_RoutePointsPostCode' print('Storing :', sDatabaseName, ' Table:', sTable)
RoutePointsPostCode.to_sql(sTable, conn, if_exists="replace") print('################')
RoutePointsPlaceName = pd.DataFrame(WarehouseGood.groupby(['Country', 'PostCode', 'PlaceName'])[['Latitude', 'Longitude']].mean())
#print(RoutePointsPlaceName.head()) print('################')
sTable = 'Assess_RoutePointsPlaceName' print('Storing :', sDatabaseName, ' Table:', sTable)
RoutePointsPlaceName.to_sql(sTable, conn, if_exists="replace") print('################')
print('################')
sView = 'Assess_RouteCountries'
print('Creating :', sDatabaseName, ' View:', sView) sSQL = "DROP VIEW IF EXISTS " + sView + ";"
sql.execute(sSQL, conn)
sSQL = "CREATE VIEW " + sView + " AS" sSQL = sSQL + " SELECT DISTINCT"
sSQL = sSQL + " S.Country AS SourceCountry," sSQL = sSQL + " S.Latitude AS SourceLatitude," sSQL = sSQL + " S.Longitude AS SourceLongitude," sSQL = sSQL + " T.Country AS TargetCountry," sSQL = sSQL + " T.Latitude AS TargetLatitude," sSQL = sSQL + " T.Longitude AS TargetLongitude" sSQL = sSQL + " FROM"
sSQL = sSQL + " Assess_RoutePointsCountry AS S"
 
sSQL = sSQL + ","
sSQL = sSQL + " Assess_RoutePointsCountry AS T" sSQL = sSQL + " WHERE S.Country <> T.Country" sSQL = sSQL + " AND"
sSQL = sSQL + " S.Country in ('GB','DE','BE','AU','US','IN')" sSQL = sSQL + " AND"
sSQL = sSQL + " T.Country in ('GB','DE','BE','AU','US','IN');"
sql.execute(sSQL, conn)


print('################')
print('Loading :', sDatabaseName, ' Table:', sView) sSQL = " SELECT "
sSQL = sSQL + " *" sSQL = sSQL + " FROM"
sSQL = sSQL + " " + sView + ";"
RouteCountries = pd.read_sql_query(sSQL, conn)
RouteCountries['Distance'] = RouteCountries.apply(lambda row: round(vincenty( (row['SourceLatitude'], row['SourceLongitude']), (row['TargetLatitude'],
row['TargetLongitude'])).miles, 4), axis=1) print(RouteCountries.head(5))

print('################')


sView = 'Assess_RoutePostCode'
print('Creating :', sDatabaseName, ' View:', sView) sSQL = "DROP VIEW IF EXISTS " + sView + ";"
sql.execute(sSQL, conn)
sSQL = "CREATE VIEW " + sView + " AS" sSQL = sSQL + " SELECT DISTINCT"
sSQL = sSQL + " S.Country AS SourceCountry," sSQL = sSQL + " S.Latitude AS SourceLatitude,"
 
sSQL = sSQL + " S.Longitude AS SourceLongitude," sSQL = sSQL + " T.Country AS TargetCountry," sSQL = sSQL + " T.PostCode AS TargetPostCode," sSQL = sSQL + " T.Latitude AS TargetLatitude," sSQL = sSQL + " T.Longitude AS TargetLongitude" sSQL = sSQL + " FROM"
sSQL = sSQL + " Assess_RoutePointsCountry AS S" sSQL = sSQL + ","
sSQL = sSQL + " Assess_RoutePointsPostCode AS T" sSQL = sSQL + " WHERE S.Country = T.Country" sSQL = sSQL + " AND"
sSQL = sSQL + " S.Country in ('GB','DE','BE','AU','US','IN')" sSQL = sSQL + " AND"
sSQL = sSQL + " T.Country in ('GB','DE','BE','AU','US','IN');"
sql.execute(sSQL, conn)


print('################')
print('Loading :', sDatabaseName, ' Table:', sView) sSQL = " SELECT "
sSQL = sSQL + " *" sSQL = sSQL + " FROM"
sSQL = sSQL + " " + sView + ";"
RoutePostCode = pd.read_sql_query(sSQL, conn)
RoutePostCode['Distance'] = RoutePostCode.apply(lambda row: round(vincenty( (row['SourceLatitude'], row['SourceLongitude']), (row['TargetLatitude'],
row['TargetLongitude'])).miles, 4), axis=1) print(RoutePostCode.head(5))

print('################')
sView = 'Assess_RoutePlaceName'
print('Creating :', sDatabaseName, ' View:', sView)
 
sSQL = "DROP VIEW IF EXISTS " + sView + ";"
sql.execute(sSQL, conn)
sSQL = "CREATE VIEW " + sView + " AS" sSQL = sSQL + " SELECT DISTINCT"
sSQL = sSQL + " S.Country AS SourceCountry," sSQL = sSQL + " S.PostCode AS SourcePostCode," sSQL = sSQL + " S.Latitude AS SourceLatitude," sSQL = sSQL + " S.Longitude AS SourceLongitude," sSQL = sSQL + " T.Country AS TargetCountry," sSQL = sSQL + " T.PostCode AS TargetPostCode,"
sSQL = sSQL + " T.PlaceName AS TargetPlaceName," sSQL = sSQL + " T.Latitude AS TargetLatitude," sSQL = sSQL + " T.Longitude AS TargetLongitude" sSQL = sSQL + " FROM"
sSQL = sSQL + " Assess_RoutePointsPostCode AS S" sSQL = sSQL + ","
sSQL = sSQL + " Assess_RoutePointsPlaceName AS T" sSQL = sSQL + " WHERE"
sSQL = sSQL + " S.Country = T.Country" sSQL = sSQL + " AND"
sSQL = sSQL + " S.PostCode = T.PostCode" sSQL = sSQL + " AND"
sSQL = sSQL + " S.Country in ('GB','DE','BE','AU','US','IN')" sSQL = sSQL + " AND"
sSQL = sSQL + " T.Country in ('GB','DE','BE','AU','US','IN');"
sql.execute(sSQL, conn)
print('Loading :', sDatabaseName, ' Table:', sView) sSQL = " SELECT "
sSQL = sSQL + " *" sSQL = sSQL + " FROM"
sSQL = sSQL + " " + sView + ";"
 
RoutePlaceName = pd.read_sql_query(sSQL, conn)
RoutePlaceName['Distance'] = RoutePlaceName.apply(lambda row: round(vincenty( (row['SourceLatitude'], row['SourceLongitude']), (row['TargetLatitude'],
row['TargetLongitude'])).miles, 4), axis=1) print(RoutePlaceName.head(5))

print('Saving File :', OutputFileName)
RoutePlaceName.to_gml(Base + '/' + Company + '/' + OutputDir + '/' + OutputFileName, nodes=True)





Output:
You can now query features out of a graph, such as shortage paths between locations and paths from a given location, using Assess_Best_Logistics.gml with appropirate application.

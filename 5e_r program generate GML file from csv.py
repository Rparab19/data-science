Understanding Your Online Visitor Data
Code:
import networkx as nx import sys
import os
import sqlite3 as sq import pandas as pd
from geopy.distance import geodesic import time

Base = 'D:/Dinesh/DS/5/' print('################################')
print('Working Base :', Base) print('################################')

sTable = 'Assess_BillboardVisitorData' sOutputFileName = 'Assess-DE-Billboard-Visitor.gml'

sDataBaseDir = Base
if not os.path.exists(sDataBaseDir): os.makedirs(sDataBaseDir)

sDatabaseName = sDataBaseDir + '/krennwallner.db' conn = sq.connect(sDatabaseName)
start_time = time.time() # Start timing print('################')

print('Loading :', sDatabaseName, ' Table:', sTable)
 
sSQL = """SELECT
A.BillboardCountry, A.BillboardPlaceName,
ROUND(A.BillboardLatitude, 3) AS BillboardLatitude, ROUND(A.BillboardLongitude, 3) AS BillboardLongitude,
(CASE WHEN A.BillboardLatitude < 0 THEN 'S' || ROUND(ABS(A.BillboardLatitude), 3) ELSE 'N' || ROUND(ABS(A.BillboardLatitude), 3) END) AS sBillboardLatitude,
(CASE WHEN A.BillboardLongitude < 0 THEN 'W' || ROUND(ABS(A.BillboardLongitude),
3)
ELSE 'E' || ROUND(ABS(A.BillboardLongitude), 3) END) AS sBillboardLongitude, A.VisitorCountry,
A.VisitorPlaceName,
ROUND(A.VisitorLatitude, 3) AS VisitorLatitude, ROUND(A.VisitorLongitude, 3) AS VisitorLongitude,
(CASE WHEN A.VisitorLatitude < 0 THEN 'S' || ROUND(ABS(A.VisitorLatitude), 3) ELSE 'N' || ROUND(ABS(A.VisitorLatitude), 3) END) AS sVisitorLatitude,
(CASE WHEN A.VisitorLongitude < 0 THEN 'W' || ROUND(ABS(A.VisitorLongitude), 3) ELSE 'E' || ROUND(ABS(A.VisitorLongitude), 3) END) AS sVisitorLongitude,
A.VisitorYearRate
FROM Assess_BillboardVistorsData AS A;""" BillboardVistorsData = pd.read_sql_query(sSQL, conn) print('################')
print("--- Data loading took %s seconds ---" % (time.time() - start_time)) start_time = time.time() # Start timing for distance calculation BillboardVistorsData['Distance'] = BillboardVistorsData.apply(lambda row:
geodesic((row['BillboardLatitude'], row['BillboardLongitude']), (row['VisitorLatitude'], row['VisitorLongitude'])).meters,
axis=1)
 
print("--- Distance calculation took %s seconds ---" % (time.time() - start_time)) G = nx.Graph()

start_time = time.time() # Start timing for graph construction for i in range(BillboardVistorsData.shape[0]):
sNode0 = 'MediaHub-' + BillboardVistorsData['BillboardCountry'][i]


sNode1 = 'B-' + BillboardVistorsData['sBillboardLatitude'][i] + '-' + BillboardVistorsData['sBillboardLongitude'][i]
G.add_node(sNode1,
Nodetype='Billboard', Country=BillboardVistorsData['BillboardCountry'][i], PlaceName=BillboardVistorsData['BillboardPlaceName'][i], Latitude=round(BillboardVistorsData['BillboardLatitude'][i], 3),
Longitude=round(BillboardVistorsData['BillboardLongitude'][i], 3))


sNode2 = 'M-' + BillboardVistorsData['sVisitorLatitude'][i] + '-' + BillboardVistorsData['sVisitorLongitude'][i]
G.add_node(sNode2,
Nodetype='Mobile', Country=BillboardVistorsData['VisitorCountry'][i], PlaceName=BillboardVistorsData['VisitorPlaceName'][i], Latitude=round(BillboardVistorsData['VisitorLatitude'][i], 3),
Longitude=round(BillboardVistorsData['VisitorLongitude'][i], 3))


print('Link Media Hub :', sNode0, ' to Billboard : ', sNode1) G.add_edge(sNode0, sNode1)

print('Link Post Code :', sNode1, ' to GPS : ', sNode2)
G.add_edge(sNode1, sNode2, distance=round(BillboardVistorsData['Distance'][i]))
 
print("--- Graph construction took %s seconds ---" % (time.time() - start_time)) print('################################')
print("Nodes of graph: ", nx.number_of_nodes(G)) print("Edges of graph: ", nx.number_of_edges(G)) print('################################')

sFileDir = Base + '/5e'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)

sFileName = sFileDir + '/' + sOutputFileName print('################################')
print('Storing :', sFileName) print('################################')

start_time = time.time() # Start timing for file writing nx.write_gml(G, sFileName)
sFileName = sFileName + '.gz' nx.write_gml(G, sFileName)
print("--- File writing took %s seconds ---" % (time.time() - start_time)) print('### Done!! ############################################')
 
Output:



 
Planning an Event for Top-Ten Customers
Code:
import sys import os
import sqlite3 as sq import pandas as pd

Base = 'D:/Dinesh/DS/5' print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')

sInputFileName = 'Retrieve_Online_Visitor.csv' sDataBaseDir = Base

if not os.path.exists(sDataBaseDir): os.makedirs(sDataBaseDir)

sDatabaseName = sDataBaseDir + '/krennwallner.db' conn = sq.connect(sDatabaseName)
sFileName = Base + '/' + sInputFileName print('################################')
print('Loading :', sFileName) print('################################')

VisitorRawData = pd.read_csv(sFileName,
header=0, low_memory=False, encoding="latin-1", skip_blank_lines=True)
VisitorRawData.drop_duplicates(inplace=True)
 
VisitorData = VisitorRawData
print('Loaded Company :', VisitorData.columns.values) print('################################')

print('################')
sTable = 'Assess_Visitor'
print('Storing :', sDatabaseName, ' Table:', sTable) VisitorData.to_sql(sTable, conn, if_exists="replace", index=False) print('################')

print(VisitorData.head()) print('################################')
print('Rows : ', VisitorData.shape[0]) print('################################')
def create_view(conn, view_name, sql): conn.execute(f"DROP VIEW IF EXISTS {view_name};") conn.execute(sql)

# Assess_Visitor_UseIt
sView = 'Assess_Visitor_UseIt' sSQL = """
CREATE VIEW Assess_Visitor_UseIt AS
SELECT A.Country, A.Place_Name, A.Latitude, A.Longitude, (A.Last_IP_Number - A.First_IP_Number) AS UsesIt
FROM Assess_Visitor AS A
WHERE Country IS NOT NULL AND Place_Name IS NOT NULL; """
create_view(conn, sView, sSQL)
sView = 'Assess_Total_Visitors_Location' sSQL = """
CREATE VIEW Assess_Total_Visitors_Location AS
 
SELECT Country, Place_Name, SUM(UsesIt) AS TotalUsesIt FROM Assess_Visitor_UseIt
GROUP BY Country, Place_Name ORDER BY TotalUsesIt DESC LIMIT 10;
"""
create_view(conn, sView, sSQL) sView = 'Assess_Total_Visitors_GPS' sSQL = """
CREATE VIEW Assess_Total_Visitors_GPS AS
SELECT Latitude, Longitude, SUM(UsesIt) AS TotalUsesIt FROM Assess_Visitor_UseIt
GROUP BY Latitude, Longitude ORDER BY TotalUsesIt DESC LIMIT 10;
"""
create_view(conn, sView, sSQL)
sTables = ['Assess_Total_Visitors_Location', 'Assess_Total_Visitors_GPS'] for sTable in sTables:
print('################')
print('Loading :', sDatabaseName, ' Table:', sTable) sSQL = f"SELECT * FROM {sTable};"
TopData = pd.read_sql_query(sSQL, conn) print('################')
print(TopData) print('################')
print('Rows : ', TopData.shape[0]) print('################################')

conn.close()
print('### Done!! ############################################')
 
Output:

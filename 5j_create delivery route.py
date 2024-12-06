import sys import os
import pandas as pd import sqlite3 as sq
from pandas.io import sql import networkx as nx
from geopy.distance import geodesic from geopy.distance import vincenty nMax=3
nMaxPath=10 nSet=False nVSet=False
Base = 'D:/Dinesh/DS/5/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
InputDir1='csv' InputDir2='csv'
InputFileName1='Retrieve_GB_Postcode_Warehouse.csv' InputFileName2='Retrieve_GB_Postcodes_Shops.csv' EDSDir='02-Assess/01-EDS'
OutputDir=EDSDir + '/02-Python' OutputFileName1='Assess_Shipping_Routes.gml' OutputFileName2='Assess_Shipping_Routes.txt' sFileDir=Base + '/' + '/' + EDSDir
if not os.path.exists(sFileDir):
 
os.makedirs(sFileDir) sFileDir=Base + '/' + '/' + OutputDir if not os.path.exists(sFileDir):
os.makedirs(sFileDir)
sDataBaseDir=Base + '/' + '/02-Assess/SQLite' if not os.path.exists(sDataBaseDir):
os.makedirs(sDataBaseDir) sDatabaseName=sDataBaseDir + '/hillman.db' conn = sq.connect(sDatabaseName)
### Import Warehouse Data
sFileName=Base + '/' + '/' + InputDir1 + '/' + InputFileName1 print('###########')
print('Loading :',sFileName) WarehouseRawData=pd.read_csv(sFileName,
header=0, low_memory=False, encoding="latin-1"
)
# Replace append with pd.concat WarehouseRawData.drop_duplicates(subset=None, keep='first', inplace=True) WarehouseRawData.index.name = 'IDNumber'
WarehouseData = WarehouseRawData.head(nMax)


# Using pd.concat instead of append
WarehouseData = pd.concat([WarehouseData, WarehouseRawData.tail(nMax)], ignore_index=True)
WarehouseData = pd.concat([WarehouseData, WarehouseRawData[WarehouseRawData.postcode == 'KA13']], ignore_index=True)

if nSet == True:
WarehouseData = pd.concat([WarehouseData,
 
WarehouseRawData[WarehouseRawData.postcode == 'SW1W']], ignore_index=True)


WarehouseData.drop_duplicates(subset=None, keep='first', inplace=True) print('Loaded Warehouses:', WarehouseData.columns.values) print('################################') print('################')
sTable='Assess_Warehouse_UK'
print('Storing :',sDatabaseName,' Table:',sTable) WarehouseData.to_sql(sTable, conn, if_exists="replace") print('################')
print(WarehouseData.head()) print('################################')
print('Rows : ',WarehouseData.shape[0]) print('################################')
### Import Shop Data
sFileName=Base + '/' + '/' + InputDir1 + '/' + InputFileName2 print('###########')
print('Loading :',sFileName) ShopRawData=pd.read_csv(sFileName,
header=0, low_memory=False, encoding="latin-1"
)
ShopRawData.drop_duplicates(subset=None, keep='first', inplace=True) ShopRawData.index.name = 'IDNumber'
ShopData=ShopRawData
print('Loaded Shops :',ShopData.columns.values) print('################################') print('################')
sTable='Assess_Shop_UK'
print('Storing :',sDatabaseName,' Table:',sTable)
 
ShopData.to_sql(sTable, conn, if_exists="replace") print('################')
print(ShopData.head()) print('################################')
print('Rows : ',ShopData.shape[0]) print('################################')
### Connect HQ print('################')
sView='Assess_HQ'
print('Creating :',sDatabaseName,' View:',sView) sSQL="DROP VIEW IF EXISTS " + sView + ";"
conn.execute(sSQL)


sSQL="CREATE VIEW " + sView + " AS" sSQL=sSQL+ " SELECT"
sSQL=sSQL+ " W.postcode AS HQ_PostCode," sSQL=sSQL+ " 'HQ-' || W.postcode AS HQ_Name," sSQL=sSQL+ " round(W.latitude,6) AS HQ_Latitude," sSQL=sSQL+ " round(W.longitude,6) AS HQ_Longitude" sSQL=sSQL+ " FROM"
sSQL=sSQL+ " Assess_Warehouse_UK as W" sSQL=sSQL+ " WHERE"
sSQL=sSQL+ " TRIM(W.postcode) in ('KA13','SW1W');"
conn.execute(sSQL)
### Connect Warehouses print('################')
sView='Assess_Warehouse'
print('Creating :',sDatabaseName,' View:',sView) sSQL="DROP VIEW IF EXISTS " + sView + ";"
conn.execute(sSQL)
 
sSQL="CREATE VIEW " + sView + " AS" sSQL=sSQL+ " SELECT"
sSQL=sSQL+ " W.postcode AS Warehouse_PostCode," sSQL=sSQL+ " 'WH-' || W.postcode AS Warehouse_Name," sSQL=sSQL+ " round(W.latitude,6) AS Warehouse_Latitude," sSQL=sSQL+ " round(W.longitude,6) AS Warehouse_Longitude" sSQL=sSQL+ " FROM"
sSQL=sSQL+ " Assess_Warehouse_UK as W;" conn.execute(sSQL)
### Connect Warehouse to Shops by PostCode print('################')
sView='Assess_Shop'
print('Creating :',sDatabaseName,' View:',sView) sSQL="DROP VIEW IF EXISTS " + sView + ";"
conn.execute(sSQL)


sSQL="CREATE VIEW " + sView + " AS" sSQL=sSQL+ " SELECT"
sSQL=sSQL+ " TRIM(S.postcode) AS Shop_PostCode,"
sSQL=sSQL+ " 'SP-' || TRIM(S.FirstCode) || '-' || TRIM(S.SecondCode) AS Shop_Name," sSQL=sSQL+ " TRIM(S.FirstCode) AS Warehouse_PostCode,"
sSQL=sSQL+ " round(S.latitude,6) AS Shop_Latitude," sSQL=sSQL+ " round(S.longitude,6) AS Shop_Longitude" sSQL=sSQL+ " FROM"
sSQL=sSQL+ " Assess_Warehouse_UK as W" sSQL=sSQL+ " JOIN"
sSQL=sSQL+ " Assess_Shop_UK as S" sSQL=sSQL+ " ON"
sSQL=sSQL+ " TRIM(W.postcode) = TRIM(S.FirstCode);" conn.execute(sSQL)
G=nx.Graph()
 
print('################')
sTable = 'Assess_HQ'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL=" SELECT DISTINCT"
sSQL=sSQL+ " *" sSQL=sSQL+ " FROM"
sSQL=sSQL+ " " + sTable + ";" RouteData=pd.read_sql_query(sSQL, conn) print('################')
print(RouteData.head()) print('################################')
print('HQ Rows : ',RouteData.shape[0]) print('################################')
for i in range(RouteData.shape[0]): sNode0=RouteData['HQ_Name'][i] G.add_node(sNode0,
Nodetype='HQ', PostCode=RouteData['HQ_PostCode'][i], Latitude=round(RouteData['HQ_Latitude'][i],6), Longitude=round(RouteData['HQ_Longitude'][i],6))
print('################')
sTable = 'Assess_Warehouse'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL=" SELECT DISTINCT"
sSQL=sSQL+ " *" sSQL=sSQL+ " FROM"
sSQL=sSQL+ " " + sTable + ";" RouteData=pd.read_sql_query(sSQL, conn) print('################')
print(RouteData.head()) print('################################')
 
print('Warehouse Rows : ',RouteData.shape[0]) print('################################')
for i in range(RouteData.shape[0]): sNode0=RouteData['Warehouse_Name'][i] G.add_node(sNode0,
Nodetype='Warehouse', PostCode=RouteData['Warehouse_PostCode'][i], Latitude=round(RouteData['Warehouse_Latitude'][i],6), Longitude=round(RouteData['Warehouse_Longitude'][i],6))
print('################')
sTable = 'Assess_Shop'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL=" SELECT DISTINCT"
sSQL=sSQL+ " *" sSQL=sSQL+ " FROM"
sSQL=sSQL+ " " + sTable + ";" RouteData=pd.read_sql_query(sSQL, conn) print('################')
print(RouteData.head()) print('################################')
print('Shop Rows : ',RouteData.shape[0]) print('################################')
for i in range(RouteData.shape[0]): sNode0=RouteData['Shop_Name'][i] G.add_node(sNode0,
Nodetype='Shop', PostCode=RouteData['Shop_PostCode'][i], WarehousePostCode=RouteData['Warehouse_PostCode'][i], Latitude=round(RouteData['Shop_Latitude'][i],6), Longitude=round(RouteData['Shop_Longitude'][i],6))
## Create Edges
 
# Loading Edges
from geopy.distance import geodesic


# Loading Edges print('################################')
print('Loading Edges') print('################################')

# Iterate through the nodes using G.nodes for sNode0 in G.nodes:
for sNode1 in G.nodes:
if G.nodes[sNode0]['Nodetype'] == 'HQ' and \ G.nodes[sNode1]['Nodetype'] == 'HQ' and \ sNode0 != sNode1:
distancemeters = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).meters, 0)


distancemiles = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).miles, 3)

if distancemiles >= 0.05:
cost = round(150 + (distancemiles * 2.5), 6) vehicle = 'V001'
else:
cost = round(2 + (distancemiles * 0.10), 6) vehicle = 'ForkLift'
 
G.add_edge(sNode0, sNode1, DistanceMeters=distancemeters, DistanceMiles=distancemiles, Cost=cost, Vehicle=vehicle)
if nVSet == True:
print('Edge-H-H:', sNode0, ' to ', sNode1, ' Distance:', distancemeters, 'meters', distancemiles, 'miles', 'Cost', cost, 'Vehicle', vehicle)



if G.nodes[sNode0]['Nodetype'] == 'HQ' and \ G.nodes[sNode1]['Nodetype'] == 'Warehouse' and \ sNode0 != sNode1:
distancemeters = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).meters, 0)


distancemiles = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).miles, 3)


if distancemiles >= 10:
cost = round(50 + (distancemiles * 2), 6) vehicle = 'V002'
else:
cost = round(5 + (distancemiles * 1.5), 6) vehicle = 'V003'

if distancemiles <= 50:
G.add_edge(sNode0, sNode1, DistanceMeters=distancemeters, DistanceMiles=distancemiles, Cost=cost, Vehicle=vehicle)
if nVSet == True:
 
print('Edge-H-W:', sNode0, ' to ', sNode1, ' Distance:', distancemeters, 'meters', distancemiles, 'miles', 'Cost', cost, 'Vehicle', vehicle)

if nSet == True and \
G.nodes[sNode0]['Nodetype'] == 'Warehouse' and \ G.nodes[sNode1]['Nodetype'] == 'Warehouse' and \ sNode0 != sNode1:
distancemeters = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).meters, 0)


distancemiles = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).miles, 3)


if distancemiles >= 10:
cost = round(50 + (distancemiles * 1.10), 6) vehicle = 'V004'
else:
cost = round(5 + (distancemiles * 1.05), 6) vehicle = 'V005'

if distancemiles <= 20:
G.add_edge(sNode0, sNode1, DistanceMeters=distancemeters, DistanceMiles=distancemiles, Cost=cost, Vehicle=vehicle)
if nVSet == True:
print('Edge-W-W:', sNode0, ' to ', sNode1, ' Distance:', distancemeters, 'meters', distancemiles, 'miles', 'Cost', cost, 'Vehicle', vehicle)
 
if G.nodes[sNode0]['Nodetype'] == 'Warehouse' and \ G.nodes[sNode1]['Nodetype'] == 'Shop' and \
G.nodes[sNode0]['PostCode'] == G.nodes[sNode1]['WarehousePostCode'] and \ sNode0 != sNode1:
distancemeters = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).meters, 0)


distancemiles = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).miles, 3)


if distancemiles >= 10:
cost = round(50 + (distancemiles * 1.50), 6) vehicle = 'V006'
else:
cost = round(5 + (distancemiles * 0.75), 6) vehicle = 'V007'

if distancemiles <= 10:
G.add_edge(sNode0, sNode1, DistanceMeters=distancemeters, DistanceMiles=distancemiles, Cost=cost, Vehicle=vehicle)
if nVSet == True:
print('Edge-W-S:', sNode0, ' to ', sNode1, ' Distance:', distancemeters, 'meters', distancemiles, 'miles', 'Cost', cost, 'Vehicle', vehicle)

if nSet == True and \ G.nodes[sNode0]['Nodetype'] == 'Shop' and \ G.nodes[sNode1]['Nodetype'] == 'Shop' and \
 
G.nodes[sNode0]['WarehousePostCode'] == G.nodes[sNode1]['WarehousePostCode'] and
\
sNode0 != sNode1:
distancemeters = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).meters, 0)


distancemiles = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).miles, 3)


if distancemiles >= 0.05:
cost = round(5 + (distancemiles * 0.5), 6) vehicle = 'V008'
else:
cost = round(1 + (distancemiles * 0.1), 6) vehicle = 'V009'

if distancemiles <= 0.075:
G.add_edge(sNode0, sNode1, DistanceMeters=distancemeters, DistanceMiles=distancemiles, Cost=cost, Vehicle=vehicle)
if nVSet == True:
print('Edge-S-S:', sNode0, ' to ', sNode1, ' Distance:', distancemeters, 'meters', distancemiles, 'miles', 'Cost', cost, 'Vehicle', vehicle)

if nSet == True and \ G.nodes[sNode0]['Nodetype'] == 'Shop' and \ G.nodes[sNode1]['Nodetype'] == 'Shop' and \
G.nodes[sNode0]['WarehousePostCode'] != G.nodes[sNode1]['WarehousePostCode'] and
 
\
sNode0 != sNode1:
distancemeters = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).meters, 0)


distancemiles = round(geodesic(
(G.nodes[sNode0]['Latitude'], G.nodes[sNode0]['Longitude']), (G.nodes[sNode1]['Latitude'], G.nodes[sNode1]['Longitude'])
).miles, 3)


cost = round(1 + (distancemiles * 0.1), 6) vehicle = 'V010'

if distancemiles <= 0.025:
G.add_edge(sNode0, sNode1, DistanceMeters=distancemeters, DistanceMiles=distancemiles, Cost=cost, Vehicle=vehicle)
if nVSet == True:
print('Edge-S-S:', sNode0, ' to ', sNode1, ' Distance:', distancemeters, 'meters', distancemiles, 'miles', 'Cost', cost, 'Vehicle', vehicle)

################################################################
sFileName=sFileDir + '/' + OutputFileName1 print('################################')
print('Storing :', sFileName) print('################################')
nx.write_gml(G,sFileName) sFileName=sFileName +'.gz' nx.write_gml(G,sFileName)
################################################################
 
print('Nodes:',nx.number_of_nodes(G)) print('Edges:',nx.number_of_edges(G))
################################################################
sFileName=sFileDir + '/' + OutputFileName2 print('################################')
print('Storing :', sFileName) print('################################')
################################################################
## Create Paths ################################################################ print('################################')
print('Loading Paths') print('################################')
f = open(sFileName,'w') l=0
sline = 'ID|Cost|StartAt|EndAt|Path|Measure' if nVSet==True: print ('0', sline) f.write(sline+ '\n')
for sNode0 in G.nodes: # Use G.nodes instead of nx.nodes_iter(G) for sNode1 in G.nodes: # Use G.nodes instead of nx.nodes_iter(G)
if sNode0 != sNode1 and \
nx.has_path(G, sNode0, sNode1) == True and \ nx.shortest_path_length(G, \
source=sNode0, \ target=sNode1, \
weight='DistanceMiles') < nMaxPath: l += 1
sID = '{:.0f}'.format(l)
spath = ','.join(nx.shortest_path(G, \ source=sNode0, \
target=sNode1, \
 
weight='DistanceMiles')) slength = '{:.6f}'.format(\ nx.shortest_path_length(G, \ source=sNode0, \ target=sNode1, \ weight='DistanceMiles'))
sline = sID + '|"DistanceMiles"|"' + sNode0 + '"|"' \
+ sNode1 + '"|"' + spath + '"|' + slength if nVSet == True:
print(sline) f.write(sline + '\n') l += 1
sID = '{:.0f}'.format(l)
spath = ','.join(nx.shortest_path(G, \ source=sNode0, \
target=sNode1, \ weight='DistanceMeters')) slength = '{:.6f}'.format(\ nx.shortest_path_length(G, \ source=sNode0, \ target=sNode1, \ weight='DistanceMeters'))
sline = sID + '|"DistanceMeters"|"' + sNode0 + '"|"' \
+ sNode1 + '"|"' + spath + '"|' + slength if nVSet == True:
print(sline) f.write(sline + '\n') l += 1
sID = '{:.0f}'.format(l)
spath = ','.join(nx.shortest_path(G, \ source=sNode0, \
 


















f.close()
 
target=sNode1, \ weight='Cost'))
slength = '{:.6f}'.format(\ nx.shortest_path_length(G, \ source=sNode0, \ target=sNode1, \ weight='Cost'))
sline = sID + '|"Cost"|"' + sNode0 + '"|"' \
+ sNode1 + '"|"' + spath + '"|' + slength if nVSet == True:
print(sline) f.write(sline + '\n')
 


################################################################
print('Nodes:',nx.number_of_nodes(G)) print('Edges:',nx.number_of_edges(G)) print('Paths:',sID)
################################################################ ################################################################ print('################')
print('Vacuum Database') sSQL="VACUUM;"
conn.execute(sSQL) print('################')
################################################################ print('### Done!! ############################################') ################################################################

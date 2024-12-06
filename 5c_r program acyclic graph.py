Directed Acyclic Graph
Code:
import networkx as nx
import matplotlib.pyplot as plt import sys
import os
import pandas as pd


Base='D:/Dinesh/DS/5/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sInputFileName='Retrieve_Router_Location.csv' sOutputFileName1='Assess-DAG-Company-Country.png' sOutputFileName2='Assess-DAG-Company-Country-Place.png' sFileName=Base + '/' + sInputFileName print('################################')
print('Loading :',sFileName) print('################################')
CompanyData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") print('Loaded Company :',CompanyData.columns.values) print('################################')
print(CompanyData) print('################################')
print('Rows : ',CompanyData.shape[0]) print('################################')
G1=nx.DiGraph() G2=nx.DiGraph()
 
for i in range(CompanyData.shape[0]): G1.add_node(CompanyData['Country'][i])
sPlaceName= CompanyData['Place_Name'][i] + '-' + CompanyData['Country'][i] G2.add_node(sPlaceName)
print('################################')


for n1 in G1.nodes(): for n2 in G1.nodes():
if n1 != n2:
print('Link :',n1,' to ', n2) G1.add_edge(n1,n2)

print('################################') print('################################')
print("Nodes of graph: ") print(G1.nodes()) print("Edges of graph: ") print(G1.edges())
print('################################')


sFileDir=Base + '/5ca'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)
sFileName=sFileDir + '/' + sOutputFileName1 print('################################')
print('Storing :', sFileName) print('################################')
nx.draw(G1,pos=nx.spectral_layout(G1), node_color='r',edge_color='g', with_labels=True,node_size=8000, font_size=12)
 
plt.savefig(sFileName) # save as png plt.show() print('################################')
for n1 in G2.nodes(): for n2 in G2.nodes():
if n1 != n2:
print('Link :',n1,' to ', n2) G2.add_edge(n1,n2)

print('################################') print('################################')
print("Nodes of graph: ") print(G2.nodes()) print("Edges of graph: ") print(G2.edges())
print('################################')
sFileDir=Base + '/5ca'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)
sFileName=sFileDir + '/' + sOutputFileName2 print('################################')
print('Storing :', sFileName) print('################################')
nx.draw(G2,pos=nx.spectral_layout(G2), node_color='r',edge_color='b', with_labels=True,node_size=8000, font_size=12)
plt.savefig(sFileName) # save as png plt.show()
--------------------------------------------------------
Undirected Acyclic Graph
Code:
import networkx as nx
import matplotlib.pyplot as plt import sys
import os
import pandas as pd Base = 'D:/Dinesh/DS/5/'
print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sInputFileName='Retrieve_Router_Location.csv' sOutputFileName='Assess-DAG-Company-GPS.png' sFileName=Base + sInputFileName print('################################')
print('Loading :',sFileName) print('################################')
CompanyData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") print('Loaded Company :',CompanyData.columns.values) print('################################')
print(CompanyData) print('################################')
print('Rows : ',CompanyData.shape[0]) print('################################')
G=nx.Graph()
for i in range(CompanyData.shape[0]): nLatitude=round(CompanyData['Latitude'][i],2) nLongitude=round(CompanyData['Longitude'][i],2)

if nLatitude < 0:
sLatitude = str(nLatitude*-1) + ' S'
 
else:
sLatitude = str(nLatitude) + ' N'


if nLongitude < 0:
sLongitude = str(nLongitude*-1) + ' W' else:
sLongitude = str(nLongitude) + ' E'


sGPS= sLatitude + '-' + sLongitude G.add_node(sGPS)

print('################################')
for n1 in G.nodes(): for n2 in G.nodes():
if n1 != n2:
print('Link :',n1,' to ', n2) G.add_edge(n1,n2)
print('################################')


print('################################')
print("Nodes of graph: ") print(G.number_of_nodes()) print("Edges of graph: ") print(G.number_of_edges())
print('################################')

sFileDir = Base + '5cb'
if not os.path.exists(sFileDir): os.makedirs(sFileDir)

sFileName = sFileDir + '/' + sOutputFileName
 
print('################################')
print('Storing :', sFileName) print('################################')

plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G) # position the nodes
nx.draw(G, pos, with_labels=True, node_size=5000, node_color='r', edge_color='g', font_size=10)
plt.title("Graph of GPS Locations") plt.savefig(sFileName) # Save the graph plt.show() # Display the graph

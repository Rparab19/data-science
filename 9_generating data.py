import sys import os
import pandas as pd import networkx as nx
import matplotlib.pyplot as plt
pd.options.mode.chained_assignment = None Base = 'D:/Dinesh/DS/9/'
print('Working Base :',Base, ' using ', sys.platform) sInputFileName='csv/Assess-Network-Routing-Customer.csv' sOutputFileName1='csv/Report-Network-Routing-Customer.gml' sOutputFileName2='csv/Report-Network-Routing-Customer.png' sFileName=Base + '/' + sInputFileName
print('Loading :',sFileName) CustomerDataRaw=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") CustomerData=CustomerDataRaw.head(100)
print('Loaded Country:',CustomerData.columns.values) print(CustomerData.head())  print(CustomerData.shape)
G=nx.Graph()
for i in range(CustomerData.shape[0]): for j in range(CustomerData.shape[0]):
Node0=CustomerData['Customer_Country_Name'][i] Node1=CustomerData['Customer_Country_Name'][j] if Node0 != Node1:
G.add_edge(Node0,Node1)
for i in range(CustomerData.shape[0]): Node0=CustomerData['Customer_Country_Name'][i]
 
Node1=CustomerData['Customer_Place_Name'][i] + '('+ CustomerData['Customer_Country_Name'][i] + ')'
Node2='('+ "{:.9f}".format(CustomerData['Customer_Latitude'][i]) + ')\ ('+ "{:.9f}".format(CustomerData['Customer_Longitude'][i]) + ')'
if Node0 != Node1: G.add_edge(Node0,Node1)
if Node1 != Node2: G.add_edge(Node1,Node2)

print('Nodes:', G.number_of_nodes()) print('Edges:', G.number_of_edges()) sFileName=Base + '/' + sOutputFileName1

print('Storing :',sFileName)


nx.write_gml(G, sFileName) sFileName=Base + '/' + sOutputFileName2

print('Storing Graph Image:',sFileName)



plt.figure(figsize=(25, 25)) pos=nx.spectral_layout(G,dim=2)
nx.draw_networkx_nodes(G,pos, node_color='k', node_size=10, alpha=0.8) nx.draw_networkx_edges(G, pos,edge_color='r', arrows=False, style='dashed') nx.draw_networkx_labels(G,pos,font_size=12,font_family='sans-serif',font_color='b') plt.axis('off')
plt.savefig(sFileName,dpi=600) plt.show()

print('### Done!! #####################')

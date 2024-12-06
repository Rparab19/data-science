Horizontal Style
Code:
import sys import os
import pandas as pd import sqlite3 as sq
Base = 'D:/Dinesh/DS/8/'
 
Practical: 8 Organizing data
 

print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sDataWarehouseDir=Base + 'csv'
if not os.path.exists(sDataWarehouseDir): os.makedirs(sDataWarehouseDir)
sDatabaseName=sDataWarehouseDir + '/datawarehouse.db' conn1 = sq.connect(sDatabaseName) sDatabaseName=sDataWarehouseDir + '/datamart.db' conn2 = sq.connect(sDatabaseName) print('################')

sTable = 'Dim-BMI'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL="SELECT * FROM [Dim-BMI];"
PersonFrame0=pd.read_sql_query(sSQL, conn1) print('################################')
sTable = 'Dim-BMI'


print('Loading :',sDatabaseName,' Table:',sTable)
 
print('################################')
sSQL="SELECT PersonID,\ Height,\
Weight,\ bmi,\ Indicator\
FROM [Dim-BMI]\ WHERE \
Height > 1.5 \ and Indicator = 1\ ORDER BY \
Height,\ Weight;"

PersonFrame1=pd.read_sql_query(sSQL, conn1) DimPerson=PersonFrame1 DimPersonIndex=DimPerson.set_index(['PersonID'],inplace=False) sTable = 'Dim-BMI-Horizontal'

print('\n#################################')
print('Storing :',sDatabaseName,'\n Table:',sTable) print('\n#################################')
DimPersonIndex.to_sql(sTable, conn2, if_exists="replace") print('################################')
sTable = 'Dim-BMI-Horizontal'


print('Loading :',sDatabaseName,' Table:',sTable) print('################################')

sSQL="SELECT * FROM [Dim-BMI];"
PersonFrame2=pd.read_sql_query(sSQL, conn2)
 
print('################################')
print('Full Data Set (Rows):', PersonFrame0.shape[0]) print('Full Data Set (Columns):', PersonFrame0.shape[1])

print('################################')
print('Horizontal Data Set (Rows):', PersonFrame2.shape[0]) print('Horizontal Data Set (Columns):', PersonFrame2.shape[1])

print('################################')




Output:



 
Vertical Style
Code:
import sys import os
import pandas as pd import sqlite3 as sq
Base = 'D:/Dinesh/DS/8/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sDataWarehouseDir=Base + 'csv'
if not os.path.exists(sDataWarehouseDir): os.makedirs(sDataWarehouseDir)
sDatabaseName=sDataWarehouseDir + '/datawarehouse.db' conn1 = sq.connect(sDatabaseName) sDatabaseName=sDataWarehouseDir + '/datamart.db' conn2 = sq.connect(sDatabaseName) print('################################')
sTable = 'Dim-BMI'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL="SELECT * FROM [Dim-BMI];"
PersonFrame0=pd.read_sql_query(sSQL, conn1) sTable = 'Dim-BMI'
print('Loading :',sDatabaseName,' Table:',sTable) print('################################')
sSQL="SELECT Height, Weight,\ Indicator\
FROM [Dim-BMI];"
PersonFrame1=pd.read_sql_query(sSQL, conn1) DimPerson=PersonFrame1 DimPersonIndex=DimPerson.set_index(['Indicator'],inplace=False)
 
sTable = 'Dim-BMI-Vertical' print('\n#################################')
print('Storing :',sDatabaseName,'\n Table:',sTable) DimPersonIndex.to_sql(sTable, conn2, if_exists="replace") sTable = 'Dim-BMI-Vertical'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL="SELECT * FROM [Dim-BMI-Vertical];"
PersonFrame2=pd.read_sql_query(sSQL, conn2) print('Full Data Set (Rows):', PersonFrame0.shape[0]) print('Full Data Set (Columns):', PersonFrame0.shape[1]) print('################################')
print('Horizontal Data Set (Rows):', PersonFrame2.shape[0]) print('Horizontal Data Set (Columns):', PersonFrame2.shape[1]) print('################################')


Output:



 
Island Style
Code:
import sys import os
import pandas as pd import sqlite3 as sq
Base = 'D:/Dinesh/DS/8/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sDataWarehouseDir=Base + 'csv'
if not os.path.exists(sDataWarehouseDir): os.makedirs(sDataWarehouseDir)
sDatabaseName=sDataWarehouseDir + '/datawarehouse.db' conn1 = sq.connect(sDatabaseName) sDatabaseName=sDataWarehouseDir + '/datamart.db' conn2 = sq.connect(sDatabaseName) print('################')
sTable = 'Dim-BMI'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL="SELECT * FROM [Dim-BMI];"
PersonFrame0=pd.read_sql_query(sSQL, conn1) sTable = 'Dim-BMI'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL="SELECT Height, Weight, Indicator\ FROM [Dim-BMI]\
WHERE Indicator > 2\ ORDER BY \
Height,\ Weight;"
PersonFrame1=pd.read_sql_query(sSQL, conn1)
 
DimPerson=PersonFrame1 DimPersonIndex=DimPerson.set_index(['Indicator'],inplace=False) sTable = 'Dim-BMI-Vertical'
print('Storing :',sDatabaseName,'\n Table:',sTable) DimPersonIndex.to_sql(sTable, conn2, if_exists="replace") sTable = 'Dim-BMI-Vertical'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL="SELECT * FROM [Dim-BMI-Vertical];"
PersonFrame2=pd.read_sql_query(sSQL, conn2) print('Full Data Set (Rows):', PersonFrame0.shape[0]) print('Full Data Set (Columns):', PersonFrame0.shape[1])
print('Horizontal Data Set (Rows):', PersonFrame2.shape[0]) print('Horizontal Data Set (Columns):', PersonFrame2.shape[1])



Output:



 
Secure Vault Style
Code:
import sys import os
import pandas as pd import sqlite3 as sq
Base = 'D:/Dinesh/DS/8/'


print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sDataWarehouseDir=Base + 'csv'
if not os.path.exists(sDataWarehouseDir): os.makedirs(sDataWarehouseDir)

sDatabaseName=sDataWarehouseDir + '/datawarehouse.db' conn1 = sq.connect(sDatabaseName) sDatabaseName=sDataWarehouseDir + '/datamart.db' conn2 = sq.connect(sDatabaseName)

print('################')
sTable = 'Dim-BMI'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL="SELECT * FROM [Dim-BMI];"
PersonFrame0=pd.read_sql_query(sSQL, conn1) print('################')

sTable = 'Dim-BMI'
print('Loading :',sDatabaseName,' Table:',sTable) sSQL="SELECT \
Height,\
 
Weight,\ Indicator,\ CASE Indicator\
WHEN 1 THEN 'Pip'\
WHEN 2 THEN 'Norman'\ WHEN 3 THEN 'Grant'\ ELSE 'Sam'\
END AS Name\ FROM [Dim-BMI]\
WHERE Indicator > 2\ ORDER BY \
Height,\ Weight;"
PersonFrame1=pd.read_sql_query(sSQL, conn1) DimPerson=PersonFrame1 DimPersonIndex=DimPerson.set_index(['Indicator'],inplace=False) sTable = 'Dim-BMI-Secure'

print('\n#################################')
print('Storing :',sDatabaseName,'\n Table:',sTable) print('\n#################################')

DimPersonIndex.to_sql(sTable, conn2, if_exists="replace") print('################################')
sTable = 'Dim-BMI-Secure'
print('Loading :',sDatabaseName,' Table:',sTable)

print('################################')
sSQL="SELECT * FROM [Dim-BMI-Secure] WHERE Name = 'Sam';"
PersonFrame2=pd.read_sql_query(sSQL, conn2) print('################################')
 
print('Full Data Set (Rows):', PersonFrame0.shape[0]) print('Full Data Set (Columns):', PersonFrame0.shape[1]) print('################################')

print('Horizontal Data Set (Rows):', PersonFrame2.shape[0]) print('Horizontal Data Set (Columns):', PersonFrame2.shape[1]) print('Only Sam Data')
print(PersonFrame2.head())




Output:



 
Association Rule Mining
Code:
import sys import os
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules


Base = 'D:/Dinesh/DS/8/' print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')
InputFileName = 'csv/Online-Retail-Billboard.xlsx' EDSAssessDir = '02-Assess/01-EDS' InputAssessDir = EDSAssessDir + '/02-Python' sFileAssessDir = Base + '/' + InputAssessDir
if not os.path.exists(sFileAssessDir): os.makedirs(sFileAssessDir)

sFileName = Base + '/' + InputFileName df = pd.read_excel(sFileName) print(df.shape)
df['Description'] = df['Description'].str.strip() df.dropna(axis=0, subset=['InvoiceNo'], inplace=True) df['InvoiceNo'] = df['InvoiceNo'].astype('str')
df = df[~df['InvoiceNo'].str.contains('C')]


basket = (df[df['Country'] == "France"]
.groupby(['InvoiceNo', 'Description'])['Quantity']
.sum().unstack().reset_index().fillna(0)
.set_index('InvoiceNo'))
 
def encode_units(x): if x <= 0:
return False return True

basket_sets = basket.apply(lambda x: x.map(encode_units)) basket_sets.drop('POSTAGE', inplace=True, axis=1)
frequent_itemsets = apriori(basket_sets, min_support=0.07, use_colnames=True) rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1, num_itemsets=None)
print(rules.head())


print(rules[(rules['lift'] >= 6) & (rules['confidence'] >= 0.8)]) sProduct1 = 'ALARM CLOCK BAKELIKE GREEN'
print(sProduct1) print(basket[sProduct1].sum())

sProduct2 = 'ALARM CLOCK BAKELIKE RED'
print(sProduct2) print(basket[sProduct2].sum())

basket2 = (df[df['Country'] == "Germany"]
.groupby(['InvoiceNo', 'Description'])['Quantity']
.sum().unstack().reset_index().fillna(0)
.set_index('InvoiceNo'))

basket_sets2 = basket2.apply(lambda x: x.map(encode_units)) basket_sets2.drop('POSTAGE', inplace=True, axis=1)

frequent_itemsets2 = apriori(basket_sets2, min_support=0.05, use_colnames=True) rules2 = association_rules(frequent_itemsets2, metric="lift", min_threshold=1,
 
num_itemsets=None)
print(rules2[(rules2['lift'] >= 4) & (rules2['confidence'] >= 0.5)]) print('### Done!! ############################################')



Output:



 
Create a Network Routing Diagram
Code:
import sys import os
import pandas as pd import networkx as nx
import matplotlib.pyplot as plt pd.options.mode.chained_assignment = None Base = 'D:/Dinesh/DS/8/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sInputFileName='csv/Assess-Network-Routing-Company.csv' ################################################################
sOutputFileName1='05-Organise/01-EDS/02-Python/Organise-Network-Routing-Company.gml' sOutputFileName2='05-Organise/01-EDS/02-Python/Organise-Network-Routing-Company.png' sFileName=Base + '/' + sInputFileName
print('################################')
print('Loading :',sFileName) print('################################')
CompanyData=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") print('################################')
print(CompanyData.head()) print(CompanyData.shape) G=nx.Graph()
for i in range(CompanyData.shape[0]): for j in range(CompanyData.shape[0]):
Node0=CompanyData['Company_Country_Name'][i] Node1=CompanyData['Company_Country_Name'][j] if Node0 != Node1:
G.add_edge(Node0,Node1)
 
for i in range(CompanyData.shape[0]): Node0=CompanyData['Company_Country_Name'][i] Node1=CompanyData['Company_Place_Name'][i] + '('+
CompanyData['Company_Country_Name'][i] + ')' if Node0 != Node1:
G.add_edge(Node0,Node1) print('Nodes:', G.number_of_nodes()) print('Edges:', G.number_of_edges())
output_dir = os.path.dirname(Base + '/' + sOutputFileName1) if not os.path.exists(output_dir):
os.makedirs(output_dir)
sFileName = Base + '/' + sOutputFileName1 print('################################')
print('Storing :', sFileName) print('################################')
nx.write_gml(G, sFileName) sFileName=Base + '/' + sOutputFileName2
print('################################')
print('Storing Graph Image:',sFileName) print('################################')
plt.figure(figsize=(15, 15)) pos=nx.spectral_layout(G,dim=2)
nx.draw_networkx_nodes(G,pos, node_color='k', node_size=10, alpha=0.8) nx.draw_networkx_edges(G, pos,edge_color='r', arrows=False, style='dashed') nx.draw_networkx_labels(G,pos,font_size=12,font_family='sans-serif',font_color='b') plt.axis('off')
plt.savefig(sFileName,dpi=600) plt.show()
print('################################') print('### Done!! #####################')
 
Output:






 
Picking Content for Billboards
Code:
import sys import os
import pandas as pd import networkx as nx
import matplotlib.pyplot as plt import numpy as np
pd.options.mode.chained_assignment = None Base = 'D:/Dinesh/DS/8/' print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sInputFileName='csv/Assess-DE-Billboard-Visitor.csv' sOutputFileName1='05-Organise/01-EDS/02-Python/Organise-Billboards.gml' sOutputFileName2='05-Organise/01-EDS/02-Python/Organise-Billboards.png' sFileName=Base + '/' + sInputFileName print('################################')
print('Loading :',sFileName) print('################################')
BillboardDataRaw=pd.read_csv(sFileName,header=0,low_memory=False, encoding="latin-1") print('################################')
print(BillboardDataRaw.head()) print(BillboardDataRaw.shape) BillboardData=BillboardDataRaw sSample=list(np.random.choice(BillboardData.shape[0],20)) G=nx.Graph()
for i in sSample: for j in sSample:
Node0=BillboardData['BillboardPlaceName'][i] + '('+ BillboardData['BillboardCountry'][i]
+ ')'
 

+ ')'
 
Node1=BillboardData['BillboardPlaceName'][j] + '('+ BillboardData['BillboardCountry'][i]


if Node0 != Node1: G.add_edge(Node0,Node1)
 
for i in sSample:
Node0=BillboardData['BillboardPlaceName'][i] + '('+ BillboardData['VisitorPlaceName'][i] +
 
')'
 


Node1=BillboardData['BillboardPlaceName'][i] + '('+ BillboardData['VisitorCountry'][i] + ')' if Node0 != Node1:
G.add_edge(Node0,Node1)
 
print('Nodes:', G.number_of_nodes()) print('Edges:', G.number_of_edges()) sFileName = Base + '/' + sOutputFileName1 print('################################')
print('Storing :', sFileName) print('################################')
nx.write_gml(G, sFileName)
sFileName = Base + '/' + sOutputFileName2 print('################################')
print('Storing Graph Image:', sFileName) print('################################')
plt.figure(figsize=(15, 15))
pos = nx.circular_layout(G, dim=2)
nx.draw_networkx_nodes(G, pos, node_color='k', node_size=150, alpha=0.8) nx.draw_networkx_edges(G, pos, edge_color='r', arrows=False, style='solid') nx.draw_networkx_labels(G, pos, font_size=12, font_family='sans-serif', font_color='b') plt.axis('off')
plt.savefig(sFileName, dpi=600) plt.show()
print('### Done!! #####################') print('################################')
 
Output:








 
Create a Delivery Route
Code:
import sys import os
import pandas as pd Base = 'D:/Dinesh/DS/8/'
print('################################')
print('Working Base :',Base, ' using ', sys.platform) print('################################')
sInputFileName='csv/Assess_Shipping_Routes.txt' sOutputFileName='05-Organise/01-EDS/02-Python/Organise-Routes.csv' sFileName=Base + '/' + sInputFileName print('################################')
print('Loading :',sFileName) print('################################')
RouteDataRaw=pd.read_csv(sFileName,header=0,low_memory=False, sep='|', encoding="latin- 1")
RouteStart=RouteDataRaw[RouteDataRaw['StartAt']=='WH-KA13'] RouteDistance=RouteStart[RouteStart['Cost']=='DistanceMiles'] RouteDistance=RouteDistance.sort_values(by=['Measure'], ascending=False) RouteMax=RouteStart["Measure"].max() RouteMaxCost=round((((RouteMax/1000)*1.5*2)),2) print('################################')
print('Maximum (£) per day:') print(RouteMaxCost) RouteMean=RouteStart["Measure"].mean()
RouteMeanMonth=round((((RouteMean/1000)*2*30)),6) print('################################')
print('Mean per Month (Miles):')
 
print(RouteMeanMonth) print('################################')
Output:







Simple Forex Trading Planner
Code:
import sys import os
import pandas as pd import sqlite3 as sq import re

Base = 'D:/Dinesh/DS/8/' print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')
sInputFileName = 'csv/Process_ExchangeRates.csv' sOutputFileName = 'Organise-Forex.csv' sDatabaseName = Base + '/' + 'csv/clark.db'
conn = sq.connect(sDatabaseName)
 
sFileName = Base + '/' + sInputFileName print('################################')
print('Loading :', sFileName) print('################################')
ForexDataRaw = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin-1") print('################################')
ForexDataRaw.index.names = ['RowID'] sTable = 'Forex_All'
print('Storing :', sDatabaseName, ' Table:', sTable) ForexDataRaw.to_sql(sTable, conn, if_exists="replace")

sSQL = """
SELECT 1 as Bag,
CAST(min(Date) AS VARCHAR(10)) as Date, CAST(1000000.0000000 as NUMERIC(12,4)) as Money,
'USD' as Currency FROM Forex_All;
"""
sSQL = re.sub("\s\s+", " ", sSQL)
nMoney = pd.read_sql_query(sSQL, conn) nMoney.index.names = ['RowID']
sTable = 'MoneyData'
print('Storing :', sDatabaseName, ' Table:', sTable) nMoney.to_sql(sTable, conn, if_exists="replace") sTable = 'TransactionData'
print('Storing :', sDatabaseName, ' Table:', sTable) nMoney.to_sql(sTable, conn, if_exists="replace")
ForexDay = pd.read_sql_query("SELECT Date FROM Forex_All GROUP BY Date;", conn)

t = 0
for i in range(ForexDay.shape[0]):
 
sDay = ForexDay['Date'][i] sSQL = f"""
SELECT M.Bag as Bag, F.Date as Date,
round(M.Money * F.Rate, 6) AS Money, F.CodeIn AS PCurrency,
F.CodeOut AS Currency


FROM MoneyData AS M JOIN (
SELECT CodeIn, CodeOut, Date, Rate FROM Forex_All
WHERE CodeIn = "USD" AND CodeOut = "GBP" UNION
SELECT CodeOut AS CodeIn, CodeIn AS CodeOut, Date, (1/Rate) AS Rate FROM Forex_All
WHERE CodeIn = "USD" AND CodeOut = "GBP"
) AS F
ON M.Currency = F.CodeIn AND F.Date = "{sDay}"; """
sSQL = re.sub("\s\s+", " ", sSQL)
ForexDayRate = pd.read_sql_query(sSQL, conn) for j in range(ForexDayRate.shape[0]):
sBag = str(ForexDayRate['Bag'][j])
nMoney = str(round(ForexDayRate['Money'][j], 2)) sCodeIn = ForexDayRate['PCurrency'][j] sCodeOut = ForexDayRate['Currency'][j]
sSQL = f"""
UPDATE MoneyData SET Date = "{sDay}",
 
Money = {nMoney}, Currency = "{sCodeOut}"
WHERE Bag = {sBag} AND Currency = "{sCodeIn}"; """
sSQL = re.sub("\s\s+", " ", sSQL) cur = conn.cursor() cur.execute(sSQL) conn.commit()
t += 1
print('Trade:', t, sDay, sCodeOut, nMoney) sSQL = f"""
INSERT INTO TransactionData ( RowID, Bag, Date, Money, Currency
)
SELECT {t} AS RowID, Bag, Date, Money,
Currency FROM MoneyData; """
sSQL = re.sub("\s\s+", " ", sSQL) cur.execute(sSQL) conn.commit()



sSQL = "SELECT RowID, Bag, Date, Money, Currency FROM TransactionData ORDER BY RowID;"
sSQL = re.sub("\s\s+", " ", sSQL)
TransactionData = pd.read_sql_query(sSQL, conn) CompanyPath = os.path.join(Base)
if not os.path.exists(CompanyPath): os.makedirs(CompanyPath)
 
OutputFile = os.path.join(CompanyPath, sOutputFileName)


print(f"Saving the file to: {OutputFile}") TransactionData.to_csv(OutputFile, index=False) print('################################') print('### Done!! #####################') print('################################')

Output:



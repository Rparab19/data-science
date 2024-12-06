import sys import os
import pandas as pd import sqlite3 as sq
from pandas.io import sql Base = 'D:/Dinesh/DS/5/'
print('################################')
print('Working Base :', Base, ' using ', sys.platform) print('################################')

InputDir = 'csv'
InputFileName1 = 'Retrieve_Product.csv' InputFileName2 = 'Retrieve_Box.csv' InputFileName3 = 'Retrieve_Container.csv' EDSDir = '02-Assess/01-EDS'
OutputDir = EDSDir + '/02-Python'
OutputFileName = 'Assess_Shipping_Containers.csv' sFileDir = Base + '/' + EDSDir
os.makedirs(sFileDir, exist_ok=True)

sFileDir = Base + '/' + OutputDir os.makedirs(sFileDir, exist_ok=True)

sDataBaseDir = Base + '/' + '/02-Assess/SQLite'
 
os.makedirs(sDataBaseDir, exist_ok=True)


sDatabaseName = sDataBaseDir + '/hillman.db' conn = sq.connect(sDatabaseName)
sFileName = Base + '/' + InputDir + '/' + InputFileName1 print('###########')
print('Loading :', sFileName)
ProductRawData = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin-1") ProductRawData.drop_duplicates(subset=None, keep='first', inplace=True) ProductRawData.index.name = 'IDNumber'
ProductData = ProductRawData[ProductRawData.Length <= 0.5].head(10) print('Loaded Product :', ProductData.columns.values) print('################################')
sTable = 'Assess_Product'
print('Storing :', sDatabaseName, ' Table:', sTable) ProductData.to_sql(sTable, conn, if_exists="replace", index=False)

print(ProductData.head()) print('################################')
print('Rows : ', ProductData.shape[0]) print('################################')
sFileName = Base + '/' + InputDir + '/' + InputFileName2



print('###########')
print('Loading :', sFileName)
BoxRawData = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin-1") BoxRawData.drop_duplicates(subset=None, keep='first', inplace=True) BoxRawData.index.name = 'IDNumber'
BoxData = BoxRawData[BoxRawData.Length <= 1].head(1000) print('Loaded Box :', BoxData.columns.values)
 
print('################################')
sTable = 'Assess_Box'
print('Storing :', sDatabaseName, ' Table:', sTable) BoxData.to_sql(sTable, conn, if_exists="replace", index=False)

print(BoxData.head()) print('################################')
print('Rows : ', BoxData.shape[0]) print('################################')
sFileName = Base + '/' + InputDir + '/' + InputFileName3 print('###########')
print('Loading :', sFileName)
ContainerRawData = pd.read_csv(sFileName, header=0, low_memory=False, encoding="latin- 1")
ContainerRawData.drop_duplicates(subset=None, keep='first', inplace=True) ContainerRawData.index.name = 'IDNumber'
ContainerData = ContainerRawData[ContainerRawData.Length <= 2].head(10) print('Loaded Container :', ContainerData.columns.values) print('################################')
sTable = 'Assess_Container'
print('Storing :', sDatabaseName, ' Table:', sTable) ContainerData.to_sql(sTable, conn, if_exists="replace", index=False)
print(ContainerData.head()) print('################################')
print('Rows : ', ContainerData.shape[0]) print('################################') print('################')
sView = 'Assess_Product_in_Box'
print('Creating :', sDatabaseName, ' View:', sView)
 
sSQL = "DROP VIEW IF EXISTS " + sView + ";"
conn.execute(sSQL)



sSQL = """
CREATE VIEW """ + sView + """ AS SELECT
P.UnitNumber AS ProductNumber, B.UnitNumber AS BoxNumber, (B.Thickness * 1000) AS PackSafeCode,
(B.BoxVolume - P.ProductVolume) AS PackFoamVolume,
((B.Length * 10) * (B.Width * 10) * (B.Height * 10)) * 167 AS Air_Dimensional_Weight, ((B.Length * 10) * (B.Width * 10) * (B.Height * 10)) * 333 AS Road_Dimensional_Weight, ((B.Length * 10) * (B.Width * 10) * (B.Height * 10)) * 1000 AS Sea_Dimensional_Weight, P.Length AS Product_Length,
P.Width AS Product_Width, P.Height AS Product_Height,
P.ProductVolume AS ProductVolume, P.ProductVolume AS Product_cm_Volume,
((P.Length * 10) * (P.Width * 10) * (P.Height * 10)) AS Product_ccm_Volume, (B.Thickness * 0.95) AS Minimum_Pack_Foam,
(B.Thickness * 1.05) AS Maximum_Pack_Foam,
B.Length - (B.Thickness * 1.10) AS Minimum_Product_Box_Length, B.Length - (B.Thickness * 0.95) AS Maximum_Product_Box_Length, B.Width - (B.Thickness * 1.10) AS Minimum_Product_Box_Width, B.Width - (B.Thickness * 0.95) AS Maximum_Product_Box_Width, B.Height - (B.Thickness * 1.10) AS Minimum_Product_Box_Height, B.Height - (B.Thickness * 0.95) AS Maximum_Product_Box_Height, B.Length AS Box_Length,
B.Width AS Box_Width, B.Height AS Box_Height,
 
B.BoxVolume AS Box_cm_Volume,
((B.Length * 10) * (B.Width * 10) * (B.Height * 10)) AS Box_ccm_Volume,
(2 * B.Length * B.Width) + (2 * B.Length * B.Height) + (2 * B.Width * B.Height) AS Box_sqm_Area,
((B.Length * 10) * (B.Width * 10) * (B.Height * 10)) * 3.5 AS Box_A_Max_Kg_Weight, ((B.Length * 10) * (B.Width * 10) * (B.Height * 10)) * 7.7 AS Box_B_Max_Kg_Weight, ((B.Length * 10) * (B.Width * 10) * (B.Height * 10)) * 10.0 AS Box_C_Max_Kg_Weight

FROM
Assess_Product as P, Assess_Box as B
WHERE
P.Length >= (B.Length - (B.Thickness * 1.10)) AND P.Width >= (B.Width - (B.Thickness * 1.10)) AND P.Height >= (B.Height - (B.Thickness * 1.10))
AND P.Length <= (B.Length - (B.Thickness * 0.95)) AND P.Width <= (B.Width - (B.Thickness * 0.95)) AND P.Height <= (B.Height - (B.Thickness * 0.95)) AND (B.Height - B.Thickness) >= 0
AND (B.Width - B.Thickness) >= 0 AND (B.Height - B.Thickness) >= 0;
"""

conn.execute(sSQL) print('################') print('################')
sView = 'Assess_Pallet_Analysis'
print('Creating :', sDatabaseName, ' View:', sView)
 
sSQL = "DROP VIEW IF EXISTS " + sView + ";"
conn.execute(sSQL)


sSQL = """
CREATE VIEW """ + sView + """ AS SELECT
A.BoxNumber, A.ProductNumber, A.PackSafeCode, A.PackFoamVolume, A.Air_Dimensional_Weight, A.Road_Dimensional_Weight, A.Sea_Dimensional_Weight, A.Product_Length, A.Product_Width, A.Product_Height, A.ProductVolume, A.Product_cm_Volume, A.Product_ccm_Volume, A.Minimum_Pack_Foam, A.Maximum_Pack_Foam,
A.Minimum_Product_Box_Length, A.Maximum_Product_Box_Length, A.Minimum_Product_Box_Width, A.Maximum_Product_Box_Width, A.Minimum_Product_Box_Height, A.Maximum_Product_Box_Height, A.Box_Length,
A.Box_Width, A.Box_Height, A.Box_cm_Volume,
 
A.Box_ccm_Volume, A.Box_sqm_Area, A.Box_A_Max_Kg_Weight, A.Box_B_Max_Kg_Weight, A.Box_C_Max_Kg_Weight
FROM
Assess_Product_in_Box as A; """
conn.execute(sSQL) print('################')
sSQL = "SELECT * FROM Assess_Pallet_Analysis" PalletData = pd.read_sql(sSQL, conn) print(PalletData.head())
print('Rows in Pallet Analysis :', PalletData.shape[0]) sFileName = Base + '/' + OutputDir + '/' + OutputFileName print('Exporting :', sFileName) PalletData.to_csv(sFileName, index=False) print('################################')

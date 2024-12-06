import os
import pandas as pd
from geopy.geocoders import Nominatim from geopy.exc import GeocoderTimedOut import time
geolocator = Nominatim(user_agent="Dinesh_GeoLocator_Application") InputFileName = 'Retrieve_GB_Postcode_Warehouse.csv' OutputFileName = 'Assess_GB_Warehouse_Address.csv'
Base = 'D:/Dinesh/DS/5/'
print('Working Base :', Base, ' using Windows') # Directory for output files
sFileDir = os.path.join(Base, '5f') if not os.path.exists(sFileDir):
os.makedirs(sFileDir)
sFileName = os.path.join(Base, InputFileName) print('###########')
print('Loading :', sFileName)
Warehouse = pd.read_csv(sFileName, header=0, low_memory=False) Warehouse.sort_values(by='postcode', ascending=1, inplace=True) WarehouseGoodHead = Warehouse[Warehouse.latitude != 0].head(5) WarehouseGoodTail = Warehouse[Warehouse.latitude != 0].tail(5) def get_address(row, attempts=3):
point = f"{row['latitude']},{row['longitude']}" for attempt in range(attempts):
 
try:
return geolocator.reverse(point, timeout=10).address except GeocoderTimedOut:
if attempt < attempts - 1: # if not the last attempt time.sleep(1) # wait a bit before retrying continue
else:
return None # return None if all attempts fail return None
WarehouseGoodHead['Warehouse_Address'] = WarehouseGoodHead.apply(get_address, axis=1)
WarehouseGoodHead = WarehouseGoodHead.drop(columns=['id', 'postcode'], errors='ignore') WarehouseGoodTail['Warehouse_Address'] = WarehouseGoodTail.apply(get_address, axis=1) WarehouseGoodTail = WarehouseGoodTail.drop(columns=['id', 'postcode'], errors='ignore') WarehouseGood = pd.concat([WarehouseGoodHead, WarehouseGoodTail], ignore_index=True) print(WarehouseGood)
sFileName = os.path.join(sFileDir, OutputFileName) WarehouseGood.to_csv(sFileName, index=False)
print('### Done!! ############################################')

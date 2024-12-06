from PIL import Image import pandas as pd
import matplotlib.pyplot as plt import numpy as np
sInputFileName = 'D:/Dinesh/DS/picture_horus/Angus.jpg' # Update this path to your image location
InputData = Image.open(sInputFileName).convert('RGBA') InputData = np.array(InputData)
plt.imshow(InputData) plt.title('Image Preview') plt.show()
print('Input Data Values ===================================')
print('X: ', InputData.shape[0]) # Height print('Y: ', InputData.shape[1]) # Width print('RGBA: ', InputData.shape[2]) # Channels
print('=====================================================')
ProcessRawData = InputData.flatten()
y = InputData.shape[2] # Number of channels x = int(ProcessRawData.shape[0] / y)
ProcessData = pd.DataFrame(np.reshape(ProcessRawData, (x, y)), columns=['Red', 'Green', 'Blue', 'Alpha'])
ProcessData['XAxis'] = np.repeat(np.arange(InputData.shape[1]), InputData.shape[0]) ProcessData['YAxis'] = np.tile(np.arange(InputData.shape[0]), InputData.shape[1]) ProcessData = ProcessData[['XAxis', 'YAxis', 'Red', 'Green', 'Blue', 'Alpha']] print('Rows: ', ProcessData.shape[0])
print('Columns:', ProcessData.shape[1])
 
print('=====================================================')
print('Process Data Values =================================') print(ProcessData.head()) print('=====================================================')
output_file = 'D:/Dinesh/DS/picture_horus/Image_to_HORUS.csv' # Adjust the output file path if needed
print(f'Storing File at: {output_file}') ProcessData.to_csv(output_file, index=False)
print('=====================================================')
print('Picture to HORUS - Done') print('=====================================================')

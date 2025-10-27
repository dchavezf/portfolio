import zipfile
import os
import time

timestamp = int(time.time())
source_file = 'd:\Users\dchav\OneDrive\Developer\portfolio\data\exchange\XR_Candles.parquet'
destination_zip = f'd:\Users\dchav\OneDrive\Developer\portfolio\data\exchange\XR_Candles_{timestamp}.parquet.zip'

with zipfile.ZipFile(destination_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
    zipf.write(source_file, os.path.basename(source_file))


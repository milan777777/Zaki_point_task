import ijson
import pandas as pd



rate = []

with open("MagnaCarePPO_In-Network.json", 'r') as f:
        for item in ijson.items(f, 'in_network.item'):
            rate.append(item)


rate_df = pd.DataFrame(rate)


print(rate_df)
rate_df.to_json('rate.json',orient='records',indent=3)
rate_df.to_parquet('rate.parquet',index=False)

# import ijson
# import zipfile
# import json
# import decimal as Decimal 

# with open('example.json','w') as out_file:
#     with zipfile.ZipFile('/home/milan-thapa/Downloads/MagnaCarePPO_In-Network.zip', 'r') as zf:
#         for name in zf.namelist():
#             with zf.open(name, 'r') as f:
#                 print(name)
#                 for item in ijson.items(f, 'provider_references.item'):
  
#                     out_file.write(json.dumps(item) + '\n')



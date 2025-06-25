import ijson
import pandas as pd



provider = []

with open("MagnaCarePPO_In-Network.json", 'r') as f:
        for item in ijson.items(f, 'provider_references.item'):
            provider.append(item)


provider_df = pd.DataFrame(provider)


print(provider_df)
provider_df.to_json('provider.json',orient='records',indent=3)
provider_df.to_parquet('provider.parquet',index=False)
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
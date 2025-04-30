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

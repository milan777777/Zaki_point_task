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


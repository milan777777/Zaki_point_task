# import zipfile
# import json
# import ijson

# def extract_data(zip_name, output_name):
#     with zipfile.ZipFile(zip_name) as zf:
#         with zf.open(zf.namelist()[0]) as f:
#             data = json.load(f)
            
#             with open(f'{output_name}_providers.json', 'w') as out:
#                 json.dump(data['provider_references'], out, indent=2)
                
#             with open(f'{output_name}_network.json', 'w') as out:
#                 json.dump(data['in_network'], out, indent=2)

# extract_data('/home/milan-thapa/Downloads/MagnaCarePPO_In-Network.zip', 'output1')
# extract_data('/home/milan-thapa/Downloads/MagnaCarePPO_In-Network (copy).zip', 'output2')
import ijson
import zipfile
import json

def extract_data(zip_name, output_name):
    with zipfile.ZipFile(zip_name) as zf:
        with zf.open(zf.namelist()[0]) as f:
            with open(f'{output_name}_providers.json', 'w') as out:
                out.write('[\n')
                items = ijson.items(f, 'provider_references.item')
                out.write(',\n'.join(json.dumps(item) for item in items))
                out.write('\n]')
            
            f.seek(0)
            with open(f'{output_name}_network.json', 'w') as out:
                out.write('[\n')
                items = ijson.items(f, 'in_network.item')
                out.write(',\n'.join(json.dumps(item) for item in items))
                out.write('\n]')

extract_data('/home/milan-thapa/Downloads/MagnaCarePPO_In-Network.zip', 'output1')
extract_data('/home/milan-thapa/Downloads/MagnaCarePPO_In-Network (copy).zip', 'output2')
import zipfile
import ijson 
import json  
import os 
from decimal import Decimal

def extract_import(zip_path):
    def con_decimal(obj): 
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError  
    
    output_path = os.path.join(os.getcwd(),'output_file')
    os.makedirs(output_path, exist_ok=True) 

    in_path = os.path.join(output_path, 'in_network.json')
    prov_path = os.path.join(output_path, 'provider_ref.json')

    with zipfile.ZipFile(zip_path, 'r') as z:
        for data in z.namelist():
            with z.open(data, 'r') as f:
                with open(in_path,'w') as in_file:
                    for item in ijson.items(f, 'in_network.item'):
                        in_file.write(json.dumps(item, default=con_decimal) + '\n')

                f.seek(0)

                with open(prov_path,'w') as prov_file:
                    for item in ijson.items(f, 'provider_references.item'):
                        prov_file.write(json.dumps(item,default=con_decimal) + '\n')

    return in_path,prov_path
 
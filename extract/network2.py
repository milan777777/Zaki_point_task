import zipfile
import ijson #for large dataset
import json #supports json file 
import os # helps in file navigation,helps to make directory 
from decimal import Decimal # for decimal calculation to change decimal into float

def extract_import(zip_path): #define function 
    def con_decimal(obj): # for managing decimal value turning decimal into float 
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError #if comes error in above condition comes type error 
    
    output_path = os.path.join(os.getcwd(),'output_file')#makes working directory 
    os.makedirs(output_path, exist_ok=True) #helps to make folder and put in output path(os.ma is a directory)

    in_path = os.path.join(output_path, 'in_network.json')#in_path for in_network.json(only path)
    prov_path = os.path.join(output_path, 'provider_ref.json')#prov_path for provider.json

    with zipfile.ZipFile(zip_path, 'r') as z:#reading the zipfile and z is alias,importing the zipfile package
        for data in z.namelist():#we move to the directory and namelist gives all the files in the directory
            with z.open(data, 'r') as f:# r helps to read the file 
                with open(in_path,'w') as in_file:# w helps to write the file,
                    for item in ijson.items(f, 'in_network.item'):
                        in_file.write(json.dumps(item, default=con_decimal) + '\n')#json.dump helps to convert python file into json format and call above function 

                f.seek(0)# gives timeout message if file not found 

                with open(prov_path,'w') as prov_file:
                    for item in ijson.items(f, 'provider_references.item'):
                        prov_file.write(json.dumps(item,default=con_decimal) + '\n')

    return in_path,prov_path
 
import sys 
from extract import network2  
from scrub import scrubbing 
from load import loading

def main(): 
    zip_path = sys.argv[1] 

    in_path,prov_path = network2.extract_import(zip_path) 
    rate_path,provider_path = scrubbing.scrub_import(in_path,prov_path) 
    loading.load_file(rate_path,provider_path) 

if __name__ == "__main__":
    main()
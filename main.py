import sys #helps in execution 
from extract import network2 #extract folder bata extract file import 
from scrub import scrubbing #
from load import loading

def main(): #main function 
    zip_path = sys.argv[1] #1 gives the input, 0 is the name of python file 

    in_path,prov_path = network2.extract_import(zip_path) #zip file pathako 
    rate_path,provider_path = scrubbing.scrub_import(in_path,prov_path) #json file ko path pathako 
    loading.load_file(rate_path,provider_path) #parquet file ko path pathako 

if __name__ == "__main__": #main call gareko yo chai 
    main()
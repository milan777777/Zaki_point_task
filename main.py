import sys 
from etl import ETL
import logging
import argparse

def main(): 
    # zip_path = sys.argv[1] 
    parser = argparse.ArgumentParser(description="ETL pipeline for processing ZIP files containing rate and provider data.")
    parser.add_argument("--zip_path",required=True , help="Path to the ZIP file to process")
    parser.add_argument("--provider_detail", help="Path to tht provider_detail.json")

    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO,filename="logging_file.log")

    logger = logging.getLogger("ETL")
    etl = ETL(logger)
    etl.execute(args,logger)


    # in_path,prov_path = network2.extract_import(zip_path) 
    # rate_path,provider_path = scrubbing.scrub_import(in_path,prov_path,etl) 
    # loading.load_file(rate_path,provider_path,etl) 

if __name__ == "__main__":
    main()
    
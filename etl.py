import yaml 
from pyspark.sql import SparkSession
from extract import network2  
from scrub import scrubbing 
from load import loading



class ETL:
    def __init__(self,logger):
        with open("new.yml", 'r') as file:
            new = yaml.safe_load(file)

            
        self.logger = logger
        self.logger.info("Initializing ETL") 

        self.core = new['SPARK']['EXECUTOR']
        self.cores = new['SPARK']['EXECUTOR']['CORES']
        self.instances = new['SPARK']['EXECUTOR']['INSTANCES']
        self.memory = new['SPARK']['EXECUTOR']['MEMORY']
        self.memories = new['SPARK']['DRIVER']['MEMORY']

        self.dbname = new['POSTGRES']['DATABASE']
        self.host = new['POSTGRES']['HOST']
        self.port = new['POSTGRES']['PORT']
        self.user = new['POSTGRES']['USER']
        self.password = new['POSTGRES']['PASSWORD']

        self.spark = SparkSession.builder \
            .appName("ETL") \
            .config("spark.driver.memory", self.memories )\
            .getOrCreate()
        
    
    def execute(self,zip_path):
       
        self.logger.info("Extract")
        net_file, prov_path = network2.extract_import(zip_path)

        self.logger.info("Scrub")
        rate_path,provider_path = scrubbing.scrub_import(net_file, prov_path, self)

        self.logger.info("Load")
        loading.load_file(rate_path,provider_path, self)





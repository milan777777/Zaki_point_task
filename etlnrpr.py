import yaml 
from extract import extractnr_pr 
from scrub import scrubnrpr 
from process import process
from load import loadnr_pr
from pyspark.sql import SparkSession

class ETL:
    def __init__(self,logger,driver_memory="4g"):
        self.driver_memory = driver_memory
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
        
    
    def execute(self,args,logger):
       
        output_folder = extractnr_pr.extract_nr_pr(args.folder)

        self.spark = SparkSession.builder.appName("etl").config("spark.driver.memory", self.driver_memory).getOrCreate()
        np_data,provider_rep,df_array3,df_rate2,df_join = scrubnrpr.scrub_nrpr(output_folder,args.pro,self)

        network_nrpr,providernr_pr = process.process(np_data,provider_rep,df_array3,df_join,logger)
        loadnr_pr.load_import(network_nrpr,providernr_pr,df_rate2,logger,self)
        








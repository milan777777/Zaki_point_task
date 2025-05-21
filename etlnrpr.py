import yaml 
from extract import extractnew 




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
        
    
    def execute(self,folder):
       
        self.logger.info("Extract")
        extractnew.extract_nr_pr(folder)
        








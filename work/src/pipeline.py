from os import getenv
import timeit
import findspark 
findspark.init() 
from pyspark import SparkFiles
from pyspark.sql import SparkSession



class Pipeline():

    def __init__(self):
        super().__init__()
        
        """
        start a new pyspark session when new pipeline created
        security

        """
        self.SPARK_WAREHOUSE = getenv('SPARK_WAREHOUSE')

        print('----------START CREATION OF SPARK SESSION----------')

        self.spark=SparkSession.builder.appName("ETL pipeline")\
        .config("spark.sql.warehouse.dir", self.SPARK_WAREHOUSE)\
        .config('spark.driver.extraJavaOptions',f'-Dderby.system.home=data/catalog')\
        .config("spark.io.encryption.enabled",True)\
        .config('spark.acls.enable',True)\
        .enableHiveSupport().getOrCreate()

        self.sc=self.spark.sparkContext
    
  
  
    def http_download_files(self,bucket,files):

        print('----------START DOWNLOAD FILES FROM URLS INTO SPARK NODES----------')

        """
        distributed download of HTTP files on every node
        
        """

        self.files=files

        for file in self.files:
            start = timeit.default_timer()
            self.sc.addFile(bucket+file)
            print(f'Successful download of {file.upper()} in {round(timeit.default_timer() - start,2)} seconds----------')  

            

    def store_data_with_catalog(self):
        """

        store data as parquet files into spark-warehouse 
        & build a data catalog to store databases metadata
        
        """
        print('----------CREATE SPARK DATAFRAMES FROM FILES & STORE DATA & METADATA INTO A WAREHOUSE----------')

        for file in self.files:

            start = timeit.default_timer()

            file_path=f'file://{SparkFiles.get(file)}'
            file_name=file.split(".")[0]

            if '.json' in file:
                df=self.spark.read.json(file_path)
                df.write.parquet(f'{self.SPARK_WAREHOUSE}/{file.split(".")[0]}')
                df.createOrReplaceTempView("order")
                
            elif '.csv' in file:
                df=self.spark.read.option('header',True).option("inferSchema",True).csv(file_path)
                df.createOrReplaceTempView('temp_table')
                self.spark.sql(f"drop table if exists {file_name}")
                self.spark.sql(f"create table {file_name} using parquet as select * from temp_table")


            print(f'Successful ingestion of {file.upper()} in {round(timeit.default_timer() - start,2)} seconds----------')  

        print('----------ETL PROCESS SUCCESSFULLY COMPLETED----------')

                    



  

        
import os
from os.path import abspath
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

        os.environ['PYSPARK_SUBMIT_ARGS']='--driver-memory 8G --executor-memory 8G pyspark-shell'
        self.spark=SparkSession.builder.appName("ETL pipeline")\
        .config("spark.sql.warehouse.dir", abspath('data/spark-warehouse'))\
        .config('spark.driver.extraJavaOptions',f'-Dderby.system.home={abspath("data/catalog")}')\
        .config("spark.io.encryption.enabled",True)\
        .enableHiveSupport().getOrCreate()

        self.sc=self.spark.sparkContext
    
  
  
    def http_download_files(self,bucket,files):

        """
        distributed download of HTTP files on every node
        
        """

        self.files=files

        for file in self.files:
            start = timeit.default_timer()
            self.sc.addFile(bucket+file)
            print(f'Download time of {file} :  {round(timeit.default_timer() - start,2)} seconds')  

            

    def store_data_with_catalog(self):
        """

        store data as parquet files into spark-warehouse 
        & build a data catalog to store databases metadata
        
        """

        for file in self.files:

            start = timeit.default_timer()

            file_path=f'file://{SparkFiles.get(file)}'
            file_name=file.split(".")[0]

            if '.json' in file:
                df=self.spark.read.json(file_path)
                df.write.parquet(f'./data/spark-warehouse/{file.split(".")[0]}')
            elif '.csv' in file:
                df=self.spark.read.option('header',True).option("inferSchema",True).csv(file_path)
                df.createOrReplaceTempView('temp_table')
                self.spark.sql(f"drop table if exists {file_name}")
                self.spark.sql(f"create table {file_name} using parquet as select * from temp_table")


            print(f'Ingestion time of {file} :  {round(timeit.default_timer() - start,2)} seconds')  

                    


    
  

        
import os
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

        """

        os.environ['PYSPARK_SUBMIT_ARGS']='--driver-memory 8G --executor-memory 8G pyspark-shell'
        self.spark=SparkSession.builder.getOrCreate()
        self.sc= self.spark.sparkContext
  
  
    def http_download_files(self,bucket,files):

        """
        distributed download of HTTP files on every node
        
        """
        
        self.files=files

        for file in self.files:
            start = timeit.default_timer()
            self.sc.addFile(bucket+file)
            print(f'Download time of {file} :  {round(timeit.default_timer() - start,2)} seconds')  

            

    def column_store_data(self):
        """
        create spark dataframes from files and store them, including the metadata, by columns in parquet files 
        
        """

        for file in self.files:

            start = timeit.default_timer()

            file_path=f'file://{SparkFiles.get(file)}'
            store_path=f'./data/{file.split(".")[0]}'

            if '.json' in file:
                df=self.spark.read.option('inferTimestamp','false').json(file_path)
                df.write.parquet(store_path)
            elif '.csv' in file:
                df=self.spark.read.option('header',True).option("inferSchema",True).csv(file_path)
                df.write.parquet(store_path)

            print(f'Ingestion time of {file} :  {round(timeit.default_timer() - start,2)} seconds')  

                    



  

        
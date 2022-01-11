import os 

from app.src.pipeline import Pipeline
from app.src.storage_verification import verify_storage_files

os.environ['SPARK_WAREHOUSE']='app/spark-warehouse'

bucket='https://ifood-data-architect-test-source.s3-sa-east-1.amazonaws.com/'
files=['restaurant.csv.gz','consumer.csv.gz','order.json.gz']

if __name__ =='__main__':

    files=verify_storage_files(files)

    if files:
        pyspark_pipeline=Pipeline()   
        pyspark_pipeline.http_download_files(bucket,files)
        pyspark_pipeline.store_data_with_catalog()


        

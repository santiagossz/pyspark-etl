# iFood Data Engineer Test

## 1st Case: ETL, Governance 

This is my proposed ETL pipeline for the [iFood Engineer Test](https://github.com/wiflore/ifood-data-engineering-test.git), to process distinct files and store its data & 
metadata in a structured way, taking into account data access and private data protection


* **Order** data comes from the file at https://ifood-data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz
* **Restaurant**  https://ifood-data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz
* **Consumer** https://ifood-data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz


## Solution

ETL: The data processing was done using Apache Spark Python interface **PySpark** to download and store the files.

Download each files using sparkContext.addFiles for distributed downloding on every worker node 
create a spark dataframe & store it in a paquet (columnar storage) in a warehouse 

Governance & Catalog: Spark dataframes are saved as hive tables to store all metadata inside a catalog. 
For securing data configuration was done to the spark session.
such as data encryption and filter access to data through the UI only for allowed users. 


### Requirements

* `docker >= 19.03.9`

## Steps to Run

pull the docker image from docker hub

`docker pull santiagossz/ifood:etl`

run the image 

`docker run -p 8888:8888 -d --name etl santiagossz/ifood:etl
`

execute the python script to complete the etl

`docker exec -it etl python /home/jovyan/work/main.py`

Note: depending on your machine resources, the process may take some time (as the docker image contains the data to speed up the process)
& at the beggining of the run, the API starts a spark session

api status (http://172.17.0.2:5000/)
- orders (http://172.17.0.2:5000/orders)
- top restaurants (http://172.17.0.2:5000/customer-top-restaurants)

### Testing

Open the following link localhost:8888 (http://localhost:8888/)

in the folder `data/`  you will see the spark-warehouse (data) & catalog (metadata)

Open the file `work/test/test.ipynb`  to test the successful etl process



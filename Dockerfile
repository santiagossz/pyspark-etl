## pyspark image
from jupyter/pyspark-notebook

COPY . /home/jovyan

## install packages
RUN pip install -r requirements.txt

## environment variables
ENV PYSPARK_SUBMIT_ARGS="--driver-memory 8G --executor-memory 8G pyspark-shell"

ENV SPARK_WAREHOUSE=app/spark-warehouse

## jupyter notebook configuration
CMD ["jupyter", "notebook", "--no-browser","--NotebookApp.token=''","--NotebookApp.password=''"]

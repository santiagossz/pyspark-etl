## pyspark image
from jupyter/pyspark-notebook

COPY . /home/jovyan

## install packages
RUN pip install -r requirements.txt

## environment variables
ENV PYSPARK_SUBMIT_ARGS="--driver-memory 8G --executor-memory 8G pyspark-shell"

ENV SPARK_WAREHOUSE=data/spark-warehouse

# ## mount data into container
VOLUME /work:/home/jovyan/work

## jupyter notebook configuration
CMD ["jupyter", "notebook", "--no-browser","--NotebookApp.token=''","--NotebookApp.password=''"]
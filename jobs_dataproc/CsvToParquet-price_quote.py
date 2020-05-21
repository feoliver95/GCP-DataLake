from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime


#Nome do bucket
bucketname="dots-project"



now =datetime.now()
job="price_quote"
dia=now.strftime("_%d%m%Y")
subpasta=job+"/"
pasta="carga"+"/"+job

dir_origem= "gs://"+bucketname+"/origem-stage/"
name_file= "price_quote.csv"

dir_destino_parquet = "gs://"+bucketname+"/origem-stage-parquet/"+pasta


if __name__ == "__main__":
    spark = SparkSession.builder.appName('Dotz-csv-to-Parquet').getOrCreate()
    df =spark.read.format('csv').options(header='true', inferSchema='true').load(dir_origem+name_file)
    df.write.parquet(dir_destino_parquet)
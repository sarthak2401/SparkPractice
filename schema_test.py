from datetime import datetime
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import functions as func
from google.cloud import bigquery
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast
import yaml
import copy
import argparse
from decimal import Decimal
import pyspark.sql.types as pst
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType


def load_bq_table(spark_session, table_name):
    client = bigquery.Client.from_service_account_json("itd-aia-de-mops.json")
    job_config = bigquery.QueryJobConfig()
    query_view = ("select * from " + table_name)
    query_job = client.query(query=query_view, job_config=job_config)
    query_job.result()
    bq_cache = query_job.destination.to_api_repr()['projectId'] + '.' + query_job.destination.to_api_repr()[
        'datasetId'] + '.' + query_job.destination.to_api_repr()['tableId']

    print("BQ Cache Table: " + bq_cache)

    loaded_table = spark.read.format("bigquery") \
        .option("table", bq_cache) \
        .option("credentialsFile", "itd-aia-de-mops.json") \
        .option("encoding","utf-8") \
        .load()
    #print(str(loaded_table.count()))

    return loaded_table

if __name__ == "__main__":


    program_name = 'LOAD_ASC_CASE_RESOLUTION_METRICS'
    timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"


    spark = SparkSession.builder \
        .appName("product_ltv") \
        .getOrCreate()

    sqlContext = spark

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    client = bigquery.Client()

    logger = spark.sparkContext._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


    asc_data = "`itd-aia-datalake.sap_dm.vw_product`"
    hierarchyall = load_bq_table(spark, asc_data)
    hierarchyall.count()


    #hierarchy=hierarchyall.select("product_code","Solution_Type","product_set","Product_Rollup","product_line")
    #h1=hierarchy.select("product_code")

    #book_2="`itd-aia-datalake.sales_shared.vw_bookings_product_detail_batch`"
    #bookings = load_bq_table(spark, book_2)
    
    
    #bookings.rdd.map(lambda x: x).count()
    #s1="`itd-aia-datalake.sfdc.account`"
    #s2=load_bq_table(spark,s1)

    #book_1 = bookings.repartition(4) \
    #.filter("product2_productcode not like '%NFR%'") \
    #.filter("opportunity_stagename like '10%'") \
    #.filter("opportunity_type in ('Initial Business','Expand Business')")

    #joined = book_1.join(broadcast(hierarchy), book_1.product2_productcode == hierarchy.product_code, how="left_outer")

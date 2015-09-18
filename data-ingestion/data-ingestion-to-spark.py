#!/usr/bin/env python

#Originally written by: https://github.com/ronaknnathani/gitgraph/blob/master/scripts/batch/2015_events.py
#Modified by mrubash1: 150916

# importing SparkContext and SQLContext from pyspark for batch processing
from pyspark import SparkContext
from pyspark.sql import SQLContext

# importing modules from cqlengine to write to cassandra table
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import os

#For accessing commoncrawl S3
from boto.s3.key import Key
import boto

#Testing Version
test_name = 'One_WARC_File'

# defining schema to write to cassandra table
# schema defined should exactly match the table created in cassandra
class userrepo(Model):
  username = columns.Text(primary_key=True)
  repo = columns.List(columns.Text)
  def __repr__(self):
    return '%s %d' % (self.username, self.repo)

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = '52.89.1.124'
master_public_dns = 'ec2-52-89-1-124.us-west-2.compute.amazonaws.com'

# setting SparkContext and SQLContext
sc = SparkContext("spark://" + master_ip + ":7077", test_name)
sqlContext = SQLContext(sc)
#test_spark_input = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/data2015/*.*")

# reading events data for 2015 from HDFS
conn = boto.connect_s3(anon=True, debug=2)
pds = conn.get_bucket('aws-publicdatasets')
fn = "common-crawl/crawl-data/CC-MAIN-2015-06/segments/1422121785385.35/wet/CC-MAIN-20150124174945-00099-ip-10-180-212-252.ec2.internal.warc.wet.gz"
k = Key(pds, fn)
base_file_name= 'http://s3.amazonaws.com/'
#Create a SchemaRDD from the file(s) pointed to by path
#hdfs://, s3n://, etc URI
#test_spark_input = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/data2015/*.*")
test_spark_input = sqlContext.jsonFile(base_file_name+fn)

# Matthew Rubashkin 150916
# Input to Spark modified from https://github.com/ronaknnathani/gitgraph/blob/master/scripts/batch/2015_events.py


#Enable to use all 4 nodes
#pyspark --master spark://ip-172-31-45-18:7077 --executor-memory 6400M --driver-memory 6400M

###
# tools to open WARC files from common crawl
import warc,boto, re
from gzipstream import GzipStreamFile
from boto.s3.key import Key
import re

# importing SparkContext and SQLContext from pyspark for batch processing
from pyspark import SparkContext
from pyspark.sql import SQLContext

# importing modules from cqlengine to write to cassandra table
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import os

#Connect to amazon S3 containing commoncrawl, no authenitcation necessary
conn = boto.connect_s3(anon=True, debug=2)
pds = conn.get_bucket('aws-publicdatasets')

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = '52.89.1.124'
master_public_dns = 'ec2-52-89-1-124.us-west-2.compute.amazonaws.com'

# setting SparkContext and SQLContext
test_name = 'MVP_Warc_150917_test' #Testing Version
sc = SparkContext("spark://" + master_ip + ":7077", test_name)
sqlContext = SQLContext(sc)

#Get JSON files into dataframe
#WET files for plaintext
#WAT files for links
filename_plaintext_json ='common-crawl_crawl-data_CC-MAIN-2015-06_segments_1422122086301.64_wet_CC-MAIN-20150124175446-00197-ip-10-180-212-252.ec2.internal.warc.wet.gz.json'
filename_links_json = 'common-crawl_crawl-data_CC-MAIN-2015-06_segments_1422122086301.64_wat_CC-MAIN-20150124175446-00197-ip-10-180-212-252.ec2.internal.warc.wat.gz.json'
#directory = '/home/ubuntu/Origin/data-ingestion/'
directory = "hdfs://" + master_public_dns + ":9000/Data_minimal_viable_unit_150917/"
plaintext_data_frame = sqlContext.read.json(directory+filename_plaintext_json) #NEED PATH TO FILE
links_data_frame = sqlContext.read.json(directory+filename_links_json)

#EXPLANATION OF THE PYSPARK CODE



###Create an iterable SPARK object for Links###
#row = individual json file by line
#reviewerID = these are the keys
#row.reviewerID gives output per individual JSON, by the indicated key
#reviewerSummaryRDD = df.map(lambda row: (row.KEY1,list([(row.KEY2, row.KEY3)]))).groupByKey()
plaintext_reviewerSummaryRDD = plaintext_data_frame.map(lambda row: (row.url,list([(row.plaintext)]))).groupByKey() #have to group by key, since we don't know the type the entry
plaintext_reviewerSummary = plaintext_reviewerSummaryRDD.map(lambda x: {"url":x[0], "plaintext":[tup for sublist in x[1] for tup in sublist]}).collect()

###Create an iterable SPARK object for plainText###
links_reviewerSummaryRDD = links_data_frame.map(lambda row: (row.url,list([(row.links)]))).groupByKey() #have to group by key, since we don't know the type the entry, row.plaintext
links_reviewerSummary = links_reviewerSummaryRDD.map(lambda x: {"url":x[0], "links":[tup for sublist in x[1] for tup in sublist]}).collect()
#MIGHT NOT NEED TO MAP REDUCE
#reviewerSummary = reviewerSummaryRDD.map(lambda x: {"reviewerID":x[0], "reviewDetail":[tup for sublist in x[1] for tup in sublist]}).collect()




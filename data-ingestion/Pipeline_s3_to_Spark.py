# Matthew Rubashkin 150916
# Input to Spark modified from https://github.com/ronaknnathani/gitgraph/blob/master/scripts/batch/2015_events.py

###
# tools to open WARC files from common crawl
import warc
from gzipstream import GzipStreamFile
import boto
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

#create WARC file from s3 WARC like file
def create_WARC_file_from_Common_Crawl(file_name, pds):
  k=Key(pds)
  k.key=file_name
  return warc.WARCFile(fileobj=GzipStreamFile(k))

#Using URL descriptor, get text information form WARC file
def get_text_from_WARC_file(WARC, descriptor,DATA_FILE):
  urls=''
  text_files=''
  counter=0
  total_records_counter=0
  for num, record in enumerate(WARC):
    url = record.header.get('warc-target-uri', None)
    if descriptor in str(url):
      text = record.payload.read()
      plaintext = re.sub(r'<.*?>', '', text)
      counter = counter + 1
      #print counter
      #print 'Node: ', url
      #print 'Plain Text: ', plaintext
      #print
      json_prefile= '"' + str(url) + '":"' + str(plaintext) + '",'  
    total_records_counter=total_records_counter+1
  #print 'total records: ', total_records_counter
  #print 'records of interest: ', counter
  #
  return = JSON.parse("{"+value+"}");JSON

#Connect to amazon S3 containing commoncrawl, no authenitcation necessary
conn = boto.connect_s3(anon=True, debug=2)
pds = conn.get_bucket('aws-publicdatasets')

#Insert file names WET: plain text; WAT: hyperlinks
WET_file_name = 'common-crawl/crawl-data/CC-MAIN-2015-06/segments/1422122086301.64/wet/CC-MAIN-20150124175446-00197-ip-10-180-212-252.ec2.internal.warc.wet.gz'
WAT_file_name = 'common-crawl/crawl-data/CC-MAIN-2015-06/segments/1422122086301.64/wat/CC-MAIN-20150124175446-00197-ip-10-180-212-252.ec2.internal.warc.wat.gz'
descriptor = "en.wikipedia"

#Run Segments
WARC_WET=create_WARC_file_from_Common_Crawl(WET_file_name,pds)
get_text_from_WARC_file(WARC_WET,descriptor)
WARC_WAT=create_WARC_file_from_Common_Crawl(WAT_file_name,pds)
get_text_from_WARC_file(WARC_WAT,descriptor)

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = '52.89.1.124'
master_public_dns = 'ec2-52-89-1-124.us-west-2.compute.amazonaws.com'

# setting SparkContext and SQLContext
sc = SparkContext("spark://" + master_ip + ":7077", test_name)
sqlContext = SQLContext(sc)

#Get JSON files into dataframe
#WET files for plaintext
#WAT files for links
filename_plaintext_json ='common-crawl_crawl-data_CC-MAIN-2015-06_segments_1422122086301.64_wet_CC-MAIN-20150124175446-00197-ip-10-180-212-252.ec2.internal.warc.wet.gz.json'
filename_links_json = 'common-crawl_crawl-data_CC-MAIN-2015-06_segments_1422122086301.64_wat_CC-MAIN-20150124175446-00197-ip-10-180-212-252.ec2.internal.warc.wat.gz.json'
directory = '/home/ubuntu/Origin/data-ingestion/'
plaintext_data_frame = sqlContext.read.json(directory+filename_plaintext_json) #NEED PATH TO FILE
links_data_frame = sqlContext.read.json(directory+filename_links_json)

#EXPLANATION OF THE PYSPARK CODE



###Create an iterable SPARK object for Links###
#row = individual json file by line
#reviewerID = these are the keys
#row.reviewerID gives output per individual JSON, by the indicated key
#reviewerSummaryRDD = df.map(lambda row: (row.KEY1,list([(row.KEY2, row.KEY3)]))).groupByKey()
plaintext_reviewerSummaryRDD = plaintext_data_frame.map(lambda row: (row.url,list([(row.links)]))).groupByKey() #have to group by key, since we don't know the type the entry
plaintext_reviewerSummary = plaintext_reviewerSummaryRDD.map(lambda x: {"reviewerID":x[0], "reviewDetail":[tup for sublist in x[1] for tup in sublist]}).collect()

###Create an iterable SPARK object for plainText###
links_reviewerSummaryRDD = links_data_frame.map(lambda row: (row.url,list([(row.plaintext)]))).groupByKey() #have to group by key, since we don't know the type the entry, row.plaintext
links_reviewerSummary = links_reviewerSummaryRDD.map(lambda x: {"reviewerID":x[0], "reviewDetail":[tup for sublist in x[1] for tup in sublist]}).collect()
#MIGHT NOT NEED TO MAP REDUCE
#reviewerSummary = reviewerSummaryRDD.map(lambda x: {"reviewerID":x[0], "reviewDetail":[tup for sublist in x[1] for tup in sublist]}).collect()




# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#Pipeline_S3_to_Spark_Batch-Large2
#Matthew Rubashkin

# <codecell>

from pyspark.sql import SQLContext
#import glob #find pathname with certain patterns


# <codecell>

# setting SparkContext and SQLContext
sqlContext = SQLContext(sc)
#file_location = "s3n://rubash-commoncrawl/Wikipedia_CDX_index_results_January_2015_upload_to_s3/"
file_location = "s3n://rubash-commoncrawl/Wikipedia_150922_SMALL/"

# <codecell>

#and download all files
df_wat = sqlContext.read.json(file_location + "*.wat.gz.json") 
df_wet = sqlContext.read.json(file_location + "*.wet.gz.json")
df_wat.printSchema()
df_wet.printSchema()

# <codecell>

df_wat.take(1)

# <codecell>

#Create an RDD of links and plaintext, with key:URL and value
RDD_links=df_wat.map(lambda column: (column.url,column.links))
RDD_plaintext=df_wet.map(lambda column: (column.url,column.plaintext))

# <codecell>

#FUNCTION NOT IN USE: for reformating RDDs
def RDD_reformat_KeyOneURL_ItemOneLink(RDD_links):
    #Switch the order of the Schema to: Root: url, links
    #Create an RDD where each row has one URL and one URL that is linked to it
    RDD_links_map=RDD_links.flatMapValues(lambda x:x)
    #RDD = df.map(lambda row: (row.url,list([(row.plaintext,row.links)]))).groupByKey() #have to group by key, since we don't know the type the entry
    RDD_links_map.take(3)

# <codecell>

#FUNCTION NOT IN USE:
#Join df_wat and df_wet and eliminate duplicates
def join_df_wat_and_df_wet(df_wat,df_wet):
    key_to_not_replicate='url'
    keep = [df_wat[c] for c in df_wat.columns] + [df_wet[c] for c in df_wet.columns if c != key_to_not_replicate]
    df = df_wat.join(df_wet, df_wat.url==df_wet.url, 'outer').select(*keep)
    df.show()
    return df

# <codecell>

#FUNCTION NOT IN USE:
###Create an iterable SPARK object for Links###
def RDD_of_combined_df_wet_and_wat(df):
    RDD = df.map(lambda row: (row.url,list([(row.plaintext,row.links)]))).groupByKey() #have to group by key, since we don't know the type the entry
    RDD_Summary = RDD.map(lambda x: {"url":x[0], "plaintext":[tup for sublist in x[1] for tup in sublist]}).collect()
    RDD.take(10)

# <codecell>

#This is an example implementation of PageRank. For more conventional use,
#Please refer to PageRank implementation provided by graphx
#https://github.com/apache/spark/blob/master/examples/src/main/python/pagerank.py

from __future__ import print_function
import re
import sys
from operator import add
from pyspark import SparkContext

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

# <codecell>

#Performing PageRank on RDD_links
#Print out to visually check if it is in the right format
RDD_links.take(1) #IN THE FUTURE COULD USE .partitionBy(128).cache() TO POSSIBLY SPEED UP - OR ANOTHER VALUE	

# <codecell>

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
#IN THE FUTURE THIS COULD BE CHANGED TO HAVE ALTERED WEIGHTS
ranks = RDD_links.map(lambda RDD_links: (RDD_links[0], 1.0))
ranks.take(10)

# <codecell>

RDD_links.join(ranks).take(1)

# <codecell>

# Calculates and updates URL ranks continuously using PageRank algorithm.
total_iterations=1
for iteration in range(int(total_iterations)):
    # Calculates URL contributions to the rank of other URLs.
    contribs = RDD_links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

    # Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15) #THIS IS THE PageRank
    #ranks.take(10)

#print ("THIS IS A TEST")

# <codecell>

# Collects all URL ranks and dump them to console.
#for (link, rank) in ranks.collect():
#    print("%s has rank: %s." % (link, rank))
ranks.take(5)

# <codecell>

#FUNCTION NOT IN USE:
#Join df_wat and df_wet and eliminate duplicates
def join_PageRank_and_plainText_RDDs(df_1,df_2,key_to_not_replicate):
    keep = [df_wat[c] for c in df_wat.columns] + [df_wet[c] for c in df_wet.columns if c != key_to_not_replicate]
    df = df_wat.join(df_wet, df_wat.url==df_wet.url, 'outer').select(*keep)
    df.show()
    return df

#FUNCTION NOT IN USE:
###Create an iterable SPARK object for Links###
def RDD_of_combined_df_wet_and_wat(df):
    RDD = df.map(lambda row: (row.url,list([(row.plaintext,row.links)]))).groupByKey() #have to group by key, since we don't know the type the entry
    RDD_Summary = RDD.map(lambda x: {"url":x[0], "plaintext":[tup for sublist in x[1] for tup in sublist]}).collect()
    RDD.take(10)

# <codecell>

#Write out ranks, plaintext and links to file
filename='Ranks_150923_small'
directory ='/mnt/my-data/origin/data-ingestion/'
save = True

if save == True:
    #ranks
    with open(directory+filename+'ranks'+'.txt', 'w+') as file:
        file.write('url, rank'+'\n')
        for (link, rank) in ranks.collect():
            file.write(("%s has rank: %s." % (str(link), str(rank))) +'\n')
    file.close()
    #links
    with open(directory+filename+'links'+'.txt', 'w+') as file:
        file.write('url, links'+'\n')
        for (url, links) in RDD_links.collect():
            file.write(("%s has rank: %s." % (str(link), str(rank))) +'\n')
    file.close()
    #plaintext
    with open(directory+filename+'plaintext'+'.txt', 'w+') as file:
        file.write('url, plaintext'+'\n')
        for (url, plaintext) in RDD_plaintext.collect():
            file.write(("%s has rank: %s." % (str(link), str(rank))) +'\n')
    file.close()


# <codecell>

#Ranks-Convert RDD to JSON Dataframe for importing to ElasticSearch
df_ranks=sqlContext.createDataFrame(ranks, ['url', 'rank'])
df_ranks.take(5)
srdd_rank=df_ranks.toJSON()
srdd_rank.take(5)

# <codecell>

#Plaintext-Convert RDD to JSON Dataframe for importing to ElasticSearch
df_plaintext=sqlContext.createDataFrame(RDD_plaintext, ['url', 'plaintext'])
srdd_plaintext=df_plaintext.toJSON()
srdd_plaintext.take(1)

# <codecell>

#Links-Convert RDD to JSON Dataframe for importing to ElasticSearch
df_links=sqlContext.createDataFrame(RDD_links, ['url', 'links'])
srdd_links=df_links.toJSON()
srdd_links.take(1)

# <codecell>

from elasticsearch import Elasticsearch
es = Elasticsearch(['172.31.2.97'])

# <codecell>

def write_to_elastic(index_name, doc_type_name, JSON_data_frame):
    count=0
    for JSONstrings in JSON_data_frame:
        print (JSONstrings)
        res = es.index(index=index_name, doc_type=doc_type_name, id=count, body=JSONstrings)
        print(res['created'])
        res = es.get(index=index_name, doc_type=doc_type_name, id=count)
        print(res['_source'])
        count=count+1
        print('Current ID #: ', count)
    
    es.indices.refresh(index=index_name)
    res = es.search(index=index_name, body={"query": {"match_all": {}}})
    print("Got %d Hits:" % res['hits']['total'])
    
    for hit in res['hits']['hits']:
        print("%(url) %(property)" % hit["_source"])

# <codecell>

doc = [{
    'url': 'wiki.TEST/1',
    'text': 'test1 test1again test1repeat',
    
},{
    'url': 'wiki.TEST/2',
    'text': 'test2 test1again test1repeat',
}]

index_name='test-index-6'
doc_type_name='test-type-1'
write_to_elastic(index_name, doc_type_name,doc)

# <codecell>

# Example for what we are converting
srdd_rank.take(1)

# <codecell>


srdd_rank.take(1)

# <codecell>

import json

def WriteToElasticSearch_bypartition(index, doc_type, kv_iter):
    """
    This preserves keys. Unlike elasticsearch-hadoop-2.1.0.jar .
    # print testrdd.saveAsNewAPIHadoopFile(
    #     path='-',
    #     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    #     keyClass="org.apache.hadoop.io.NullWritable",
    #     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    #     conf={ "es.resource" : "test/test3" })
    """
    from elasticsearch import Elasticsearch
    es = Elasticsearch(['172.31.2.97'])

    for kv in kv_iter:
        _id = kv[0]
        body = kv[1]

        es.index(index,doc_type, id=_id, body=body)
        print ('Working')

# <codecell>

# Write to ElasticSearch partition by partition
index_name='test-10'
doc_type='rank'
try:
    srdd_rank2=srdd_rank.map(lambda x:ast.literal_eval(x))
    srdd_rank2.foreachPartition(lambda kv_iter: WriteToElasticSearch_bypartition(index_name,doc_type,kv_iter))
    print ('it worked!')
except:
    print ("Error: Perhaps %s does not exist?")

# <codecell>

index_name='test-index-6'
es.indices.refresh(index_name)
res = es.search(index_name, body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])

# <codecell>

print ('done')

# <codecell>

import ast
test_rdd=srdd_rank.map(lambda x:x[3])
test_rdd.take(5)

# <codecell>

test_rdd_2=test_rdd.map(lambda x:x[0])
test_rdd_2.take(5)

# <codecell>




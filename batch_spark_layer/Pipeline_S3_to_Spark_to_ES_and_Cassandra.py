# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#!/usr/bin/env python
#Pipeline_S3_to_Spark_Batch-Large2
#Matthew Rubashkin

# <codecell>

from pyspark.sql import SQLContext
#import glob #find pathname with certain patterns


# <codecell>

# setting SparkContext and SQLContext
sqlContext = SQLContext(sc)
file_location = "s3n://rubash-commoncrawl/Wikipedia_CDX_index_results_January_2015_upload_to_s3/"
#file_location = "s3n://rubash-commoncrawl/Wikipedia_150922_SMALL/"

# <codecell>

#and download all files
df_wat = sqlContext.read.json(file_location + "*.wat.gz.json") 
df_wet = sqlContext.read.json(file_location + "*.wet.gz.json")
print ('Schema of Links (WAT) files:')
df_wat.printSchema()
print ('Schema of Text (WET) files:')
df_wet.printSchema()

# <codecell>

df_wat.take(1)

# <codecell>

#Create an RDD of links and plaintext, with key:URL and value
RDD_links=df_wat.map(lambda column: (column.url,column.links)).cache() #Cached to speed up the pagerank
RDD_plaintext=df_wet.map(lambda column: (column.url,column.plaintext))

# <codecell>

#FUNCTION NOT IN USE:
###Create an iterable SPARK object for Links###
def RDD_of_combined_df_wet_and_wat(df):
    RDD = df.map(lambda row: (row.url,list([(row.plaintext,row.links)]))).groupByKey() #have to group by key, since we don't know the type the entry
    RDD_Summary = RDD.map(lambda x: {"url":x[0], "plaintext":[tup for sublist in x[1] for tup in sublist]}).collect()
    RDD.take(10)

# <codecell>

#Performing PageRank on RDD_links
#Print out to visually check if it is in the right format
RDD_links.cache()
RDD_links.take(1) #IN THE FUTURE COULD USE .partitionBy(128).cache() TO POSSIBLY SPEED UP - OR ANOTHER VALUE	

# <codecell>

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
#IN THE FUTURE THIS COULD BE CHANGED TO HAVE ALTERED WEIGHTS
ranks = RDD_links.map(lambda RDD_links: (RDD_links[0], 1.0))
ranks.take(10)

# <codecell>

RDD_links.join(ranks).take(1)

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
#ranks.take(5)
print (type(ranks))
ranks.take(1)

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
save = False

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

def WriteToElasticSearch_bypartition(index_name, doc_type_name, kv_iter):
    
    print ('SECOND TEST')
        
    from elasticsearch import Elasticsearch
    es = Elasticsearch(['172.31.2.97'])
    
    #THIS MUST BE IN AN ITERABLE FUNCTION
    #if it is not an iterable
    for kv in kv_iter:
        _id=str(kv[0])
        body=kv[1]
        
        doc = {'url': str(_id),'rank': body,}
        
        es.index(index=index_name,doc_type=doc_type_name, id=_id, body=doc)

# <codecell>

def test_function(index_name, doc_type_name, rdd):
    # Write to ElasticSearch partition by partition
    #rdd.foreachPartition(lambda kv_iter: WriteToElasticSearch_bypartition(index_name,doc_type_name,kv_iter))
    try:
        rdd.foreachPartition(lambda kv_iter: WriteToElasticSearch_bypartition(index_name,doc_type_name,kv_iter,))
        print ('Data sent to Elasticsearch')
    except:
        print ('Data NOT sent to Elasticsearch')
        pass

# <codecell>

#Send the plaintext to elasticsearch
index_name='test-32_large'
doc_type_name='plaintext'
test_function(index_name,doc_type_name,RDD_plaintext)

# <codecell>

#Query Elasticsearch to confirm that data was sent
from elasticsearch import Elasticsearch
es = Elasticsearch(['172.31.2.97'])
es.indices.refresh(index_name)
res = es.search(index_name, body={"query": {"match_all": {}}})
print("Got %d Entries:" % res['hits']['total'])

#Print all - Will crash for large dataset
#for hit in res['hits']['hits']:
#    print("URL: %(url)s /n %(rank)s: " % hit["_source"])

# <codecell>

print ('done')

# <codecell>


# <codecell>

#convert RDD plaintext and links to JSON to import to cassandra
#And repartition to prevent cassandra from crashing
repartition_number=128
RDD_ranks_JSON = ranks.partitionBy(repartition_number).map(lambda x: {"url":x[0], "ranks":x[1]})
#RDD_ranks_JSON = ranks.map(lambda x: {"url":x[0], "ranks":x[1]})
RDD_ranks_JSON.take(3)

# <codecell>

#convert RDD plaintext and links to JSON to import to cassandra
RDD_links_JSON = RDD_links.partitionBy(repartition_number).map(lambda x: {"url":x[0], "links":x[1]})
#RDD_links_JSON = RDD_links.map(lambda x: {"url":x[0], "plaintext":x[1]})
RDD_links_JSON.take(1)

# <codecell>


# importing modules from cqlengine to write to cassandra table

#Cannot currently overwrite the url_ranks

def AddToCassandra_allcountsbatch_bypartition(d_iter):
    #from cassandra.cluster import Cluster
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    CASSANDRA_KEYSPACE = "test"
    # connecting to cassandra key space "watch_events" and syncing the desired table
    connection.setup(['52.88.228.98','52.11.49.170'], CASSANDRA_KEYSPACE) #only using two
    #cluster = Cluster(['52.88.228.98','52.11.49.170','52.88.193.89','52.88.5.151'])
    #session = cluster.connect(CASSANDRA_KEYSPACE)
    class url_ranks_4(Model):
        url = columns.Text(primary_key=True)
        ranks = columns.Float()#this will be stored as a double
        def __repr__(self):
            return '%s %s' % (self.url, self.ranks)
    sync_table(url_ranks_4)
    for d in d_iter:
        url_ranks_4.create(**d)

       
# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
AddToCassandra_allcountsbatch_bypartition([])
RDD_ranks_JSON.foreachPartition(AddToCassandra_allcountsbatch_bypartition)
            

# <codecell>


# <codecell>


# <codecell>

# importing modules from cqlengine to write to cassandra table
def AddToCassandra_allcountsbatch_bypartition(d_iter):
    #from cassandra.cluster import Cluster
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    CASSANDRA_KEYSPACE = "test"
    class url_links_3(Model):
        url = columns.Text(primary_key=True)
        links = columns.List(columns.Text)#this will be stored as a double
        def __repr__(self):
            return '%s %s' % (self.url, self.links)#http://docs.datastax.com/en/cql/3.1/cql/cql_using/use_list_t.html
        # connecting to cassandra key space "watch_events" and syncing the desired table
    connection.setup(['52.88.228.98','52.11.49.170'], CASSANDRA_KEYSPACE)
    #cluster = Cluster(['52.88.228.98','52.11.49.170','52.88.193.89','52.88.5.151'])
    #session = cluster.connect(CASSANDRA_KEYSPACE)
    sync_table(url_links_3)
    for d in d_iter:
        url_links_3.create(**d)
        
# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
AddToCassandra_allcountsbatch_bypartition([])
RDD_links_JSON.foreachPartition(AddToCassandra_allcountsbatch_bypartition)

# <codecell>


# <codecell>

print ('complete')



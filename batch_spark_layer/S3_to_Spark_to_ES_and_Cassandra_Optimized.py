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
#file_location = "s3n://rubash-commoncrawl/Wikipedia_CDX_index_results_January_2015_upload_to_s3/"
#file_location = "s3n://rubash-commoncrawl/Wikipedia_150922_SMALL/"
file_location = "s3n://rubash-commoncrawl/Wikipedia_CDX_index_results_January_2015_WHOLE_DATA_SET_s3_12nodeDownload/"

# <codecell>

#and download all files
df_wat = sqlContext.read.json(file_location + "*.wat.gz.json") 
df_wet = sqlContext.read.json(file_location + "*.wet.gz.json")
print ('Schema of Links (WAT) files:')
df_wat.printSchema()
print ('Schema of Text (WET) files:')
df_wet.printSchema()

# <codecell>

#Create an RDD of links and plaintext, with key:URL and value
repartition_number=240
RDD_links=df_wat.map(lambda column: (column.url,column.links)).partitionBy(repartition_number)#.persist(StorageLevel.MEMORY_AND_DISK_SER) #Persisted, but avoid crashing machine
#Make sure all edges go both way in the RDD_links
RDD_plaintext=df_wet.map(lambda column: (column.url,column.plaintext)).partitionBy(repartition_number).persist(StorageLevel.MEMORY_AND_DISK_SER)

#Make sure to run the creation of RDD_links and RDD_plaintext
#RDD_links.take(1)
RDD_links.getNumPartitions()
#RDD_plaintext.take(1)
RDD_plaintext.getNumPartitions()

#Takes ~8.5 seconds with 24 partitions

# <codecell>

def WriteToElasticSearch_bypartition(index_name, doc_type_name, kv_iter):
    
    print ('SECOND TEST')
        
    from elasticsearch import Elasticsearch
    #es = Elasticsearch(['172.31.2.97']) #Original 3 node cluster
    #es= Elasticsearch(['52.89.148.224']) #dead 8 node cluster
    es = Elasticsearch(['52.89.66.139'])
    
    #THIS MUST BE IN AN ITERABLE FUNCTION
    #if it is not an iterable
    for kv in kv_iter:
        _id=str(kv[0])
        body=kv[1]
        
        #doc = {'url': str(_id),'rank': body,} #was incorrectly assigned to rank
        doc = {'url': str(_id),'plaintext': body,}
        
        
        es.index(index=index_name,doc_type=doc_type_name, id=_id, body=doc)

def test_function(index_name, doc_type_name, rdd):
    # Write to ElasticSearch partition by partition
    #rdd.foreachPartition(lambda kv_iter: WriteToElasticSearch_bypartition(index_name,doc_type_name,kv_iter))
    try:
        rdd.foreachPartition(lambda kv_iter: WriteToElasticSearch_bypartition(index_name,doc_type_name,kv_iter,))
        print ('Data sent to Elasticsearch')
    except:
        print ('Data NOT sent to Elasticsearch')
        pass

#Send the plaintext to elasticsearch
#index_name='test_no_colon'
index_name='test_151001'
doc_type_name='plaintext'
#test_function(index_name,doc_type_name,RDD_plaintext)
#test_function(index_name,doc_type_name,RDD_plaintext_repartioned)
#took 130 seconds

#Query Elasticsearch to confirm that data was sent
from elasticsearch import Elasticsearch
es= Elasticsearch(['52.89.66.139'])
es.indices.refresh(index_name)
res = es.search(index_name, body={"query": {"match_all": {}}})
print("Got %d Entries:" % res['hits']['total'])

#Unpersist RDD
RDD_plaintext.unpersist()

# <codecell>

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
#IN THE FUTURE THIS COULD BE CHANGED TO HAVE ALTERED WEIGHTS
ranks = RDD_links.map(lambda RDD_links: (RDD_links[0], 1.0))
#ranks.take(1)
#takes 11.5 seconds

# <codecell>

RDD_links.join(ranks)
#RDD_links.take(1)

# <codecell>


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

# <codecell>

# Calculates and updates URL ranks continuously using PageRank algorithm.
total_iterations=1
for iteration in range(int(total_iterations)):
    # Calculates URL contributions to the rank of other URLs.
    contribs = RDD_links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

    # Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15) #THIS IS THE PageRank


print ("Page Rank Results:")
ranks.take(5)

#Took 7.2 seconds

# <codecell>


# <codecell>

#Get descending order of ranks in rank file
ranks_sorted=ranks.sortBy(lambda x: x[1], ascending=False)
ranks_sorted.take(5)
#sorted(ranks.take(20))

#took 1.2 seconds

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

#already re partioned above
#repartition_number=9
#RDD_plaintext_repartioned = RDD_plaintext.partitionBy(repartition_number)

# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>

print ('done')

# <codecell>

#Create JSON files for import into Cassandra
RDD_url_ranks_links=ranks.leftOuterJoin(RDD_links)
RDD_url_ranks_links.take(2)

# <codecell>


# <codecell>

repartition_number
#Create RDD with multiple columns and filtered to not have Null positions
RDD_url_ranks_links_partioned= RDD_url_ranks_links.partitionBy(repartition_number)
#Make sure that the RDD has both ranks and links
RDD_JSON_FILTERED = RDD_url_ranks_links_partioned.filter(lambda x: x[1][0] != None and x[1][1] != None)
#RDD_JSON=RDD_JSON_FILTERED.map(lambda x: {"url":x[0], "ranks":x[1][0], "links":x[1][1]}).persist(StorageLevel.MEMORY_AND_DISK_SER) 

#create Json form of RDD, and add an additional url_ID column for future sorting
#RDD_JSON=RDD_JSON_FILTERED.map(lambda x: {"url":x[0], "ranks":x[1][0], "links":x[1][1]}).persist(StorageLevel.MEMORY_AND_DISK_SER) 
#where user_ID is the total amount of links used as an ad hoc partition key to increase speed, and reduce hot to warm spots
RDD_JSON=RDD_JSON_FILTERED.map(lambda x: {"url":x[0],"ranks":x[1][0], "links":x[1][1], }).persist(StorageLevel.MEMORY_AND_DISK_SER) 
RDD_JSON.take(10)

#3.5 seconds

#Without filtering:
#RDD_JSON=RDD_url_ranks_links_partioned.map(lambda x: {"url":x[0], "ranks":x[1][0], "links":x[1][1]}).persist(StorageLevel.MEMORY_AND_DISK_SER)
#RDD_JSON_FILTERED.take(10)

# <codecell>


# <codecell>

# importing modules from cqlengine to write to cassandra table
#Cannot currently overwrite the url_ranks

def AddToCassandra_allcountsbatch_bypartition(d_iter): #filter_missing_values=True for RDDs
    #from cassandra.cluster import Cluster
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    #CASSANDRA_KEYSPACE = "wikipedia_jan_2015"
    CASSANDRA_KEYSPACE = "test"
    connection.setup(['52.89.66.139','52.89.34.7','52.89.116.45','52.89.78.4', '52.89.27.115','52.89.133.147','52.89.1.48'], CASSANDRA_KEYSPACE)
    class url_ranks_links_23(Model):
        #primary key is url which is dictated by the number of links
        url = columns.Text(primary_key=True)
        ranks = columns.Float()#this will be stored as a double # this is a primary key to sort on later
        links = columns.List(columns.Text)#this will be stored as a double
        def __repr__(self):
            return '%s %s' % (self.url, self.ranks)
    
    sync_table(url_ranks_links_23)
    for d in d_iter:
        url_ranks_links_23.create(**d)

       
# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
AddToCassandra_allcountsbatch_bypartition([])
RDD_JSON.foreachPartition(AddToCassandra_allcountsbatch_bypartition)
print ('ranks table in cassandra transfered')

#46 seconds

# <codecell>

#Calls in Cassandra cqlsh in terminal:
#CREATE TABLE url_ranks_links_15 ("user_ID" double, ranks double, url text,  links list<text>, PRIMARY KEY (("user_ID"), ranks), ) WITH CLUSTERING ORDER BY (ranks DESC);
#SELECT DISTINCT "user_ID" FROM url_ranks_links_15
#cqlsh:test> SELECT ranks FROM url_ranks_links_15 where "user_ID"=172 ORDER BY ranks DESC  ;

# <codecell>


# <codecell>


# <codecell>


# <codecell>




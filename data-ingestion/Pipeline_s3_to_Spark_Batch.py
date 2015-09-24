#Pipeline_S3_to_Spark_Batch
#150922
#mrubash1

from pyspark.sql import SQLContext
#import glob #find pathname with certain patterns

from __future__ import print_function
import re
import sys
from operator import add
from pyspark import SparkContext

#FUNCTIONS FOR PAGE RANK
def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


#create a SQL object from all WAT and WET files
sqlContext = SQLContext(sc)
file_location = "s3n://rubash-commoncrawl/Wikipedia_CDX_index_results_January_2015_upload_to_s3/"

#Download files and create dataframs
df_wat = sqlContext.read.json(file_location + "*.wat.gz.json") 
df_wet = sqlContext.read.json(file_location + "*.wet.gz.json")
df_wat.printSchema()
df_wet.printSchema()

#Create RDD
RDD_links=df_wat.map(lambda column: (column.url,column.links))
RDD_links_map=RDD_links.flatMapValues(lambda x:x)
RDD_links_map.take(3)

#Join df_wat and df_wet and eliminate duplicates
#NOT USED
def join_df_wat_and_df_wet(df_wat,df_wet):
    key_to_not_replicate='url'
    keep = [df_wat[c] for c in df_wat.columns] + [df_wet[c] for c in df_wet.columns if c != key_to_not_replicate]
    df = df_wat.join(df_wet, df_wat.url==df_wet.url, 'outer').select(*keep)
    df.show()
    return df

#Create an iterable SPARK object for Links
#NOT USED
def RDD_of_combined_df_wet_and_wat(df):
    RDD = df.map(lambda row: (row.url,list([(row.plaintext,row.links)]))).groupByKey() #have to group by key, since we don't know the type the entry
    RDD_Summary = RDD.map(lambda x: {"url":x[0], "plaintext":[tup for sublist in x[1] for tup in sublist]}).collect()
    return RDD

"""
This is an example implementation of PageRank. For more conventional use,
Please refer to PageRank implementation provided by graphx
http://www.apache.org/licenses/LICENSE-2.0
"""

# Earlier steps: Initialize the spark context.
# Earlier steps: Loads all URLs from input file and initialize their neighbors.
# Format:
# Loads in input file. It should be in format of:
#     URL         neighbor URL
#     URL         neighbor URL
#     URL         neighbor URL
#     ...
links = RDD_links_map

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks = links.map(lambda links: (links[0], 1.0))
ranks.take(10)

# Calculates and updates URL ranks continuously using PageRank algorithm.
total_iterations=1
for iteration in range(int(total_iterations)):
    # Calculates URL contributions to the rank of other URLs.
    contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

    # Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    ranks.take(10)

# Collects all URL ranks and dump them to console.
for (link, rank) in ranks.collect():
    print("%s has rank: %s." % (link, rank))

print ('END')

#result = es.search(index="movie_db", body={'query': {'match': {'description': 'CIA'}}})
from elasticsearch import Elasticsearch
import json
es = Elasticsearch(['52.88.228.98'])
#result = es.search(index="test-31")
#print json.dumps(result, indent=2)

#Query Elasticsearch to see what URLs contain this query
query_phrase = 'colossus'
index_name='test-32_large'
res = es.search(index_name, q=query_phrase)
#res = es.search(index="test-index", body={"query":{'match': {'rank': query_phrase }}})
#result = es.search(index="movie_db", body={'query': {'match': {'description': 'CIA'}}})
#print (json.dumps(res, indent=1))
print("Got %d Hits:" % res['hits']['total'])

from cqlengine import connection
from cassandra.cluster import Cluster
CASSANDRA_KEYSPACE = "test"
connection.setup(['52.88.228.98','52.11.49.170'],CASSANDRA_KEYSPACE)
cluster = Cluster(['52.88.228.98','52.11.49.170'])
session = cluster.connect(CASSANDRA_KEYSPACE)

#Formatting goal to avoid errors
#print session.execute("SELECT ranks FROM url_ranks_3 WHERE url='http://en.wikipedia.org/wiki/Shareef_Cousin';")

#define extract_queried_urls_ranks_links():
for hit in res['hits']['hits']:
    key_lookup= ("%(url)s"% hit["_source"])
    #modify the url so that cqlsh can read it, even though it has a colon
    key_lookup="'" + key_lookup + "';"
    query1 = "SELECT * FROM url_ranks_4 WHERE url="+key_lookup
    #print query1
    #consult the cassandra table with the above query
    rows=(session.execute(query1))
    #check to make sure we are not taking information from nodes that are not currently in elastic search or cassandra
    for row in rows:
        if row[1] != None: #row[0]=url, row[1]=rank
            #print row[0]
            key_lookup_for_links="'" + str(row[0]) + "';"
            #print key_lookup
            query2 = "SELECT * FROM url_links_3 WHERE url="+key_lookup_for_links
            #print query2
            rows_links=(session.execute(query2))
            #have a method to export all information here
            for row_links in rows_links:
                print row_links[0], row[1], row_links[1]
                print

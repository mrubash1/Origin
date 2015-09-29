#!/usr/bin/env python

# importing flask and cassandra modules
from cassandra.cluster import Cluster
from flask import Flask, jsonify, render_template, request
import json
from cassandra.query import SimpleStatement
import operator
from sets import Set
from flask_restful import Resource, Api

#set up connection to elasticsearch
from elasticsearch import Elasticsearch
es = Elasticsearch(['52.88.228.98']) #has to be public IP, as this t2 micro is spun up on a different AWS account

# setting up connections to cassandra
from cqlengine import connection
connection.setup(['52.88.228.98','52.11.49.170'], 'test')
cluster = Cluster(['52.88.228.98','52.11.49.170'])
#session = cluster.connect('watch_events')

app = Flask(__name__)
api = Api(app)

# homepage
@app.route("/")
@app.route("/index")
def hello():
  return render_template("index.html")

# graph query page
@app.route("/graph")
def graph():
  return render_template("graph_query.html")

# get username and return graph
@app.route("/graph", methods=['POST'])
def graph_post():
  search_term = request.form["search term"] # get username entered
  density = request.form["density"]
  #UPDATE THE NODES HERE TO INFORM THE PROCESSING SPEED OF THE ELASTICSEARCH AND CASSANDRA QUERIES
  if density=="Max (1000 nodes)":
    nodes = 1000
  elif density=="High (500 nodes)":
    nodes = 500
  elif density=="Medium (200 nodes)":
    nodes = 200
  elif density=="Low (100 nodes)":
    nodes = 100
  elif density=="Very Low (50 nodes)":
    nodes = 50

  #LAUNCH ELASTICSEARCH HYBRID
  query_phrase=search_term
  #query_phrase = 'colossus'
  index_name='test-32_large'
  def elasticsearch(query_phrase,index_name):
    #Query Elasticsearch to see what URLs contain this query
    #result = es.search(index="movie_db", body={'query': {'match': {'description': 'CIA'}}})
    from elasticsearch import Elasticsearch
    es = Elasticsearch(['52.88.228.98'])
    #Query Elasticsearch to see what URLs contain this query
    res = es.search(index_name, q=query_phrase)
    #Calculate hits out of the total amount
    Hits=res['hits']['total']
    #Find total amount by query that is a false positive, 100%, wiki search
    total_Hits = (es.search(index_name, q='wiki'))['hits']['total'] #modify later to direct call to get total amount
    #Output
    print( ("Got %d Hits:" % res['hits']['total']) + ' out of total: ' + str(total_Hits))
    return res
  
  #return the elastic search item
  res=elasticsearch(query_phrase,index_name)
  #Formatting goal to avoid errors
  #print session.execute("SELECT ranks FROM url_ranks_3 WHERE url='http://en.wikipedia.org/wiki/Shareef_Cousin';")

  #declare empty arrays for filling with information
  url_total=[]
  ranks_total=[]
  links_total=[]
  links_listedPerURL=[]
  
  #CASSANDRA HYBRID  
  def extract_queried_urls_ranks_links(res,url_total,ranks_total,links_total,links_listedPerURL):
    from cqlengine import connection
    from cassandra.cluster import Cluster
    CASSANDRA_KEYSPACE = "test"
    connection.setup(['52.88.228.98','52.11.49.170'],CASSANDRA_KEYSPACE)
    cluster = Cluster(['52.88.228.98','52.11.49.170'])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    #Loop through ElasticSearch then loop through 
    for hit in res['hits']['hits']:
        key_lookup= ("%(url)s"% hit["_source"]) #taking the url only from the cassandra!
        #modify the url so that cqlsh can read it, even though it has a colon
        key_lookup="'" + key_lookup + "';"
        #select everything from table url_ranks4 where url==url(from ES filter)
        query1 = "SELECT * FROM url_ranks_4 WHERE url="+key_lookup# DOES NOT WORK BECAUSE OF CASSANDRA SHORTCOMING + "ORDER BY ranks DESC" print query1
        #consult the cassandra table with the above query
        rows=(session.execute(query1))
        #print(type(rows))
        #check to make sure we are not taking information from nodes that are not currently in elastic search or cassandra
        for row in rows:
            if row[1] != None: #row[0]=url, row[1]=rank
                print row[1]
                
                ###ADD HERE###
                #Declare filtering step by page rank value
                #Restrict to the top 1000 to avoid crashing the EC2 micro instance
                ###ADD HERE###
                
                #print row[0]
                key_lookup_for_links="'" + str(row[0]) + "';"
                #print key_lookup
                query2 = "SELECT * FROM url_links_3 WHERE url="+key_lookup_for_links
                #print query2
                rows_links=(session.execute(query2))
                #have a method to export all information here
                for row_links in rows_links:
                    #print row_links[0], row[1], row_links[1]
                    print row_links[0], row[1], len(row_links[1])
                    url_total.append(row_links[0])
                    ranks_total.append(row[1])
                    links_total.append(row_links[1])
                    links_listedPerURL.append(row_links[1])
                    print
    #Uncertain if these need to be returned or not
    return(url_total,ranks_total,links_total,links_listedPerURL)
        

  (url_total,ranks_total,links_total,links_listedPerURL)=extract_queried_urls_ranks_links(res,url_total,ranks_total,links_total,links_listedPerURL)   

#print links_listedPerURL[1]

if __name__ == "__main__":
  app.run(host='0.0.0.0')

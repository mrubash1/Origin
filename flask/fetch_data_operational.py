#!/usr/bin/env python

#150930 Flask Run App

import json, operator, re
from cassandra.cluster import Cluster
from flask import Flask, jsonify, render_template, request
from cassandra.query import SimpleStatement
from sets import Set
from operator import itemgetter

#set up connection to elasticsearch
from elasticsearch import Elasticsearch
#es_public_url= '52.89.66.139'
#es = Elasticsearch([es_public_url]) #has to be public IP, as this t2 micro is spun up on a different AWS account
elastic_search_index_name='test_150929'
es= Elasticsearch(['52.89.66.139'])

# setting up connections to cassandra
from cqlengine import connection
cassandra_public_url_array=['52.89.66.139','52.89.34.7','52.89.116.45','52.89.78.4', '52.89.27.115','52.89.133.147','52.89.1.48']
cassandra_table='url_ranks_links_21'
CASSANDRA_KEYSPACE = "test"
connection.setup(cassandra_public_url_array,CASSANDRA_KEYSPACE)
cluster = Cluster(cassandra_public_url_array)
session = cluster.connect(CASSANDRA_KEYSPACE)

#session = cluster.connect('watch_events')

app = Flask(__name__)

print 'VERIFICATION: SCRIPT RUNNING'
# homepage
@app.route("/origin")
@app.route("/origin/index")
def hello():
  return render_template("index.html")

# graph query page
@app.route("/origin/graph")
def graph():
  return render_template("graph_query.html")

# get username and return graph
@app.route("/origin/graph", methods=['POST'])
def graph_post():
  search_term = request.form["Visualization_Density"] # get username entered
  print search_term
  density = request.form["year"]
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
   #artificially put in

  def elasticsearch(es,query_phrase,index_name):
      #Query Elasticsearch to see what URLs contain this query
      #See how large the index is
      test=es.search(index_name,q='wiki')
      size_total=test['hits']['total']
      #print size_total
      
      #make res global so it can be read in future functions
      global res

      #Query Elasticsearch to see what URLs contain this query, return all elements via size call
      #Return metadata, json format, which includes 'u_id
      res = es.search(index_name, q=query_phrase,fields=[''],size=(size_total))
      
      #Find total amount by query that is a false positive, 100%, wiki search
      total_Hits = (es.search(index_name, q='wiki'))['hits']['total'] #modify later to direct call to get total amount
      #Output
      query_html_printout = (("%d contain " % res['hits']['total']) +'"' + query_phrase +'", out of ' + str(size_total))
      print query_html_printout
      #return the res elastic search results for future processing
      return res, query_html_printout
  


  #return the elastic search item
  res, query_html_printout=elasticsearch(es,query_phrase,elastic_search_index_name)
  

  #print res

# get username and return graph
#@app.route("/origin/graph", methods=['POST']) #not sure if this needs the post statement
#def graph_produce(res):
  print ('In GRAPH_PRODUCE LOOP')
  def loop_to_get_links_from_es_query(res):
      query_url_list=[]
      for hit in res['hits']['hits']:
          query_data=json.dumps(hit)
          result = re.search('"_id": "(.*)", "_index":', query_data)
          result = result.group(1)
          #print result
          query_url_list.append(result)
      return query_url_list
      #print 'Length of query_url_list: ', len(query_url_list)

  #return the list of urls corresponding to text search in elastic search
  query_url_list=loop_to_get_links_from_es_query(res)

  #Establish the URL by order according to the query, using the query_url_list
  def extract_queried_urls_ranks_links(query_url_list, session):
      print 'Inside loop: extract_queried_urls_ranks_links'
      #Arrays to fill in
      url_total=[]
      ranks_total=[]
      links_total=[]
      links_listedPerURL=[]
      data_json=[]
      for query in query_url_list:
              #print query
          #to avoid missing data, make sure to test if the key could be found
          try:
              key_lookup="'" +  query + "';" #extra parentheses to avoid colon error
              query1 = "SELECT * FROM " + cassandra_table + " WHERE url="+key_lookup# DOES NOT WORK BECAUSE OF CASSANDRA SHORTCOMING + "ORDER BY ranks DESC" print query1
              try:
                rows=(session.execute(query1))             
                if rows != []:
                  for row in rows:
                      #Grab information from file
                      #print row[0], row[2], len(row[1]),
                      url_total.append(row[0])
                      links_total.append(len(row[1]))
                      links_listedPerURL.append(row[1][:3])
                      ranks_total.append(row[2])
                      
                      #Create JSON for html insertion, by constructing dictionary for assembly in array 
                      #data_json_sample=({"url": row[0], "rank": row[2], "total_links" : len(row[1]), "example_links" : (row[1][0]+row[1][1]+row[1][2])})
                      data_json_sample=({"url": row[0], "rank": str(row[2])[:100], "total_links" : len(row[1]), "example_links" : (row[1][0]+row[1][1]+row[1][2])})

                      #Fill the array with dicts
                      data_json.append(data_json_sample)
                      #print
              except:
                #print 'inner loop error' 
                pass
          except:
              #pass if there is an apostraphe in the url lookup
              #print query1
              #print 'outer loop error'
              pass
      #return information for creating JSON files    
      return url_total, ranks_total, links_total, links_listedPerURL, data_json

  #Run extract and keep arrays
  url_total, ranks_total, links_total, links_listedPerURL,data_json=extract_queried_urls_ranks_links(query_url_list,session)
  #print ('json extraction complete')

  #query_phrase described above in searching elasticsearch
  #jsonresponse={'query_name': query_phrase, 'nodes': [x for x in data_json]}
  #json_for_html_table=json.dumps(jsonresponse)
  #json_for_html_table=json.dumps(jsonresponse)
  #json_for_html_table2=json.dumps(data_json)
  #json_for_html_table=jsonify(jsonresponse)
  #print 'json_for_html_tables: ' (json_for_html_table)

  json_for_html_table=sorted(data_json, key=itemgetter('rank'), reverse=True)

  print json_for_html_table
  print 'About to render template...'
  #return render_template("origin_table_render.html", output=json_for_html_table)
  #return render_template("test1.html", output=json_for_html_table)
  #return render_template("origin_table_render.html", output=json_for_html_table)
  #return render_template("origin_table_render.html", output=json_for_html_table)
  return render_template("origin_table_render2.html", query_output= query_html_printout, output=json_for_html_table) #make 100 the max nodes

#print links_listedPerURL[1]
  # link to the slides


@app.route("/slides")
def slides():
  return render_template("slides.html")



if __name__ == "__main__":
  print 'running'
  app.run(host='0.0.0.0', port = 80)

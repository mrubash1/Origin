# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#Flask_Cass_EC2_Analyzer_150928
import json
import re

# <codecell>

query_phrase = 'sebastian'
index_name='test_150929'
def elasticsearch(query_phrase,index_name):
    #Query Elasticsearch to see what URLs contain this query
    from elasticsearch import Elasticsearch
    es= Elasticsearch(['52.89.66.139'])
    
    #See how large the index is
    test=es.search(index_name,q='wiki')
    size_total=test['hits']['total']
    #print size_total
    
    #Query Elasticsearch to see what URLs contain this query, return all elements via size call
    #Return metadata, json format, which includes 'u_id
    res = es.search(index_name, q=query_phrase,fields=[''],size=(size_total))
    
    #Find total amount by query that is a false positive, 100%, wiki search
    total_Hits = (es.search(index_name, q='wiki'))['hits']['total'] #modify later to direct call to get total amount
    #Output
    print( ("Got %d Hits:" % res['hits']['total']) + ' out of total: ' + str(size_total))
    return res

#return the elastic search item
res=elasticsearch(query_phrase,index_name)

# <codecell>

###LOOP TO GET LINKS FROM ES###
query_url_list=[]
for hit in res['hits']['hits']:
    query_data=json.dumps(hit)
    result = re.search('"_id": "(.*)", "_index":', query_data)
    result = result.group(1)
    #print result
    query_url_list.append(result)
print 'Length of query_url_list: ', len(query_url_list)

# <codecell>

#declare empty arrays for filling with information
#make an empty dict
data={}
cassandra_table='url_ranks_links_21'

# <codecell>

#Establish the URL by order according to the query, using the query_url_list
def extract_queried_urls_ranks_links(query_url_list,cassandra_table):
    from cqlengine import connection
    from cassandra.cluster import Cluster
    CASSANDRA_KEYSPACE = "test"
    connection.setup(['52.89.66.139','52.89.34.7','52.89.116.45','52.89.78.4', '52.89.27.115','52.89.133.147','52.89.1.48'],CASSANDRA_KEYSPACE)
    cluster = Cluster(['52.89.66.139','52.89.34.7','52.89.116.45','52.89.78.4', '52.89.27.115','52.89.133.147','52.89.1.48'])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    
    url_total=[]
    ranks_total=[]
    links_total=[]
    links_listedPerURL=[]
    data_json=[]
    
    #print 'URL, PageRank, Number of Links:'    
    for query in query_url_list:
        #to avoid missing data, make sure to test if the key could be found
        try:
            key_lookup="'" +  query + "';" #extra parentheses to avoid colon error
            query1 = "SELECT * FROM " + cassandra_table + " WHERE url="+key_lookup# DOES NOT WORK BECAUSE OF CASSANDRA SHORTCOMING + "ORDER BY ranks DESC" print query1
            #print query1
            rows=(session.execute(query1))
            
            ###ADD HERE###
            #Declare filtering step by page rank value
            #Restrict to the top 1000 to avoid crashing the EC2 micro instance
            ###ADD HERE###

            for row in rows:
                #Grab information from file
                #print row[0], row[2], len(row[1]),
                url_total.append(row[0])
                links_total.append(len(row[1]))
                links_listedPerURL.append(row[1][:3])
                ranks_total.append(row[2])
                
                #Create JSON for html insertion, by constructing dictionary for assembly in array 
                data_json_sample=({"url": row[0], "rank": row[2], "total link" : len(row[1]), "example_links" : (row[1][0]+row[1][1]+row[1][2])})
                #data_json_sample=dict([ ("url", row[0]), ("rank", row[2]), ("total link" , len(row[1]) ), ("example_links" , row[1][0])])
                #print data_json_sample
                data_json.append(data_json_sample)
                #print
        except:
            #pass if there is an apostraphe in the url lookup
            #print query1
            pass
    #return information for creating JSON files    
    return url_total, ranks_total, links_total, links_listedPerURL, data_json

#Run extract and keep arrays
url_total, ranks_total, links_total, links_listedPerURL,data_json=extract_queried_urls_ranks_links(query_url_list, cassandra_table)
print ('json extraction complete')

# <codecell>

#query_phrase described above in searching elasticsearch
#jsonresponse = {"username": username, "following": [x for x in repojson]}
jsonresponse={'query_name': query_phrase, 'nodes': [x for x in data_json]}
#json_for_html_table=json.dumps(jsonresponse)
#print (type(data_json))
json_for_html_table=json.dumps(data_json)
#print (json_for_html_table)





# <codecell>


# <codecell>

data_json[6]

# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>


# <codecell>





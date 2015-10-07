# [Origin] (http://mrubash1.com/origin)

## Index
1. [Introduction] (README.md#1-introduction)
2. [AWS Clusters] (README.md#2-aws-clusters)
3. [Data Pipeline] (README.md#3-data-pipeline)
4. [Database] (README.md#4-database)
5. [Front End] (README.md#5-front-end)
6. [Presentation] (README.md#6-presentation)

![Example graph query] (flask/static/img/origin.png)

## 1. Introduction
[Origin](http://mrubash1.com/origin) is an analysis platform for querying and analyzing large networks on the world wide web. It is a big data pipeline that enables data scientists, network analysts or any web user to discover new communities. Origin uses [Commoncrawl] (http://commoncrawl.org/) as a primary data source to serve the application, [CCBlast] (https://github.com/mrubash1/CCblast) and [cdx-index-client] (https://github.com/ikreymer/cdx-index-client) for data production, Spark for batch processing of network interactions, a hybrid Cassandra/ElasticSearch database and Flask/D3 as the web serving layer.

![Example graph query] (flask/static/img/graph.png)

[Origin] (http://mrubash1.com/origin/graph) allows users to search networks on the web from Wikipedia Articles to Etsy Merchants or PubMed Contributors. The network above consists of wikipedia articles that contain the phrase 'data', the Nodes, and the hyperlink connections between those nodes, the Edges. It is a visualization of the 100 most influential articles (by the PageRank algorithim) that mention 'data'.

### Data Sources
* [Commoncrawl] (http://commoncrawl.org/): 
The Common Crawl Foundation corpus contains petabytes of data trawled from the world wide web over the last 7 years. It is a 501(c)(3) public project founded by Gil Elbaz, with tools provided by Ilya Kreymer, that contains raw web page data, extracted metadata and text extractions, with ~160 new TB every month. I generously thank The Common Crawl Foundation for providing the raw data for the Origin Platform!

## 2. AWS Clusters 
![Clusters] (/flask/static/img/clusters.png)
[Origin] (http://mrubash1.com/origin/graph) is powered by four clusters on AWS
* 4 m3.xlarge producers collecting data from CommonCrawl, and pushing processed JSON files to S3 via CCBlast
* 8 m3.large for [Spark] (https://spark.apache.org/) batch processing data, using distrubted computing to calculate [PageRank](https://en.wikipedia.org/wiki/PageRank) 
* 8 m3.large for [Cassndra](http://cassandra.apache.org/), and [Elasticsearch] (https://www.elastic.co/) hybrid database
* 1 t2.micro for [Flask] (http://flask.pocoo.org/) and [D3] (http://d3js.org/) web serving, visualization and analytics
* As of October, 2015, this system costs ~$300 a month if AWS spot instances are used


## 3. Data Pipeline

![Pipeline] (/flask/static/img/pipeline.png)

 * ### Data Production
  * The [Commoncrawl] (http://commoncrawl.org/)raw data of ~1.8 billion web pages is stored on [S3](https://aws.amazon.com/s3/).The location of specific urls (i.e. *.wikipedia.org) are accessible via a queryable API, the [cdx-index-clinet] (https://github.com/ikreymer/pywb/wiki/CDX-Server-API#api-reference). Using the cdx-index-client, the location of the three file types per url, WARC, WET and WAT can be found.
    *WARC files which store the raw crawl data
    *WAT files which store computed metadata for the data stored in the WARC, including plaintext
    *WET files which store extracted plaintext from the data stored in the WARC 

  * In order to download, parse and clean the commoncrawl data, the producing cluster of four m4 x xlarge run [CCBlast] (https://github.com/mrubash1/CCblast). CCBlast takes the S3 file location information from the [cdx-index-clinet] (https://github.com/ikreymer/pywb/wiki/CDX-Server-API#api-reference), downloads the relevant WAT and WET files to the local machines, parses the data to remove unnecessary metadata, cleans the data for problematic strings (i.e. 'true' instead of 'True'), then uploads the information to a personal S3 bucket. This personal S3 bucket and associated data will serve as the 'source of truth' for the Origin Platform.

  * [WARC file type](/flask/static/img/warc.png) 

 * ### Batch Processing
  *Data from S3 is pulled into Spark using [pyspark(https://spark.apache.org/docs/0.9.0/python-programming-guide.html). (INSERT) and [Spark SQL](https://spark.apache.org/docs/1.3.1/api/python/pyspark.sql.html). The data is then filtered into the following schema in dataframe format then transformed into memory cached RDDs for the [PageRank](https://en.wikipedia.org/wiki/PageRank) calculation. Below is an example of the Spark schema:
  
  ![Schema] (/flask/static/img/schema.png)

  *Pagerank was implemented using a previously developed [pyspark pagerank algorithim](https://github.com/apache/spark/blob/master/examples/src/main/python/pagerank.py) for RDDs. 1st order neighbor weight of 0.85 was used.   

## 4. Database
  *The plaintext information (corresponding to each URL key) is then downloaded to Elasticsearch in the database cluster. Properties including pagerank and links (corresponding to each URL key) are then downloaded to Cassandra. In Cassandra, the URL is the partition key, and the PageRank in descending order is the clustering key. Download functions to both Elasticsearch and Cassandra were custom written, as this functionality is still lacking from pyspark. 
  
  ![Database Schema] (/flask/static/img/database_schema.png)
  
  *The hybrid elasticsearch, cassandra dataframe was implemented to reduce latency for Origin web queries. Elasticsearch [Lucene] (https://lucene.apache.org/core/) serves as an inverted index for text queries, where individual words serve as keys and urls as the value pair; whereas Cassandra provides very fast lookup of key-value pair information including links and page rank.

## 5. Front end
  *Flask is used for the web app and D3 to visualize results. The python [cqlengine](https://github.com/cqlengine/cqlengine) is used to connnect to Cassandra and the [python elasticsearch client](https://elasticsearch-py.readthedocs.org/en/master/) is used to connect and download from the elastic text-indexed database.

  *For an example web query from the Flask t2micro instance:'insight' over '1000 max nodes', the query word is first sent to elasticsearch which quickly (sub 1 second for 8e6 Wikipedia articles) returns all matching url keys to Cassandra, which then returns the links and pagerank property information. The urls are then sorted for page rank, and the top 100 (i.e. 1000 max nodes / 10) are returned as the seed nodes (in blue) for the D3 visualization, ![Origin] (http://mrubash1.com/origin). If a seed node url has a hyperlink to another seed node, an edge is created. For every seed node, 10 edges are allocated. Therefore if a seed node is connected to 3 other seed nodes, 7 secondary nodes to the original seed node are instantiated. These secondary nodes are pulled in order of hyperlink appearance on the seed node website. 

  ![Force directed graph] (/flask/static/img/graph_example.png)

  *The D3 graph visualization is based upon [force directed graphs] (http://bl.ocks.org/mbostock/4062045) developed by Mike Bostock. In this graph, nodes experience a repulsive force from another, while simultaneously tehtered by an elastic linker (the edge) to nodes which they are connected via hyperlink. The result of the attractive and repulsive forces is to cluster communities of nodes that share many edges, and seperate nodes that share few edges and have many secondary nodes.



## 6. Presentation
Presentation for [Origin] (http://mrubash1.com/origin) can be found [here](http://mrubash1.com/origin/slides)



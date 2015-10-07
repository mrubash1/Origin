# [Origin] (http://mrubash1.com/origin)

## Index
1. [Introduction] (README.md#1-introduction)
2. [AWS Clusters] (README.md#2-aws-clusters)
3. [Data Pipeline] (README.md#3-data-pipeline)
4. [API] (README.md#4-api)
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

 * ### Data Collection and Ingestion 
  * The [Commoncrawl] (http://commoncrawl.org/)raw data of ~1.8 billion web pages is stored on [S3](https://aws.amazon.com/s3/).The location of specific urls (i.e. *.wikipedia.org) are accessible via a queryable API, the [cdx-index-clinet] (https://github.com/ikreymer/pywb/wiki/CDX-Server-API#api-reference). Using the cdx-index-client, the location of the three file types per url, WARC, WET and WAT can be found.
    *WARC files which store the raw crawl data
    *WAT files which store computed metadata for the data stored in the WARC, including plaintext
    *WET files which store extracted plaintext from the data stored in the WARC 

  * In order to download, parse and clean the commoncrawl data, the producing cluster of four m4 x xlarge run [CCBlast] (https://github.com/mrubash1/CCblast). CCBlast takes the S3 file location information from the [cdx-index-clinet] (https://github.com/ikreymer/pywb/wiki/CDX-Server-API#api-reference), downloads the relevant WAT and WET files to the local machines, parses the data to remove unnecessary metadata, cleans the data for problematic strings (i.e. 'true' instead of 'True'), then uploads the information to a personal S3 bucket. This personal S3 bucket and associated data will serve as the 'source of truth' for the Origin Platform.

  * [WARC file type](/flask/static/img/WARC.png) 

 * ### Batch Processing
  [Spark SQL](https://spark.apache.org/docs/1.3.1/api/python/pyspark.sql.html) is used here for all the batch processing. For the data from [GitHub Archive] (https://www.githubarchive.org/), events like 'WatchEvent', 'ForkEvent', 'CommitCommentEvent' ae filtered as they are representative of the fact that a user has either contributed to a repository or is following one. 

  The data from [GitHub archive](https://www.githubarchive.org/) has inconsistent schema. It is filtered in the batch process to a two column schema with a user and a list of all the repositories that he/she has contributed to or is following. Below is an example of schema from 2015 and 2012 and the resulting two column schema.
  
  ![Pipeline] (/flask/static/img/pipeline.png)

  The two column schema form the batch process is used to create a table in [Cassndra](http://cassandra.apache.org/)
  ![Pipeline] (/flask/static/img/userrepo.png)
  
  [GitHub's API](https://developer.github.com/v3/) returns many fields in its json response for usernames and IDs which are not really important for the application. So, 12GB of raw data for 12M+ users is reduced to 450MB by extrcting login names and IDs in the batch process from the raw responses and is further compressed while consumed by [Camus](https://github.com/linkedin/camus) to 122MB.
  
  ![Pipeline] (/flask/static/img/users.png)

  These usernames are used to pull records of who these users are following to create the [Cassndra](http://cassandra.apache.org/) table below.
  
  ![Pipeline] (/flask/static/img/userfollowing.png)

  Weekly trends are learned from 'WatchEvents' in [GitHub archive](https://www.githubarchive.org/) and a table with repo names and corresponding watch counts is created.



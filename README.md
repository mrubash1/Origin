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
[Origin](http://mrubash1.com/origin) is an analysis platform for querying and analyzing large networks on the world wide web. It is a big data pipeline that enables data scientists, network analysts or any web user to discover new communities. Origin uses [Commoncrawl] (http://commoncrawl.org/) as a primary data source to serve the application, Spark for batch processing of network interactions, a hybrid Cassandra/ElasticSearch database and Flask/D3 as the web serving layer.

![Example graph query] (flask/static/img/graph.png)

[Origin] (http://mrubash1.com/origin/graph) allows users to search networks on the web from Wikipedia Articles to Etsy Users or PubMed Contributors. The network above consists of wikipedia articles that contain the phrase 'data', the Nodes, and the hyperlink connections between those nodes, the Edges. It is a visualization of the 100 most influential articles (by the PageRank algorithim) that mention 'data'.

### Data Sources
* [Commoncrawl] (http://commoncrawl.org/): 
The Common Crawl Foundation corpus contains petabytes of data trawled from the world wide web over the last 7 years. It is a 501(c)(3) public project founded by Gil Elbaz that contains raw web page data, extracted metadata and text extractions, with ~160 new TB every month. I generously thank The Common Crawl Foundation for providing the raw data for the Origin Platform!



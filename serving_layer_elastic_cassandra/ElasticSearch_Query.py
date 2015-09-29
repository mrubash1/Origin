#ElasticSearch_Query.py
from elasticsearch import Elasticsearch
import json

es = Elasticsearch()
#result = es.search(index="movie_db", body={'query': {'match': {'description': 'CIA'}}})
result = es.search(index="test-31")
print json.dumps(result, indent=2)

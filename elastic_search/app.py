import requests
from datetime import datetime
from elasticsearch import Elasticsearch

#Initialize Elastic Search
es = Elasticsearch()

    # Check connection
res = requests.get('http://localhost:9200')
print(res.content)


    # Add/Get Data
doc = {
    'author': 'kimchy',
    'text': 'Elasticsearch: cool. bonsai cool.',
    'timestamp': datetime.now(),
}
res = es.index(index="test-index", id=1, document=doc)
print(res['result'])
res = es.get(index="test-index", id=1)
print(res['_source'])
res = es.search(index="test-index", body={"query": {"match": {'key':'value'}}})
print(res['_source'])
#Update
es.update(index='test-index',doc_type='test-index',id=1,body={"doc": {"author": "Me", "text": "Hello world" }})
# Delete
es.delete(index='test-index',doc_type='test-index',id=id)
# Delete by query
query = {"query": {"terms": {"_id": id}}}
res = es.delete_by_query(index='test-index', body=query)
print(res)



# Refresh indices
es.indices.refresh(index="test-index")

res = es.search(index="test-index", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total']['value'])
for hit in res['hits']['hits']:
    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])



    # Example 2
import json
r = requests.get('http://localhost:9200')
i = 1
while r.status_code == 200:
    r = requests.get('https://swapi.dev/api/people/'+ str(i))
    es.index(index='sw', id=i, body=json.loads(r.content))
    i=i+1
print(i)

    # Get Data 
res = es.get(index='sw', id=5)
print(res['_source'])

    # Search Data
res = es.search(index="sw", body={"query": {"match": {'name':'Darth Vader'}}})
print(res['_source'])
res = es.search(index="sw", body={"query": {"prefix" : { "name" : "lu" }}})
print(res['_source'])
res = es.search(index="sw", body={"query":
{"fuzzy_like_this_field" : { "name" :
{"like_text": "jaba", "max_query_terms":5}}}})
print(res['_source'])

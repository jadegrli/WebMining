# code to search in elasticsearch
from elasticsearch import Elasticsearch


es = Elasticsearch()
es_index = "wikipedia"


def search(query):
    query = {
        "multi_match": {
            "query": query,
            "fields": ["name^2", "headline^1.5", "first_paragraph"],
        }
    }

    return es.search(index=es_index, query=query)


response = search("Britney Spears")
# print the results
for hit in response["hits"]["hits"]:
    print(hit["_source"]["name"])
    print(hit["_source"]["url"])
    print(hit["_score"])
    print("")

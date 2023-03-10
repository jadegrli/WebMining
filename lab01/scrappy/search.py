import scrapy
from elasticsearch import Elasticsearch
import json
from urllib.parse import urljoin


def search_es(query):
    es = Elasticsearch()
    es_index = "wikipedia"

    # write query and boost the name field, the headline field and the first paragraph field
    body = {
            "multi_match": {
                "query": query,
                "fields": ["name^2", "headline^1.5", "first_paragraph"]
            }
    }

    # execute the query
    res = es.search(index=es_index, query=body)

    # print the results
    for hit in res["hits"]["hits"]:
        print(hit["_source"]["name"])
        print(hit["_source"]["url"])
        print(hit["_score"])
        print("")


search_es("Britney Spears")




    


    



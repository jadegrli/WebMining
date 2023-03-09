import scrapy
from elasticsearch import Elasticsearch
import json
from urllib.parse import urljoin


class WikiSpider(scrapy.Spider):
    name = "wiki"
    allowed_domain = "fr.wikipedia.org/wiki/"
    start_urls = ["https://fr.wikipedia.org/wiki/Britney_Spears"]

    # Define Elasticsearch instance
    es = Elasticsearch()
    es_index = "wikipedia"

    # in the constructor, we delete the index if it exists
    def __init__(self, *args, **kwargs):
        super(WikiSpider, self).__init__(*args, **kwargs)
        if self.es.indices.exists(index=self.es_index):
            self.es.indices.delete(index=self.es_index)

    def parse(self, response):
        # Check if current URL is a subpage of fr.wikipedia.org
        if self.allowed_domain in response.url:
            schema_org = response.xpath(
                '//script[@type="application/ld+json"]/text()'
            ).get()
            json_data = json.loads(schema_org)

            # save the url and name in the json data in a separate dictionary
            final_dict = {
                "url": json_data["url"],
                "name": json_data["name"],
            }

            self.es.index(index=self.es_index, body=final_dict)

        # Find all links to subpages of fr.wikipedia.org
        for href in response.css("a::attr(href)").getall():
            absolute_url = urljoin(response.url, href)
            if self.allowed_domain in absolute_url:
                yield response.follow(href, self.parse)

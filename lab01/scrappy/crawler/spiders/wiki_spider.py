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

            # count number of external links in the page
            external_links = 0
            for href in response.css("a::attr(href)").getall():
                absolute_url = urljoin(response.url, href)
                if self.allowed_domain in absolute_url:
                    external_links += 1
                    yield response.follow(href, self.parse)

            headline = ""
            if "headline" in json_data:
                headline = json_data["headline"]

            # parse the first paragraph of the page and remove the bold tags
            p_tags = response.css("div.mw-parser-output > p")
            first_paragraph_text = ""
            if len(p_tags)!=0 :
                for p_tag in p_tags:
                    if p_tag.attrib.get("class") and "mw-empty-elt" in p_tag.attrib.get("class"):
                        continue
                    else:
                        p_text = "".join(p_tag.css("::text").getall())
                        first_paragraph_text += p_text
                        break

            # save the url and name in the json data in a separate dictionary and the external links count as a int
            final_dict = {
                "url": json_data["url"],
                "name": json_data["name"],
                "external_links" : external_links,
                "headline": headline,
                "first_paragraph_text": first_paragraph_text,
            }

            self.es.index(index=self.es_index, body=final_dict)
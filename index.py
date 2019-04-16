import re
import json
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Index, Document, Text, Keyword, Integer
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.analysis import tokenizer, analyzer
from elasticsearch_dsl.query import MultiMatch, Match

# Connect to local host server
connections.create_connection(hosts=['127.0.0.1'])

# Create elasticsearch object
es = Elasticsearch()

# Define analyzers appropriate for your data.
# You can create a custom analyzer by choosing among elasticsearch options
# or writing your own functions.
# Elasticsearch also has default analyzers that might be appropriate.
my_analyzer = analyzer('custom', tokenizer='standard', filter=['lowercase', 'stop'])
title_analyzer = analyzer('custom', tokenizer='whitespace', filter=['lowercase'])
text_analyzer = analyzer('custom', tokenizer='standard', filter=['lowercase', 'stop'])
runtime_analyzer = analyzer('simple', ignore_malformed=True)


def test_analyzer(text=r"The 2 QUICK Brown-Foxes jumped over the lazy dog's bone.", analyzer=text_analyzer):
    """
    you might want to test your analyzer after you define it
    :param text: a string
    :param analyzer: the analyzer you defined
    :return: list of tokens processed by analyzer
    """
    output = analyzer.simulate(text)
    return [t.token for t in output.tokens]


# --- Add more analyzers here ---
# use stopwords... or not?
# use stemming... or not?

# Define document mapping (schema) by defining a class as a subclass of Document.
# This defines fields and their properties (type and analysis applied).
# You can use existing es analyzers or use ones you define yourself as above.
class Movie(Document):
    title = Text(analyzer=title_analyzer)
    director = Text(analyzer=title_analyzer)
    starring = Text(analyzer=title_analyzer)
    runtime = Integer(analyzer=runtime_analyzer)
    country = Text(analyzer=title_analyzer)
    language = Text(analyzer=title_analyzer)
    time = Text(analyzer=title_analyzer)
    location = Text(analyzer=title_analyzer)
    text = Text(analyzer=text_analyzer)
    categories = Text(analyzer=title_analyzer)

    # --- Add more fields here ---
    # What data type for your field? List?
    # Which analyzer makes sense for each field?

    # override the Document save method to include subclass field definitions
    def save(self, *args, **kwargs):
        return super(Movie, self).save(*args, **kwargs)


def create_index(es_object, index_name='sample_film_index'):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 1,
            "index.mapping.ignore_malformed": True,
            "analysis": {
                "analyzer": {
                    "title_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": ["lowercase"]
                    },
                    "text_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "english_stop"]
                    },
                },
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    }
                }
            }
        },
        "mappings": {
            "doc": {
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "title_analyzer",
                    },
                    "starring": {
                        "type": "text",
                        "analyzer": "title_analyzer",
                    },
                    "director": {
                        "type": "text",
                        "analyzer": "title_analyzer",
                    },
                    "country": {
                        "type": "text",
                        "analyzer": "title_analyzer",
                    },
                    "language": {
                        "type": "text",
                        "analyzer": "title_analyzer",
                    },
                    "location": {
                        "type": "text",
                        "analyzer": "title_analyzer",
                    },
                    "categories": {
                        "type": "text",
                        "analyzer": "title_analyzer",
                    },
                    "runtime": {
                        "type": "long",
                        "analyzer": "simple",
                        "ignore_malformed": True,
                    },
                    "time": {
                        "type": "long",
                        "analyzer": "simple",
                        "ignore_malformed": True
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "text_analyzer",
                    },
                }
            }
        }
    }
    try:
        if es_object.indices.exists(index_name):
            es_object.indices.delete(index=index_name)
        # Ignore 400 means to ignore "Index Already Exist" error.
        es_object.indices.create(index=index_name, ignore=400, body=settings)
        print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name='sample_film_index', record='films_corpus.json'):
    try:
        # Open the json film corpus
        with open(record, 'r', encoding='utf-8') as data_file:
            # load movies from json file into dictionary
            movies = json.load(data_file)
            size = len(movies)

        # Action series for bulk loading with helpers.bulk function.
        # Implemented as a generator, to return one movie with each call.
        # Note that we include the index name here.
        # The Document type is always 'doc'.
        # Every item to be indexed must have a unique key.
        def actions():
            # mid is movie id (used as key into movies dictionary)
            for mid in range(1, size + 1):
                yield {
                    "_index": index_name,
                    "_type": 'doc',
                    "_id": mid,
                    "title": movies[str(mid)]['Title'],
                    "director": movies[str(mid)]['Director'],
                    "starring": movies[str(mid)]['Starring'],
                    "runtime": parse_runtime(movies[str(mid)]['Running Time']),
                    "language": movies[str(mid)]['Language'],
                    "time": movies[str(mid)]['Time'],
                    "location": movies[str(mid)]['Location'],
                    "text": movies[str(mid)]['Text'],
                    "categories": movies[str(mid)]['Categories'],
                }

        helpers.bulk(elastic_object, actions())
        # outcome = elastic_object.index(index=index_name, doc_type='movie', body=record)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))


def parse_runtime(time):
    if type(time) == int:
        return time
    else:
        return None


# Populate the index
def buildIndex():
    """
    buildIndex creates a new film index, deleting any existing index of
    the same name.
    It loads a json file containing the movie corpus and does bulk loading
    using a generator function.
    """
    film_index = Index('sample_film_index')
    # film_index.settings(index={'mapping': {'ignore_malformed': True}})
    if film_index.exists():
        film_index.delete()  # Overwrite any previous version
    film_index.create()

    # Open the json film corpus
    with open('films_corpus.json', 'r', encoding='utf-8') as data_file:
        # load movies from json file into dictionary
        movies = json.load(data_file)
        size = len(movies)

    # Action series for bulk loading with helpers.bulk function.
    # Implemented as a generator, to return one movie with each call.
    # Note that we include the index name here.
    # The Document type is always 'doc'.
    # Every item to be indexed must have a unique key.
    def actions():
        # mid is movie id (used as key into movies dictionary)
        for mid in range(1, size + 1):
            yield {
                "_index": "sample_film_index",
                "_type": 'doc',
                "_id": mid,
                "title": movies[str(mid)]['Title'],
                "runtime": parse_runtime(movies[str(mid)]['Running Time']),
                "language": movies[str(mid)]['Language'],
                "director": movies[str(mid)]['Director'],
                "starring": movies[str(mid)]['Starring'],
                "location": movies[str(mid)]['Location'],
                "time": movies[str(mid)]['Time'],
                "text": movies[str(mid)]['Text'],
                "categories": movies[str(mid)]['Categories'],
            }

    helpers.bulk(es, actions())


# command line invocation builds index and prints the running time.
def main():
    start_time = time.time()
    buildIndex()
    # create_index(es)
    # store_record(es)

    print("=== Built index in %s seconds ===" % (time.time() - start_time))


if __name__ == '__main__':
    main()
    # print(test_analyzer())

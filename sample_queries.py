"""
Search API Documentation at https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html
"""

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

client = Elasticsearch()


# Match all documents
def match_all():
    s = Search(using=client, index="sample_film_index")
    s = s.query('match_all')
    response = s.execute()
    print("Num hits:", len(response.to_dict()['hits']['hits']))  # default 10 limit


# Search in title
def free_search_in_title(word):
    s = Search(using=client, index="sample_film_index")
    # Q is a shortcut for constructing a query object
    q = Q('match', title=word)
    # At some point, q has to be added to the search object.
    s = s.query(q)
    s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')  # for html
    s = s.highlight('title', word, fragment_size=999999999, number_of_fragments=1)
    response = s.execute()
    print("Num hits for", word, len(response.to_dict()['hits']['hits']))
    for hit in response:
        print(hit.meta.score)  # doc score
        print(hit.meta.highlight)  # highlighted snippet


# Match exact phrase in text
def match_phrase_in_text(phrase):
    s = Search(using=client, index="sample_film_index")
    q = Q('match_phrase', text=phrase)
    s = s.query(q)
    s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')  # for html
    s = s.highlight('text', fragment_size=999999999, number_of_fragments=1)
    response = s.execute()
    print("Num hits for", phrase, len(response.to_dict()['hits']['hits']))
    for hit in response:
        print(hit.meta.score)  # doc score
        print(hit.meta.highlight)  # highlighted snippet


def test_analyzer(text, analyzer):
    """
    you might want to test your analyzer after you define it
    :param text: a string
    :param analyzer: the analyzer you defined
    :return: list of tokens processed by analyzer
    """
    output = analyzer.simulate(text)
    return [t.token for t in output.tokens]


match_all()
free_search_in_title('cats')
# Compare to:
free_search_in_title('Cats')  # the 'simple' analyzer does lowercasing
free_search_in_title('cat')  # but not stemming
match_phrase_in_text('she knows')

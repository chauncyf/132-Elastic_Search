"""
This module implements a (partial, sample) query interface for elasticsearch movie search. 
You will need to rewrite and expand sections to support the types of queries over the fields in your UI.

Documentation for elasticsearch query DSL:
https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html

For python version of DSL:
https://elasticsearch-dsl.readthedocs.io/en/latest/

Search DSL:
https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html
"""

import re
from flask import *
from index import Movie
from pprint import pprint
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
from elasticsearch_dsl.utils import AttrList

app = Flask(__name__)

# Initialize global variables for rendering page
tmp_text = ""
tmp_title = ""
tmp_star = ""
tmp_min = ""
tmp_max = ""
gresults = {}


# display query page
@app.route("/")
def search():
    return render_template('page_query.html')


# display results page for first set of results and "next" sets.
@app.route("/results", defaults={'page': 1}, methods=['GET', 'POST'])
@app.route("/results/<page>", methods=['GET', 'POST'])
def results(page):
    global tmp_title
    global tmp_text
    global tmp_star
    global tmp_director
    global tmp_language
    global tmp_location
    global tmp_time
    global tmp_categories
    global tmp_min
    global tmp_max
    global gresults

    # convert the <page> parameter in url to integer.
    if type(page) is not int:
        page = int(page.encode('utf-8'))
        # if the method of request is post (for initial query), store query in local global variables
    # if the method of request is get (for "next" results), extract query contents from client's global variables  
    if request.method == 'POST':
        text_query = request.form['query']
        star_query = request.form['starring']
        director_query = request.form['director']
        language_query = request.form['language']
        location_query = request.form['location']
        time_query = request.form['time']
        categories_query = request.form['categories']
        mintime_query = request.form['mintime']
        if len(mintime_query) is 0:
            mintime = 0
        else:
            mintime = int(mintime_query)
        maxtime_query = request.form['maxtime']
        if len(maxtime_query) is 0:
            maxtime = 99999
        else:
            maxtime = int(maxtime_query)

        # update global variable template data
        tmp_text = text_query
        tmp_star = star_query
        tmp_director = director_query
        tmp_language = language_query
        tmp_location = location_query
        tmp_time = time_query
        tmp_categories = categories_query
        tmp_min = mintime
        tmp_max = maxtime
    else:
        # use the current values stored in global variables.
        text_query = tmp_text
        star_query = tmp_star
        director_query = tmp_director
        language_query = tmp_language
        location_query = tmp_location
        time_query = tmp_time
        categories_query = tmp_categories

        mintime = tmp_min
        if tmp_min > 0:
            mintime_query = tmp_min
        else:
            mintime_query = ""
        maxtime = tmp_max
        if tmp_max < 99999:
            maxtime_query = tmp_max
        else:
            maxtime_query = ""

    # store query values to display in search boxes in UI
    shows = {}
    shows['text'] = text_query
    shows['star'] = star_query
    shows['director'] = director_query
    shows['language'] = language_query
    shows['location'] = location_query
    shows['time'] = time_query
    shows['categories'] = categories_query
    shows['maxtime'] = maxtime_query
    shows['mintime'] = mintime_query

    # Create a search object to query our index 
    search = Search(index='sample_film_index')

    # Build up your elasticsearch query in piecemeal fashion based on the user's parameters passed in.
    # The search API is "chainable".
    # Each call to search.query method adds criteria to our growing elasticsearch query.
    # You will change this section based on how you want to process the query data input into your interface.

    # phrase = re.findall(r'"(.*?)"', text_query)
    # if len(phrase) != 0:
    #     # s = s.query(Q('match_phrase', text=phrase))
    #     s = search.query('match_phrase', query=phrase[0])
    # else:
    # search for runtime using a range query
    s = search.query('range', runtime={'gte': mintime, 'lte': maxtime})
    # Conjunctive search over multiple fields (title and text) using the text_query passed in
    if len(text_query) > 0:
        s = s.query('multi_match', query=text_query, type='cross_fields', fields=['title^3', 'text'], operator='and')

        response = s.execute()
        if len(response) == 0:
            s = search.query('range', runtime={'gte': mintime, 'lte': maxtime})
            s = s.query('multi_match', query=text_query, type='cross_fields', fields=['title^3', 'text'],
                        operator='or')

        phrase = re.findall(r'"(.*?)"', text_query)
        if len(phrase) != 0:
            s = s.query(Q('match_phrase', text=phrase[0]))

    # support multiple values (list)
    if len(star_query) > 0:
        s = s.query('match', starring=star_query)
    if len(director_query) > 0:
        s = s.query('match', director=director_query)
    if len(language_query) > 0:
        s = s.query('match', language=language_query)
    if len(location_query) > 0:
        s = s.query('match', location=location_query)
    if len(time_query) > 0:
        s = s.query('match', time=time_query)
    if len(categories_query) > 0:
        s = s.query('match', categories=categories_query)

    # highlight
    s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')
    for key in shows:
        s = s.highlight(key, fragment_size=999999999, number_of_fragments=1)

    # determine the subset of results to display (based on current <page> value)
    start = 0 + (page - 1) * 10
    end = 10 + (page - 1) * 10

    # execute search and return results in specified range.
    response = s[start:end].execute()

    # insert data into response
    resultList = {}
    for hit in response.hits:
        result = {}
        result['score'] = hit.meta.score

        for field in hit:
            if field != 'meta':
                result[field] = getattr(hit, field)
        result['title'] = ' | '.join(result['title'])
        if 'highlight' in hit.meta:
            for field in hit.meta.highlight:
                result[field] = getattr(hit.meta.highlight, field)[0]
        resultList[hit.meta.id] = result

    # make the result list available globally
    gresults = resultList

    # get the total number of matching results
    result_num = response.hits.total

    # if we find the results, extract title and text information from doc_data, else do nothing
    if result_num > 0:
        return render_template('page_SERP.html', results=resultList, res_num=result_num, page_num=page, queries=shows)
    else:
        message = []
        if len(text_query) > 0:
            message.append('Unknown search term: ' + text_query)
        if len(star_query) > 0:
            message.append('Cannot find star: ' + star_query)

        return render_template('page_SERP.html', results=message, res_num=result_num, page_num=page, queries=shows)


# display a particular document given a result number
@app.route("/documents/<res>", methods=['GET'])
def documents(res):
    global gresults
    film = gresults[res]
    filmtitle = film['title']
    for term in film:
        if type(film[term]) is AttrList:
            film[term] = ', '.join(film[term])

    # fetch the movie from the elasticsearch index using its id
    movie = Movie.get(id=res, index='sample_film_index')
    filmdic = movie.to_dict()
    film['runtime'] = str(filmdic['runtime']) + " min"
    return render_template('page_targetArticle.html', film=film, title=filmtitle)


if __name__ == "__main__":
    app.run()

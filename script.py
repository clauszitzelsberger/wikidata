### Import packages
from qwikidata.sparql import return_sparql_query_results
import time

wikidata_properties = {
    "instance_of": "wdt:P31",
    "continent": "wdt:P30",
    "country": "wdt:P17"}

wikidata_items = {
    "sovereign_state": "wd:Q3624078",
    "big city": "wd:Q1549591",

    # continents
    "africa": "wd:Q15",
    "south america": "wd:Q18",
    "europe": "wd:Q46",
    "asia": "wd:Q48",
    "north america": "wd:Q49",
    "oceania": "wd:Q538"
}


### Sparql Queries
big_cities_country = """
select ?itemLabel ?countryLabel
where {
  ?item wdt:P31 wd:Q1549591 ;
  rdfs:label ?itemLabel .
  filter(lang(?itemLabel) = "en") .

  ?item wdt:P17 ?country .
  ?country rdfs:label ?countryLabel .
  filter(lang(?countryLabel) = "en") .

  ?item wdt:P17 wd:%s
  }
"""

def standard_query(item, prop, prop_of_property=None, prop_item=None, limit=None):
    """
    SELECT *
    WHERE ?item is instance of item
    AND ?item property ?property
    AND ?property prop_of_property prob_item

    E.g.
    SELECT *
    WHERE ?item is instance of big cities
    AND ?item sovereign state of this item ?property
    AND ?property continent of which the subject is a part is africa
    """

    item = wikidata_items[item]
    prop = wikidata_properties[prop]
    prop_of_property = wikidata_properties[prop_of_property] if prop_of_property else None
    prop_item = wikidata_items[prop_item] if prop_item else None

    query = """
    SELECT *
    WHERE {
        ?item wdt:P31 %s ;
        rdfs:label ?itemLabel .
          filter(lang(?itemLabel) = "en") .
        ?item %s ?property .
        ?property rdfs:label ?propertyLabel .
          filter(lang(?propertyLabel) = "en") . 
    """ % (item, prop)

    if prop_of_property and prop_item:
        query += "?property %s %s . " % (prop_of_property, prop_item)

    query += "} "

    if limit:
        query += "Limit %i" % (limit)

    return query

def queryWikidata(query):
    success = False
    max_tries = 5
    tries = 1

    while not success and tries <= max_tries:
        try:
            res = return_sparql_query_results(query)
            success = True
        except Exception as e:
            print(e)
            tries += 1
            time.sleep(tries ** 2)
    if not success:
        raise f"Unable to retrieve data from wikidata using this query: {query}"
    else:
        print(f"Query successful: {len(res['results']['bindings'])} entries retrieved from Wikidata.")
        return res['results']['bindings']
     
if __name__ == '__main__':
    country_query = standard_query("sovereign_state", "continent")
    countries = queryWikidata(country_query) #returns list of dictionaries

    # Count iterations
    i=0
    n_countries = len(countries)

    # Make sure that countries which are located in more than continent
    # are not written more than one time to Neo4j
    country_list = list()

    # Iterate over countries
    for country in countries:
        country_name = country['itemLabel']['value']
        print(country_name)
        
        # Get all cities
        wikidata_country_tag = country['item']['value'][31:] # Wikidata wd:? tag for given country
        cities = queryWikidata(big_cities_country % (wikidata_country_tag))
        
        if len(cities)==0:
            continue
        else:
            for city in cities:
                print(city['itemLabel']['value'])

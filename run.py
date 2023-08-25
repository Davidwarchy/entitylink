from config import DB_CONNECTION_STRING
import time

def process_phrases_serial_test():
    subjects = get_subjects()
    for subject in subjects:
        try:
            article_id, title, text, noun_phrase = subject
            noun_phrase = noun_phrase.strip() 

            result = get_best_candidate(article_id, text, noun_phrase)
            

            if result:
                try:
                    qid = result['id']
                    qid_description = result['description']
                    add_qid_to_db(article_id, qid, qid_description)
                except Exception as e:
                    print(f"Error adding entity to local database: {e}")

        except Exception as e:
            print(f'Error handling subject {article_id} : {e}')
        finally:
            time.sleep(2)

def get_db_connection():
    return sqlite3.connect(PATH_DB)

def add_qid_to_db(id_article, qid, qid_description):
    """
    Update the qid column in the destination table with the given qid for the specified otry_id
    """
    query = """
            UPDATE destination
            SET qid = ?, qidDescription = ?
            WHERE id = ? 
            """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (qid, qid_description, id_article))
        conn.commit()

import sqlite3
PATH_DB = 'source.db'

def get_subjects():
    """
    function to get the subjects (mentions) from the database 
    """
    with get_db_connection() as conn:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Search for the given phrase in the local knowledge graph using the OLKG table
        select_query = """
                        SELECT  id, title, text, subject
                        FROM    destination
                        LIMIT   100;
                        """

        cursor.execute(select_query)
        results = cursor.fetchall()
    
    return results    

def get_best_candidate(article_id, sentence, query):
    """
    given a query phrase, we want to return an entity in the knowledge graph that is most likely a match for the query
    """
    res = get_nouns(sentence)
    if res:
        sentence_context = res
    candidates = fetch_candidates(query)

    # if no candidates found, return 
    if not candidates:
        return 
    
    extract_context(candidates)
    
    for candidate in candidates:
        candidate['similarity_score'] = calculate_similarity(sentence_context, candidate['context'])
    
    best_candidate = mark_best_candidate(candidates)

    if best_candidate['similarity_score'] > 0:
        return best_candidate
    else:
        return None

def extract_context(candidates):
    for candidate in candidates:
        context = []
        for label in candidate['aliases']:
            context.extend(label.split())
        if 'property_values' in candidate:
            for property_value in candidate['property_values']:
                context.extend(property_value.split())
        context.extend(candidate['description'].split())
        candidate['context'] = context

def calculate_similarity(sentence_context, candidate_context):
    sentence_context_lower = [word.lower() for word in sentence_context]
    candidate_context_lower = [word.lower() for word in candidate_context]
    
    return sum(1 for word in sentence_context_lower if word in candidate_context_lower)


def mark_best_candidate(candidates):
    """
    marks 
    """
    best_candidate = max(candidates, key=lambda candidate: candidate['similarity_score'])
    for candidate in candidates:
        if candidate == best_candidate:
            candidate['is_best_candidate'] = 1
        else:
            candidate['is_best_candidate'] = 0
    return best_candidate

import spacy

# Load the spaCy model
nlp = spacy.load("en_core_web_sm")

def get_nouns(sentence):
    doc = nlp(sentence)
    nouns = [token.text for token in doc if token.pos_ in ("NOUN", "PROPN") or token.tag_ in ("NN", "NNS", "NNP", "NNPS")]
    return nouns

def fetch_candidates(query):
    qids = query_wikidata(query)
    candidates = get_wikidata_info(qids)
    return candidates

from wikidataintegrator import wdi_core
from joblib import Memory
import requests 
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
# Create a cache directory for joblib
cache_dir = "./__cache__"
memory = Memory(cache_dir, verbose=0)

@memory.cache
def query_wikidata(query, limit=20):
    """
    query wikidata like in browser (more likely to return results)
    """
    params = {
        "action": "query",
        "list": "search",
        "format": "json",
        "srsearch": query
    }
    response = requests.get(WIKIDATA_API, params=params)
    if response.status_code == 200:
        results = response.json()["query"]["search"]
        qids = [entry["title"] for entry in results]
        return qids
    else:
        return []

@memory.cache
def get_wikidata_info(qids):
    """
    Get label, description, aliases, and property values of a set of qids
    """
    # SPARQL query template for basic info with aliases
    sparql_query_template = """
    SELECT ?item ?itemLabel ?description ?alias WHERE {{
      VALUES ?item {{ {} }}
      ?item rdfs:label ?itemLabel.
      FILTER (LANG(?itemLabel) = 'en')
      OPTIONAL {{ ?item schema:description ?description. FILTER(LANG(?description) = 'en') }}
      OPTIONAL {{
        ?item skos:altLabel ?alias.
        FILTER(LANG(?alias) = 'en')
      }}
    }}
    """

    # SPARQL query template for property values
    sparql_property_query = """
    SELECT ?item ?property ?propertyLabel ?value ?valueLabel
    WHERE {{
      VALUES ?item {{ {} }}
      
      ?item ?property ?value.
      
      OPTIONAL {{
        ?property rdfs:label ?propertyLabel.
        FILTER(LANG(?propertyLabel) = "en").
      }}
      
      ?value rdfs:label ?valueLabel.
      FILTER(LANG(?valueLabel) = "en").
    }}
    """

    # Create a comma-separated list of QIDs for the queries
    qid_list = " ".join(f"wd:{qid}" for qid in qids)

    # Construct the SPARQL queries
    sparql_query = sparql_query_template.format(qid_list)
    sparql_property_query = sparql_property_query.format(qid_list)

    # Execute the queries
    query_result = wdi_core.WDItemEngine.execute_sparql_query(sparql_query)
    property_result = wdi_core.WDItemEngine.execute_sparql_query(sparql_property_query)

    # Process basic info results and store in a dictionary
    results = {}
    for result in query_result["results"]["bindings"]:
        qid = result["item"]["value"].split("/")[-1]
        label = result["itemLabel"]["value"]
        description = result.get("description", {}).get("value", "No description available")
        alias = result.get("alias", {}).get("value", [])

        if qid not in results:
            results[qid] = {
                "id": qid,
                "label": label,
                "description": description,
                "aliases": []
            }

        if alias:
            results[qid]["aliases"].append(alias)

    # Add property values to the results
    for prop_result in property_result["results"]["bindings"]:
        qid = prop_result["item"]["value"].split("/")[-1]
        prop_id = prop_result["property"]["value"].split("/")[-1]
        prop_label = prop_result.get("propertyLabel", {}).get("value", f"Property {prop_id}")
        value = prop_result["value"]["value"]
        value_label = prop_result["valueLabel"]["value"]

        if qid in results:
            if "property_values" not in results[qid]:
                results[qid]["property_values"] = []

            results[qid]["property_values"].append( value_label )

    return list(results.values())

if __name__ == '__main__':
    process_phrases_serial_test()
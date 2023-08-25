from config import DB_CONNECTION_STRING
import time, pyodbc 

def get_db_connection():
    return pyodbc.connect(DB_CONNECTION_STRING)

def process_phrases_serial_test():
    subjects = get_subjects_otry()
    for subject in subjects:
        try:
            print(subject)
            otrx_id, noun_phrase = subject
            noun_phrase = noun_phrase.strip() 
            sentence = get_text_for_triple(otrx_id)

            result = get_best_candidate(otrx_id, sentence, noun_phrase)
            
            # result = find(sentence, noun_phrase)

            if result:
                try:
                    qid = result['id']
                    desc = result['description']
                    add_qid_desc_to_db(otrx_id, qid, desc)
                except Exception as e:
                    print(f"Error adding to OTRY: {e}")

        except Exception as e:
            print(f'Error handling subject {otrx_id} : {e}')
        finally:
            time.sleep(2)

def add_qid_desc_to_db(otry_id, qid, desc):
    query = """
            UPDATE OTRY SET qidSubject = ?, wdDescriptionSubject = ? where id = ? 
            """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute( query, (qid, desc, otry_id))

def get_subjects_otry():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute( """
                        SELECT			TOP 100
                                        t0.id,
                                        t0.subject
                        FROM			OTRY t0
                                        INNER JOIN OART t1 ON t0.idArticle = t1.id

                        WHERE			t1.lang = 'en'
										AND  t0.isCheckedSubject <> 1
                        ORDER BY		T0.id DESC
                        """)
        return cursor.fetchall()
    
def get_text_for_triple(otrx_id):
    """
    get a sentence corresponding to a triple
    """
    query = "SELECT text FROM OART WHERE id = (SELECT idArticle FROM OTRX WHERE id = ? )"
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute( query, (otrx_id))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
        

def get_best_candidate(otry_id, sentence, query):
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
    store_stats_to_db( otry_id, sentence_context, candidates)

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

def store_stats_to_db(otry_id, sentence_context, candidates):
    # 
    sentence_context = ', '.join(sentence_context)

    # Store the information in the database
    with get_db_connection() as conn:
        cursor = conn.cursor()

        for candidate in candidates:
            qid_candidate = candidate['id']
            label_candidate = candidate['label']
            description_candidate = candidate['description']
            context_candidate = ' '.join(candidate['context'])
            score_similarity = candidate['similarity_score']

            # Check if the 'is_best_candidate' key exists in the candidate dictionary
            if 'is_best_candidate' in candidate:
                is_best_candidate = candidate['is_best_candidate']
            else:
                is_best_candidate = 0  # Default value if the key is not present

            insert_query = """
            INSERT INTO TRY1 (idOtrc, contextSentence, qidCandidate, labelCandidate, descriptionCandidate, contextCandidate, scoreSimilarity, isBestCandidate, dateSimilarityScore)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            cursor.execute(insert_query, (otry_id, sentence_context, qid_candidate, label_candidate, description_candidate, context_candidate, score_similarity, is_best_candidate))

    return

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
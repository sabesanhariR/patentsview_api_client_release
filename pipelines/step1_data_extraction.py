import os
from time import time

from helpers import fetch_all_patents, save_entity_ids

from config import OUTPUT_DIR, YEAR_FILTER, CPC_CODES, KEYWORDS, PATENT_FIELDS

def run():
    print("=== Step 1: Patent Data Extraction ===")

    os.makedirs(os.path.join(OUTPUT_DIR, "patents"), exist_ok=True)

    # Define CPC and keyword queries
    cpc_query = {
        "_and":[
            YEAR_FILTER,
            {
                "_or": [
                    {"_begins": {"cpc_current.cpc_group_id":code}}
                    for code in CPC_CODES
                ]
            }
        ]
    }

    keyword_query = {
        "_and":[
            YEAR_FILTER,
            {
                "_or": [
                    {"_text_any": {"patent_title": term}} 
                    for term in KEYWORDS
                ]
            }
        ]
    }

    seen_patent_ids = set()

    # Step 1.1: Fetch patents by CPC codes
    start_time = time()
    seen_patent_ids, id_store = fetch_all_patents(
        cpc_query, 
        PATENT_FIELDS, 
        label="CPC", 
        seen_patent_ids=seen_patent_ids,
    )
    end_time = time()
    print(f"[TIME] CPC-based patent fetching took {end_time - start_time:.2f} seconds.")

    # Step 1.2: Fetch patents by keywords
    start_time = time()
    _, id_store = fetch_all_patents(
        keyword_query, 
        PATENT_FIELDS,
        label="KEYWORD", 
        seen_patent_ids=seen_patent_ids,
        id_storage=id_store
    )
    end_time = time()
    print(f"[TIME] Keyword-based patent fetching took {end_time - start_time:.2f} seconds.")

    # Step 1.3: Save entity IDs
    save_entity_ids(id_store)

    print("=== Step 1 Completed ===")

if __name__ == "__main__":
    run()
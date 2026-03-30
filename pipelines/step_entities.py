# import os
# import json
# import pandas as pd
# from helpers import extract_entities_by_ids, stitch_entity_parts
# from config import OUTPUT_DIR, PER_PAGE, INVENTOR_URL, INVENTOR_FIELDS

# def load_entity_ids(entity):
#     path = os.path.join(OUTPUT_DIR, "entity_ids", f'{entity}_ids.csv')

#     if not os.path.exists(path):
#         raise FileNotFoundError(f"Entity ID file not found: {path}")
    
#     return pd.read_csv(path)[f'{entity}_id'].dropna().astype(str).unique().tolist()

# def run():
#     print ("=== Step 2: Entity Data Extraction ===")

#     inventor_ids = load_entity_ids("inventor")

#     output_dir = os.path.join(OUTPUT_DIR, "entities", "inventors")
#     os.makedirs(output_dir, exist_ok=True)

#     total_ids = len(inventor_ids)

#     extract_entities_by_ids(
#             entity_name="inventors",
#             api_url=INVENTOR_URL,
#             id_field="inventor_id",
#             ids=total_ids,
#             fields=INVENTOR_FIELDS,
#             output_prefix= "entities/inventors"
#     )

#     final_path = os.path.join(OUTPUT_DIR, "entities","inventors.csv")

#     stitch_entity_parts(entity_dir=output_dir, output_path=final_path)
                              
#     # Need to find a way to properly integrate batch skipping
#     batch = 1
#     for i in range(0, total_ids, PER_PAGE):
#         if entity_batch_exists(output_dir, batch):
#             print(f'[SKIP] Inventor batch {batch} already exists. Skipping...')
#             batch += 1
#             continue

#         batch_ids = inventor_ids[i:i + PER_PAGE]

#         extract_entities_by_ids(
#             entity_name="inventors",
#             api_url=INVENTOR_URL,
#             id_field="inventor_id",
#             ids=batch_ids,
#             fields=INVENTOR_FIELDS,
#             output_prefix= "entities/inventors",
#             batch=batch
#         )

#         batch += 1

#     print("=== Step 2 Completed ===")

# if __name__ == "__main__":
#     run()
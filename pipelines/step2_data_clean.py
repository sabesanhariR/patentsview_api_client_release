import os
from time import time
from helpers import (
    clean_and_flag,
    stitch_patent_parts,
    flatten_nested_columns,
    select_final_columns,
    add_gvkey
    )

from config import LOG_DIR, OUTPUT_DIR, ANALYSIS_FIELDS


def run():
    print("=== Step 2: Data Cleaning ===")

    input_path = OUTPUT_DIR + "/patents/"
    output_path = OUTPUT_DIR + "/cleaned_data/"

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Step 2.1: Stitch patent CSV parts into a single DataFrame
    start_time = time()
    stitched_df = stitch_patent_parts(input_path, output_path)
    end_time = time()
    print(f"[TIME] Stitching patent parts took {end_time - start_time:.2f} seconds.")

    # Step 2.2: Flatten nested columns in the stitched DataFrame
    nested_cols = [
        "assignees",
        "inventors",
        "cpc_current",
        "application"
    ]

    flat_df = flatten_nested_columns(
        stitched_df,
        output_path,
        nested_cols,
        id_col="patent_id")

    selected_df = select_final_columns(flat_df, ANALYSIS_FIELDS)
    print(f"[INFO] selected_df has {len(selected_df)} records and {len(selected_df.columns)} columns.")
    selected_df.to_csv(output_path + "selected_df.csv", index=False)
    
    # Step 2.3: Lookup and add gvkey column
    start_time = time()
    gvkey_added_df = add_gvkey(selected_df)
    end_time = time()
    print(f"[TIME] gvkey lookup took {end_time - start_time:.2f} seconds.")
    print(f"[DONE] {gvkey_added_df['gvkeyUO'].nunique()} GVKEYs has been added.")

    # Step 2.4: Clean and flag firm names
    cleaned_df = clean_and_flag(gvkey_added_df,
                                      firm_col="assignee_organization",
                                      log_path=LOG_DIR + "/invalid_firm_details.csv")

    print(f"[INFO] cleaned_df has {len(cleaned_df)} records and {len(cleaned_df.columns)} columns.")
    cleaned_df.to_csv(output_path + "cleaned_df.csv", index=False)
    
    print(f"[DONE - 200] Data cleaning completed. Cleaned data saved to {output_path}")
    
    print("=== Step 2 Completed ===")

if __name__ == "__main__":
    run()
    
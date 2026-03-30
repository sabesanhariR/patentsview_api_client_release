import os
# from pathlib import Path
import pandas as pd
import numpy as np
from time import time
from helpers import (
    attach_location_coordinates,
    compute_inventor_firm_distances,
    compute_coinventor_distances,
    expand_to_network_years,
    build_network_edges,
    create_firm_network_panel,
    add_temporal_metrics,
    build_master_panel
)

from config import OUTPUT_DIR, CACHE_DIR

def run():
    print("=== Step 3: Network Construction ===")

    # Step 3.0: Setup paths and load data
    input_file = OUTPUT_DIR + "/cleaned_data/cleaned_df.csv"
    output_path = OUTPUT_DIR + "/processed/"

    # Ensure output directories exist
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

    # Load lookup data
    locations_lookup = pd.read_csv(
        "input_files/g_location_disambiguated.tsv",
        sep="\t",
        usecols=[
            "location_id",
            "latitude",
            "longitude"
        ]
    )

    # Load cleaned data
    df = pd.read_csv(input_file)
    df = df[df['gvkeyUO'].notna()].copy()
    print(f"[INFO] Loaded cleaned data with {len(df)} records.")

    # Step 3.1: Compute spatial distances (inv-firm, co-inventor distances)
    # Merge location coordinates
    start_time = time()
    df = attach_location_coordinates(df, locations_lookup)
    inv_firm_dist_df = compute_inventor_firm_distances(df)
    coinventor_df = compute_coinventor_distances(inv_firm_dist_df)
    end_time = time()
    print(f"[TIME] Spatial distance calculations took {end_time - start_time:.2f} seconds.")
    
    inv_firm_dist_df_output_path = output_path + "inv_firm_distances.csv"
    inv_firm_dist_df.to_csv(inv_firm_dist_df_output_path, index=False)

    coinventor_df_output_path = output_path + "coinventor_distances.csv"
    coinventor_df.to_csv(coinventor_df_output_path, index=False)
    print(f"[DONE] Inv-Firm & Co-inventor distances saved...")
    print(f"[DONE] Step 3.1 Compute spatial distances (inv-firm & co-inv)...")

    # Step 3.2: Agg spatial distances to firm-year metrics and save them
    start_time = time()

    # First, aggregation of inventor-firm distances
    inv_firm_cols = [
        "gvkeyUO",
        "filing_year",
        "dist_inventor_firm_km",
        "inv_firm_dist_z",
        "inv_firm_dist_pct",
        "inv_firm_dist_above_p75"
    ]

    inv_firm_df = inv_firm_dist_df[inv_firm_cols].dropna(subset=["dist_inventor_firm_km"])
    inv_firm_exp = expand_to_network_years(inv_firm_df, year_col="filing_year")

    firm_network_year_inv_firm = (
        inv_firm_exp
        .groupby(["gvkeyUO", "network_year", "specification"])
        .agg(
            mean_inv_firm_km=("dist_inventor_firm_km", "mean"),
            median_inv_firm_km=("dist_inventor_firm_km", "median"),
            max_inv_firm_km=("dist_inventor_firm_km", "max"),
            mean_inv_firm_z=("inv_firm_dist_z", "mean"),
            share_inv_firm_long=("inv_firm_dist_above_p75", "mean"),
            n_inv_firm_obs=("dist_inventor_firm_km", "count")
        )
        .reset_index()
    )

    firm_year_inventor_firm_output_path = output_path + "firm_year_inv_firm_distances.csv"
    firm_network_year_inv_firm.to_csv(firm_year_inventor_firm_output_path, index=False)
    print(f"[DONE] Firm-year inventor-firm distance metrics saved...")

    # Now, aggregation of coinventor (inv-inv) distances
    coinv_cols = [
        "gvkeyUO",
        "filing_year",
        "distance_coinventor_km",
        "inv_inv_dist_z",
        "inv_inv_dist_pct",
        "inv_inv_dist_above_p75"
    ]
    
    coinv_df = coinventor_df[coinv_cols].dropna(subset=["distance_coinventor_km"])
    coinv_exp = expand_to_network_years(coinv_df, year_col="filing_year")

    firm_network_year_coinv = (
        coinv_exp
        .groupby(["gvkeyUO", "network_year", "specification"])
        .agg(
            mean_inv_inv_km=("distance_coinventor_km", "mean"),
            median_inv_inv_km=("distance_coinventor_km", "median"),
            max_inv_inv_km=("distance_coinventor_km", "max"),
            mean_inv_inv_z=("inv_inv_dist_z", "mean"),
            share_inv_inv_long=("inv_inv_dist_above_p75", "mean"),
            n_inv_inv_obs=("distance_coinventor_km", "count")
        )
        .reset_index()
    )

    firm_year_coinventor_output_path = output_path + "firm_year_coinventor_distances.csv"
    firm_network_year_coinv.to_csv(firm_year_coinventor_output_path, index=False)
    print(f"[DONE] Firm-year coinventor distance metrics saved...")

    end_time = time()
    print(f"[TIME] Firm-year spatial distance aggregation took {end_time - start_time:.2f} seconds.")
    print(f"[DONE] Step 3.2 Agg spatial distances to firm-year metrics and saved...")

    # Step 3.3: Aggregate Innovation output
    print("[INFO] Computing innovation output (patent counts)...")

    start_time = time()
    patent_cols = ["gvkeyUO", "filing_year", "patent_id"]
    patent_df = df[patent_cols].drop_duplicates()

    # Count patents by firm-year
    patent_count = (
        patent_df.groupby(['gvkeyUO', 'filing_year'])
        .agg(n_patents=("patent_id", "nunique"))
        .reset_index()
    )
    print(f"[INFO] Innovation output computed for {len(patent_count)} firm-years")

    # Expand to network years (same logic as spatial metrics)
    patent_count_exp_raw = expand_to_network_years(patent_count, year_col="filing_year")

    patent_count_exp = (
        patent_count_exp_raw.groupby(
            ['gvkeyUO', 'specification', 'network_year'],
            as_index=False
        )
        .agg({
            'filing_year': 'max',
            'n_patents': 'sum'
        })
    )

    end_time = time()
    print(f"[TIME] Computing innovation output took {end_time - start_time:.2f} seconds.")
    
    patent_count_path = output_path + "firm_year_patent_count.csv"
    patent_count_exp.to_csv(patent_count_path, index=False)
    print(f"[DONE] Step 3.3 Innovation output saved...")

    # Step 3.4: Create firm-year network panel
    start_time = time()
    print("[INFO] Computing firm-year network panel...")
    edges_exp_df = build_network_edges(df)
    network_panel = create_firm_network_panel(edges_exp_df)
    end_time = time()
    print(f"[TIME] Network construction took {end_time - start_time:.2f} seconds.")
    
    edges_exp_df.to_csv("./output_files/processed/edges_exp_df.csv", index=False)
    network_output_path = output_path + "firm_network_panel.csv"
    network_panel.to_csv(network_output_path,index=False)
    print(f"[DONE] Step 3.4 Firm-year network panel data saved...")

    # Step 3.5: Construct temporal network variables
    start_time = time()
    print("[INFO] Computing temporal network variables...")
    metric_cols = [
        "density",
        "avg_degree",
        "avg_weighted_degree",
        "avg_betweenness",
        "max_betweenness",
        "clustering",
    ]

    network_temporal_panel = add_temporal_metrics(
        network_panel,
        metrics=metric_cols,
        lags=[3,4,5]
    )
    end_time = time()
    print(f"[TIME] Temporal variable construction took {end_time - start_time:.2f} seconds.")

    temporal_output_path = output_path + "firm_network_temporal_panel.csv"
    network_temporal_panel.to_csv(temporal_output_path, index=False)
    print(f"[DONE] Step 3.5 Firm-year temporal metrics saved...")

    # Step 3.6: Merge spatial and network metrics to create master panel dataset
    start_time = time()
    print("[INFO] Building Master Panel dataset...")
    firm_year_master_df = build_master_panel(
        network_panel=network_temporal_panel,
        inv_firm_spatial=firm_network_year_inv_firm,
        coinv_spatial=firm_network_year_coinv,
        pt_count=patent_count_exp
    )
    end_time = time()
    print(f"[INFO] Master panel rows: {len(firm_year_master_df):,}")
    print(f"[TIME] Merging firm-year panels took {end_time - start_time:.2f} seconds.")
    
    master_output_path = output_path + "firm_year_master_panel.csv"
    firm_year_master_df.to_csv(master_output_path, index=False)
    print(f"[DONE] Step 3.6 Master firm-year panel saved...")

    print("=== Step 3 Completed ===")

if __name__ == "__main__":
    run()
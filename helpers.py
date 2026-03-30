import os
import json
import requests
import ast
import glob
import re
import pandas as pd
import numpy as np
from itertools import product, combinations
import networkx as nx
from typing import Sequence, List

from config import API_KEY, PATENT_URL, SAVE_QUERY, PER_PAGE, OUTPUT_DIR, CPC_CODES


# ============================================================
# STEP 1 — PATENT DATA EXTRACTION HELPERS
# ============================================================

def search_patents(query_json, fields, after=None):    
    headers = {
        'X-Api-Key': API_KEY,
        'Content-Type': 'application/json'
    }

    options = {
        "size": PER_PAGE,
        "count": False
        }
    
    if after is not None:
        options["after"] = str(after)
    
    params = {
        'q': json.dumps(query_json),
        'f': json.dumps(fields),
        'o': json.dumps(options)
        # 's': json.dumps([{"patent_id": "asc"}]),
    }

    print("Requesting:", PATENT_URL)
    response = requests.get(PATENT_URL, headers=headers, params=params)
    response.raise_for_status()  # raises an error if status != 200
    return response.json()


def extract_unique_entity_ids(patent_records, store):
    for patent in patent_records:
        for inventor in patent.get("inventors", []):
            iid = inventor.get("inventor_id")
            if iid:
                store["inventors"].add(iid)
        for assignee in patent.get("assignees", []):
            aid = assignee.get("assignee_id")
            if aid:
                store["assignees"].add(aid)
        for cpc in patent.get("cpc_current", []):
            raw_cpc = cpc.get("cpc_group_id")
            if not raw_cpc:
                continue
            cpc_prefix = raw_cpc.split("/")[0]
            if cpc_prefix:
                store["cpc_groups"].add(cpc_prefix)    
    return store


def save_chunk(dfs, label, file_counter, total_fetched, save_excel=True):
    combined = pd.concat(dfs, ignore_index=True)

    out_dir = os.path.join(OUTPUT_DIR, "patents")
    os.makedirs(out_dir, exist_ok=True)

    csv_filename = f"{label}_part_{file_counter}_total_{total_fetched}.csv"
    csv_path = os.path.join(out_dir, csv_filename)
    combined.to_csv(csv_path, index=False)

    if save_excel:
        excel_filename = f"{label}_part_{file_counter}_total_{total_fetched}.xlsx"
        excel_path = os.path.join(out_dir, excel_filename)
        combined.to_excel(excel_path, index=False)
    
    print(f"Saved {len(combined)} records → {csv_path}")


def fetch_all_patents(query, fields, label, seen_patent_ids=None, id_storage=None):
    
    buffer = []
    after = None
    file_counter = 1
    total_patents_fetched = 0
    total_patents_skipped = 0

    if seen_patent_ids is None:
        seen_patent_ids = set()

    if id_storage is None:
        id_storage = {
            "inventors": set(),
            "assignees": set(),
            "cpc_groups": set()
        }

    print(f'Starting patent extraction for [{label}]...')

    while True:
        data = search_patents(query, fields, after=after)
        patents = data.get("patents", [])
        
        if not patents:
            print(f'[{label}] No more records returned')
            break
        
        id_storage = extract_unique_entity_ids(patents, id_storage)
        df = pd.json_normalize(patents)

        
        initial_len = len(df)
        df = df[~df["patent_id"].isin(seen_patent_ids)]
        skipped = initial_len - len(df)
        total_patents_skipped += skipped

        if not df.empty:
            seen_patent_ids.update(df["patent_id"].tolist())
            buffer.append(df)
            total_patents_fetched += len(df)

        after = str(patents[-1]["patent_id"])
        print(
            f'[{label}] API batch: {len(patents)} | '
            f'kept: {len(df)} | '
            f'skipped: {skipped} | '
            f'cursor: {after}'
        )

        if sum(len(x) for x in buffer) >= SAVE_QUERY:
            save_chunk(buffer, label, file_counter, total_patents_fetched)
            buffer = []
            file_counter += 1

    if buffer:
        save_chunk(buffer, label, file_counter, total_patents_fetched)

    print(
        f'[{label}] Extraction complete | '
        f'Total kept: {total_patents_fetched} | '
        f'Total duplicates skipped: {total_patents_skipped}'
    )

    return seen_patent_ids, id_storage


def save_entity_ids(id_store):
    # Writes extracted entity IDs to CSV files for Step 2 Reference.
    entity_dir = os.path.join(OUTPUT_DIR, "entity_ids")
    os.makedirs(entity_dir, exist_ok=True)

    for entity, ids in id_store.items():
        if not ids:
            continue
        
        col = f'{entity[:-1]}_id'
        df = pd.DataFrame(sorted(ids), columns=[col])
        file_path = os.path.join(entity_dir, f"{col}s.csv")
        df.to_csv(file_path, index=False)

        print(f'[ID STORE] Saved {len(df)} {entity} IDs → {file_path}')


# ============================================================
# STEP 2 — PATENT DATA CLEANING HELPERS
# ============================================================

def stitch_patent_parts(
        input_dir:str,
        output_path:str,
        id_col:str = "patent_id"
        ) -> pd.DataFrame:
    # Combines all patent parts into a single DataFrame.
    
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")
    
    all_dfs = []
    for file in sorted(csv_files):
        try:
            df = pd.read_csv(file)
            all_dfs.append(df)
        except Exception as e:
            print(f"[ERROR] Reading CSV file {file}: {e}")
            continue

    stitched_df = pd.concat(all_dfs, ignore_index=True)
    stitched_df.drop_duplicates(subset=[id_col], inplace=True)
    stitched_df.reset_index(drop=True, inplace=True)

    stitched_df.to_csv(output_path + "stitched_patents.csv", index=False)

    return stitched_df

def flatten_nested_columns(
        df: pd.DataFrame,
        output_dir: str,
        nested_cols: list,
        id_col: str = "patent_id",
        ) -> pd.DataFrame:
    
    """
    Flatten patent data into relational rows while preserving all patents.

    Rules:
    - inventors x assignees → Cartesian product
    - if one side missing → explode the other
    - if both missing → keep single patent-level row
    - CPC: only cpc_sequence == 0 (primary CPC)
    """
    
    flat_records = []

    for _, row in df.iterrows():
        base_row = row.to_dict()
        # patent_id = base_row.get(id_col)

        inventors = []
        try:
            inventors = ast.literal_eval(row.get("inventors", "[]"))
            if not isinstance(inventors, list):
                inventors = []
        except Exception as e:
            inventors = []

        assignees = []
        try:
            assignees = ast.literal_eval(row.get("assignees", "[]"))
            if not isinstance(assignees, list):
                assignees = []
        except Exception as e:
            assignees = []

        primary_cpc = None
        try:
            cpc_list = ast.literal_eval(row.get("cpc_current", "[]"))
            if isinstance(cpc_list, list):
                for cpc in cpc_list:
                    if not isinstance(cpc, dict):
                        continue
                    raw_cpc = cpc.get("cpc_group_id")
                    if not raw_cpc:
                        continue
                    cpc_prefix = raw_cpc.split("/")[0]
                    if cpc_prefix in CPC_CODES:
                        primary_cpc = cpc
                        break
        except Exception as e:
            primary_cpc = None
        
        application = []
        try:
            application = ast.literal_eval(row.get("application", "[]"))
            if not isinstance(application, list):
                application = []
        except Exception as e:
            application = []

        for col in nested_cols:
            base_row.pop(col, None)

        # Determine the row combinations based on available nested data
        if inventors and assignees:
            combinations = product(inventors, assignees)
        elif inventors:
            combinations = [(inv, None) for inv in inventors]
        elif assignees:
            combinations = [(None, assg) for assg in assignees]
        else:
            combinations = [(None, None)]

        for inventor, assignee in combinations:
            new_row = base_row.copy()
            if isinstance(inventor, dict):
                for k, v in inventor.items():
                    new_row[k] = v
            if isinstance(assignee, dict):
                for k, v in assignee.items():
                    new_row[k] = v
            if primary_cpc:
                for k, v in primary_cpc.items():
                    new_row[k] = v
            if application:
                for k, v in application[0].items():
                    new_row[k] = v
            flat_records.append(new_row)

    flat_df = pd.DataFrame(flat_records)

    # Saving the file to the specified output directory
    output_path = output_dir + "flattened_patents.csv"
    flat_df.to_csv(output_path, index=False)
    print(f"[SAVE] Flattened patent data → {output_path}")

    return flat_df

def select_final_columns (df: pd.DataFrame, final_columns: list):

    available_cols = [col for col in final_columns if col in df.columns]
    missing_cols = list(set(final_columns) - set(available_cols))

    if missing_cols:
        print(f"[WARN] Missing columns not found in DataFrame: {missing_cols}")

    final_df = df.loc[:, available_cols].copy()

    return final_df

def add_gvkey(df: pd.DataFrame):

    gvkey_df = pd.read_csv('./input_files/gvkey_mapping.csv')
    # Add gvkey column in patent_df using lookup from gvkey_df via patent_id
    df = df.merge(
        gvkey_df[['patent_id', 'gvkeyUO', 'gvkeyFR', 'clean_name']], 
        on='patent_id', how='left', suffixes=('', '_gvkey'))

    return df

def is_valid_firm_name(name):
    if not isinstance(name, str):
        return False
    if not re.search(r"[A-Za-z]", name):
        return False
    if len(name.strip()) < 3:
        return False
    return True

def clean_and_flag(df, firm_col, log_path):
    df = df.copy()
    df['filing_year'] = pd.to_datetime(df['filing_date'], errors='coerce').dt.year.astype('Int64')
    df["valid_firm_name"] = df[firm_col].apply(is_valid_firm_name)

    df.drop(columns=["filing_date"], inplace=True, errors='ignore')
    invalid_df = df[~df["valid_firm_name"]].drop_duplicates()
    invalid_df.to_csv(log_path, index=False)
    
    return df[df["valid_firm_name"]]


# ============================================================
# STEP 3 — NETWORK CONSTRUCTION AND PANEL PREP HELPERS
# ============================================================

# Step 3.1
def attach_location_coordinates(
        df: pd.DataFrame,
        locations_lookup: pd.DataFrame
    ) -> pd.DataFrame:
    """
    Attach inventor and assignee latitude/longitude to the main dataframe.
    """

    out = df.merge(
        locations_lookup,
        left_on="inventor_location_id",
        right_on="location_id",
        how="left"
    ).rename(columns={
        "latitude": "inventor_lat",
        "longitude": "inventor_lon"
    }).drop(columns=["location_id"])

    out = out.merge(
        locations_lookup,
        left_on="assignee_location_id",
        right_on="location_id",
        how="left"
    ).rename(columns={
        "latitude": "assignee_lat",
        "longitude": "assignee_lon"
    }).drop(columns=["location_id"])

    return out

def haversine_km(lat1, lon1, lat2, lon2):
    """
    Vectorized haversine distance (km).
    Accepts scalars or NumPy arrays.
    """

    lat1 = np.asarray(lat1, dtype="float64")
    lon1 = np.asarray(lon1, dtype="float64")
    lat2 = np.asarray(lat2, dtype="float64")
    lon2 = np.asarray(lon2, dtype="float64")

    mask = (
        np.isnan(lat1) | np.isnan(lon1) |
        np.isnan(lat2) | np.isnan(lon2)
    )

    # radians
    lat1, lon1, lat2, lon2 = map(
        np.radians, [lat1, lon1, lat2, lon2]
    )

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        np.sin(dlat / 2) ** 2 +
        np.cos(lat1) * np.cos(lat2) *
        np.sin(dlon / 2) ** 2
    )

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371.0 * c

    if km.ndim == 0:
        return np.nan if mask else km

    km[mask] = np.nan
    return km

def normalize_within_firm(
        df: pd.DataFrame,
        distance_col: str,
        firm_col: str = "gvkeyUO",
        prefix: str | None = None
        ) -> pd.DataFrame:
    
    """
    Compute within-firm normalized distance measures.

    Adds:
    - z-score
    - percentile rank
    - above-P75 indicator

    Normalization is firm-specific and vectorized.
    """

    if prefix is None:
        prefix = distance_col.replace("_km", "")

    out = df.copy()

    grp = out.groupby(firm_col)[distance_col]

    mean = grp.transform("mean")
    std = grp.transform("std")
    p75 = grp.transform(lambda x: x.quantile(0.75))

    out[f"{prefix}_z"] = (out[distance_col] - mean) / std.replace(0, np.nan)
    out[f"{prefix}_pct"] = grp.rank(pct=True)
    out[f"{prefix}_above_p75"] = (out[distance_col] > p75).astype(int)

    return out

def compute_coinventor_distances(df):
    coinventor_rows = []

    for pid, g in df.groupby("patent_id"):
        gvkey = g['gvkeyUO'].iloc[0]
        fyear = g['filing_year'].iloc[0]
        inventors = g[
            ["inventor_id", "inventor_lat", "inventor_lon"]
        ].dropna().drop_duplicates()
        
        if len(inventors) < 2:
            continue

        coords = inventors[["inventor_lat", "inventor_lon"]].values
        ids = inventors["inventor_id"].values

        for i in range(len(coords)):
            for j in range(i + 1, len(coords)):
                coinventor_rows.append({
                    "patent_id": pid,
                    "gvkeyUO": gvkey,
                    'filing_year': fyear,
                    "inventor_id_1": ids[i],
                    "inventor_id_2": ids[j],
                    "distance_coinventor_km": haversine_km(
                        coords[i][0], coords[i][1],
                        coords[j][0], coords[j][1]
                    )
                })

    coinv_df = pd.DataFrame(coinventor_rows)
    coinv_normalized = normalize_within_firm(
        coinv_df,
        distance_col="distance_coinventor_km",
        firm_col="gvkeyUO",
        prefix="inv_inv_dist"
    )

    return coinv_normalized

def compute_inventor_firm_distances(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute raw and within-firm normalized inventor-firm distances.
    """

    valid = (
        df["inventor_lat"].notna() &
        df["inventor_lon"].notna() &
        df["assignee_lat"].notna() &
        df["assignee_lon"].notna()
    )

    out = df.copy()
    out["dist_inventor_firm_km"] = np.nan

    out.loc[valid, "dist_inventor_firm_km"] = haversine_km(
        out.loc[valid, "inventor_lat"].values,
        out.loc[valid, "inventor_lon"].values,
        out.loc[valid, "assignee_lat"].values,
        out.loc[valid, "assignee_lon"].values
    )

    # Normalize within firm
    out = normalize_within_firm(
        out,
        distance_col="dist_inventor_firm_km",
        firm_col="gvkeyUO",
        prefix="inv_firm_dist"
    )

    return out

# Step 3.2
def expand_to_network_years(
    df: pd.DataFrame,
    year_col: str,
    time_windows: tuple[int, ...] = (3, 4, 5),
    prefix: str = "time_window"
) -> pd.DataFrame:
    """
    Expand observations to network formation years.
    
    For each patent filed in year f, the collaboration network is assumed
    to span [f - tw, f]. We assign each patent to multiple network_years
    within this R&D formation window.
    
    For fixed 4-year gap:
    - network_year represents the calendar year within the R&D window
    - Patents contribute to all network_years in [f - tw, f]
    
    Example: Patent filed in 2003 with tw=4
      - R&D window: [1999, 2000, 2001, 2002, 2003]
      - Contributes to network_years: 1999, 2000, 2001, 2002, 2003
    """

    rows = []

    for r in df.itertuples(index=False):
        filing_year = getattr(r, year_col)
        
        for tw in time_windows:
            # R&D formation window: [filing_year - tw, filing_year]
            formation_start = filing_year - tw
            formation_end = filing_year
            
            # Patent contributes to all network_years in this window
            for ny in range(int(formation_start), int(formation_end) + 1):
                rows.append({
                    **r._asdict(),
                    "network_year": ny,
                    "specification": f"{prefix}_{tw}_network_years"
                })

    return pd.DataFrame(rows)

'''
All functions below operate on:
- gvkeyUO as firm identifier
- network_year as temporal index
- rolling time-window inventor networks
'''
# Step 3.4
def build_network_edges(
    cleaned_df: pd.DataFrame,
    time_windows: Sequence[int] = (3, 4, 5),
) -> pd.DataFrame:
    """
    Build expanded inventor dyads with R&D formation windows.
    
    For fixed 4-year gap:
    - Each patent filed in year f has an R&D window [f - tw, f]
    - Co-inventor ties from that patent contribute to all network_years
      within that R&D window
    - The network at formation year f-tw includes all patents whose
      R&D window covers that year
    
    Output: edges_exp_df (edge list with network_year expansion)
    """

    # --- Minimal required columns only ---
    df = cleaned_df.loc[
        cleaned_df["gvkeyUO"].notna(),
        ["patent_id", "inventor_id", "filing_year", "clean_name", "gvkeyUO", "assignee_organization"]
    ].copy()

    df["filing_year"] = df["filing_year"].astype(int)

    # --- Patent → inventor lists ---
    inventors_by_patent = (
        df.groupby(["patent_id", "filing_year"])["inventor_id"]
          .apply(list)
          .reset_index()
    )

    # --- Dyad generation ---
    dyads = []
    for patid, fyear, inventors in inventors_by_patent.itertuples(index=False):
        if len(inventors) < 2:
            continue
        for i1, i2 in combinations(inventors, 2):
            dyads.append((patid, fyear, i1, i2))

    dyads_df = pd.DataFrame(
        dyads,
        columns=["patent_id", "filing_year", "inventor_1", "inventor_2"]
    )

    # --- Attach firm identifiers ---
    firm_key = (
        df[["patent_id", "gvkeyUO", "clean_name", "assignee_organization"]]
        .drop_duplicates()
    )

    dyads_df = dyads_df.merge(firm_key, on="patent_id", how="left")

    # --- Aggregate joint patents ---
    dyads_df = (
        dyads_df
        .groupby(
            ["inventor_1", "inventor_2", "gvkeyUO", "clean_name", "filing_year"],
            as_index=False
        )
        .size()
        .rename(columns={"size": "joint_patents"})
    )

    # --------------------------------------------------
    # R&D formation window expansion
    # --------------------------------------------------

    records = []

    for row in dyads_df.itertuples(index=False):
        filing_year = row.filing_year
        
        for tw in time_windows:
            # R&D formation window: [filing_year - tw, filing_year]
            formation_start = filing_year - tw
            formation_end = filing_year
            
            # This co-inventor tie contributes to all network_years
            # within the R&D window
            for nyear in range(formation_start, formation_end + 1):
                records.append({
                    "inventor_1": row.inventor_1,
                    "inventor_2": row.inventor_2,
                    "joint_patents": row.joint_patents,
                    "gvkeyUO": row.gvkeyUO,
                    "clean_name": row.clean_name,
                    "filing_year": filing_year,
                    "network_year": nyear,
                    "specification": f"time_window_{tw}_network_years",
                })

    edges_exp_df = pd.DataFrame(records)

    edges_final = (
        edges_exp_df.groupby(
            ["gvkeyUO", "specification", "network_year", 
             "inventor_1", "inventor_2"],
            as_index=False
        )
        .agg({
            "clean_name": "first",
            "filing_year": "max",
            "joint_patents": "sum"  # Total joint patents from all contributing patents
        })
    )

    n_edge_dups = len(edges_exp_df) - len(edges_final)
    if n_edge_dups > 0:
        print(f"[DEDUP] build_network_edges: Aggregated {n_edge_dups} duplicate edges")

    return edges_final

def build_firm_year_graph(df: pd.DataFrame) -> nx.Graph:
    G = nx.Graph()
    for r in df.itertuples(index=False):
        if G.has_edge(r.inventor_1, r.inventor_2):
            G[r.inventor_1][r.inventor_2]["weight"] += r.joint_patents
        else:
            G.add_edge(r.inventor_1, r.inventor_2, weight=r.joint_patents)
    return G

def compute_network_metrics(G: nx.Graph) -> dict:
    if G.number_of_nodes() == 0:
        return dict.fromkeys(
            ["density", "avg_degree", "avg_weighted_degree",
             "avg_betweenness", "max_betweenness", "clustering"],
            0.0
        )

    deg = dict(G.degree())
    wdeg = dict(G.degree(weight="weight"))
    btw = nx.betweenness_centrality(G)

    return {
        "density": nx.density(G),
        "avg_degree": sum(deg.values()) / len(deg),
        "avg_weighted_degree": sum(wdeg.values()) / len(wdeg),
        "avg_betweenness": sum(btw.values()) / len(btw),
        "max_betweenness": max(btw.values()),
        "clustering": nx.average_clustering(G),
    }

def create_firm_network_panel(edges_exp_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (gvkey, nyear, spec), grp in edges_exp_df.groupby(
        ["gvkeyUO", "network_year", "specification"]
    ):
        clean_name = grp['clean_name'].iloc[0]
        G = build_firm_year_graph(grp)
        metrics = compute_network_metrics(G)

        rows.append({
            "gvkeyUO": gvkey,
            "network_year": nyear,
            "specification": spec,
            "clean_name": clean_name,
            **metrics
        })

    network_panel_raw = pd.DataFrame(rows)

    network_panel = network_panel_raw.groupby(
        ['gvkeyUO', 'specification', 'network_year'], 
        as_index=False,
        sort=False
    ).agg({
        'clean_name': 'first',
        'density': 'mean',
        'avg_degree': 'mean',
        'avg_weighted_degree': 'mean',
        'avg_betweenness': 'mean',
        'max_betweenness': 'max',
        'clustering': 'mean'
    })
    
    n_duplicates = len(network_panel_raw) - len(network_panel)
    if n_duplicates > 0:
        print(f"[INFO] create_firm_network_panel: Aggregated {n_duplicates} duplicate observations")

    return network_panel

# Step 3.5
def add_temporal_metrics(
    network_panel: pd.DataFrame,
    metrics: List[str],
    lags: List[int] = [3, 4, 5]
) -> pd.DataFrame:
    """
    Add lagged variables and fixed-lag temporal changes (deltas).
    
    Creates:
    - {metric}_lag{n}: Network metric from exactly n years ago
    - delta_{metric}_lag{n}: Change over exactly n years (metric_t - metric_{t-n})
    
    Only observations with valid t-n comparisons are retained for delta calculations.
    """

    df = network_panel.sort_values(
        ["gvkeyUO", "specification", "network_year"]
    ).copy()

    for m in metrics:
        for lag in lags:
            # Create lagged variable via merge on (year + lag)
            df_temp = df[["gvkeyUO", "specification", "network_year", m]].copy()
            df_temp["network_year"] = df_temp["network_year"] + lag
            df_temp = df_temp.rename(columns={m: f"{m}_lag{lag}"})
            
            df = df.merge(
                df_temp,
                on=["gvkeyUO", "specification", "network_year"],
                how="left"
            )
            
            # Compute fixed-lag delta
            df[f"delta_{m}_lag{lag}"] = df[m] - df[f"{m}_lag{lag}"]

    return df

# Step 3.6
def build_master_panel(
    network_panel: pd.DataFrame,
    inv_firm_spatial: pd.DataFrame,
    coinv_spatial: pd.DataFrame,
    pt_count: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge network dynamics and spatial exposure metrics into a single
    master panel indexed by (gvkeyUO x network_year x specification).

    Assumes all inputs are already aggregated to this level.
    """

    # Merge inventor–firm distances
    master = network_panel.merge(
        inv_firm_spatial,
        on=["gvkeyUO", "network_year", "specification"],
        how="left"
    )

    # Merge co-inventor distances
    master = master.merge(
        coinv_spatial,
        on=["gvkeyUO", "network_year", "specification"],
        how="left"
    )

    # Merge patent counts
    master = master.merge(
        pt_count,
        on=["gvkeyUO", "network_year", "specification"],
        how="left"
    )

    # Remove duplicate columns
    master = master.drop(columns=["filing_year_y"], errors="ignore").rename(
        columns={"filing_year_x": "filing_year"}
    )

    # Fill missing patent counts with 0 (firms with no patents in that year)
    master['n_patents'] = master['n_patents'].fillna(0).astype(int)

    # Enforce deterministic ordering
    master = (
        master
        .sort_values(["gvkeyUO", "specification", "network_year"])
        .reset_index(drop=True)
    )

    return master
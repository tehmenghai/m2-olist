#!/usr/bin/env python3
"""
Simple CSV → GCS loader for Ben's pipeline (workaround for Meltano compatibility issues)
"""

import json
from pathlib import Path
from google.cloud import storage

def load_csvs_to_gcs():
    """Load CSV files from data/ to GCS bucket"""
    gcs_client = storage.Client(project="olist-dsai-491108")
    bucket = gcs_client.bucket("olist-dsai-ben-bronze")
    try:
        bucket.create()
    except:
        pass  # Bucket already exists

    data_dir = Path("/home/auyan/m2-olist/data")
    csv_files = list(data_dir.glob("*.csv"))

    print(f"Found {len(csv_files)} CSV files to upload")

    for csv_file in sorted(csv_files):
        blob_path = f"olist/raw/{csv_file.name.replace('.csv', '.jsonl')}"

        # Convert CSV to JSONL format
        import pandas as pd
        df = pd.read_csv(csv_file)
        jsonl_content = df.to_json(orient='records', lines=True)

        # Upload to GCS
        blob = bucket.blob(blob_path)
        blob.upload_from_string(jsonl_content, content_type="application/jsonl")
        print(f"✓ {csv_file.name} → gs://olist-dsai-ben-bronze/{blob_path} ({len(df)} rows)")

    print("✓ All CSVs loaded to GCS")

if __name__ == "__main__":
    load_csvs_to_gcs()

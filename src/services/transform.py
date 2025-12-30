# src/services/transform.py
import json
import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Prepares dataframe for SQL insertion."""
    if df.empty:
        return df

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
    
    # Convert lists/dicts to JSON strings to allow Postgres JSONB ingestion
    for col in df.columns:
        # Check if column object type contains list or dict
        if df[col].dtype == 'object':
             if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
            
    return df
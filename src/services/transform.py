import json
from datetime import datetime

import pandas as pd
from bson import ObjectId


class MongoJSONEncoder(json.JSONEncoder):
    """
    Custom JSON Encoder to handle MongoDB specific types
    like ObjectId and Datetime inside nested dictionaries/lists.
    """
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Prepares dataframe for SQL insertion."""
    if df.empty:
        return df

    # 1. Convert Primary Key '_id' explicitly to string first
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
    
    # 2. Iterate over object columns to handle nested structures and ObjectIds
    for col in df.columns:
        if df[col].dtype == 'object':
            
            def process_cell(x):
                # If it's a list or dict, dump it to JSON string (using custom encoder)
                if isinstance(x, (list, dict)):
                    return json.dumps(x, cls=MongoJSONEncoder)
                # If it's a standalone ObjectId (not nested), convert to string
                if isinstance(x, ObjectId):
                    return str(x)
                return x

            df[col] = df[col].apply(process_cell)
            
    return df
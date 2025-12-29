import json
import os
import sys

import ijson
from pymongo import MongoClient
from tqdm import tqdm

# --- CONFIG ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018/")
DB_NAME = "source_db"
COLLECTION_NAME = "display_ticket"
JSON_FILE = "display_tickets.json"
BATCH_SIZE = 1000

# --- CONNECT TO MONGODB ---
def connect_to_mongo():
    """Establish connection to MongoDB with error handling"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print(" Connected to MongoDB!")
        return client
    except Exception as e:
        print(f" Failed to connect to MongoDB: {e}")
        sys.exit(1)

# --- DETECT FILE FORMAT ---
def detect_format(file_path):
    """Detect if file is JSON array or line-delimited JSON"""
    try:
        with open(file_path, "rb") as f:
            first_char = f.read(1).decode("utf-8").strip()
            return "array" if first_char == "[" else "lines"
    except FileNotFoundError:
        print(f" File not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f" Error reading file: {e}")
        sys.exit(1)

# --- COUNT ITEMS (OPTIONAL) ---
def count_items(file_path, file_format):
    """Count total items for progress bar (optional, can be slow for large files)"""
    try:
        with open(file_path, "rb") as f:
            if file_format == "array":
                return sum(1 for _ in ijson.items(f, "item"))
            else:
                return sum(1 for line in f if line.strip())
    except Exception as e:
        print(f" Could not count items: {e}")
        return None

# --- INSERT DATA WITH PROGRESS BAR ---
def insert_batches(collection, data_iter, total=None, file_format="array"):
    """Insert documents in batches with progress tracking and error handling"""
    batch = []
    count = 0
    failed = 0
    error_log = []
    
    pbar = tqdm(total=total, desc="Inserting documents", unit=" docs")
    
    try:
        for idx, record in enumerate(data_iter):
            # Validate record
            if not isinstance(record, dict):
                error_log.append(f"Line {idx + 1}: Not a valid JSON object")
                failed += 1
                continue
            
            batch.append(record)
            
            if len(batch) >= BATCH_SIZE:
                try:
                    collection.insert_many(batch, ordered=False)
                    count += len(batch)
                    pbar.update(len(batch))
                except Exception as e:
                    error_log.append(f"Batch at record {count}: {str(e)[:100]}")
                    failed += len(batch)
                batch = []
        
        # Insert remaining documents
        if batch:
            try:
                collection.insert_many(batch, ordered=False)
                count += len(batch)
                pbar.update(len(batch))
            except Exception as e:
                error_log.append(f"Final batch: {str(e)[:100]}")
                failed += len(batch)
    
    except Exception as e:
        print(f"\n Unexpected error during insertion: {e}")
    finally:
        pbar.close()
    
    # Print summary
    print(f"\n Successfully inserted: {count} records")
    if failed > 0:
        print(f" Failed to insert: {failed} records")
        if error_log:
            print("\n Error summary (first 5 errors):")
            for err in error_log[:5]:
                print(f"   - {err}")
    
    return count, failed

# --- PROCESS JSON ARRAY ---
def process_json_array(file_path, collection, total=None):
    """Process JSON array format using ijson for streaming"""
    print(" Processing JSON array format...")
    with open(file_path, "rb") as f:
        data_iter = ijson.items(f, "item")
        return insert_batches(collection, data_iter, total, "array")

# --- PROCESS LINE-DELIMITED JSON ---
def process_line_delimited(file_path, collection, total=None):
    """Process line-delimited JSON format"""
    print(" Processing line-delimited JSON format...")
    
    def parse_lines(file_handle):
        """Generator to parse JSON lines with error handling"""
        for line_num, line in enumerate(file_handle, 1):
            line_str = line.decode('utf-8').strip()
            if not line_str:  # Skip empty lines
                continue
            try:
                yield json.loads(line_str)
            except json.JSONDecodeError as e:
                print(f"\n Invalid JSON at line {line_num}: {e}")
                continue
    
    with open(file_path, "rb") as f:
        data_iter = parse_lines(f)
        return insert_batches(collection, data_iter, total, "lines")

# --- CREATE INDEXES ---
def create_indexes(collection):
    """Create indexes for better query performance"""
    try:
        # Add your indexes here based on your query patterns
        # Example: collection.create_index([("ticket_id", 1)], unique=True)
        # Example: collection.create_index([("created_at", -1)])
        
        # Uncomment and modify based on your schema:
        # collection.create_index([("ticket_id", 1)])
        # print(" Indexes created successfully")
        
        pass  # Remove this if you add indexes
    except Exception as e:
        print(f" Failed to create indexes: {e}")

# --- MAIN EXECUTION ---
def main():
    """Main execution flow"""
    print("=" * 50)
    print(" MongoDB JSON Import Tool")
    print("=" * 50)
    
    # Validate file exists
    if not os.path.exists(JSON_FILE):
        print(f" File not found: {JSON_FILE}")
        sys.exit(1)
    
    # Connect to MongoDB
    client = connect_to_mongo()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Clear existing data
    try:
        deleted_count = collection.delete_many({}).deleted_count
        print(f"Cleared {deleted_count} existing documents from '{COLLECTION_NAME}'")
    except Exception as e:
        print(f" Failed to clear collection: {e}")
        client.close()
        sys.exit(1)
    
    # Detect file format
    file_format = detect_format(JSON_FILE)
    print(f" Detected format: {file_format}")
    
    # Count items (optional - comment out for very large files)
    print(" Counting items...")
    total_items = count_items(JSON_FILE, file_format)
    if total_items:
        print(f" Found {total_items:,} items")
    else:
        print(" Skipping count, processing without progress percentage")
    
    # Process file based on format
    try:
        if file_format == "array":
            success, failed = process_json_array(JSON_FILE, collection, total_items)
        else:
            success, failed = process_line_delimited(JSON_FILE, collection, total_items)
        
        # Create indexes
        if success > 0:
            print("\n Creating indexes...")
            create_indexes(collection)
        
        # Final summary
        print("\n" + "=" * 50)
        print(" IMPORT SUMMARY")
        print("=" * 50)
        print(f" Total inserted: {success:,}")
        print(f" Total failed: {failed:,}")
        print(f" Collection: {DB_NAME}.{COLLECTION_NAME}")
        print(f" Final count: {collection.count_documents({}):,}")
        print("=" * 50)
        
    except KeyboardInterrupt:
        print("\n\n Import interrupted by user")
    except Exception as e:
        print(f"\n Unexpected error: {e}")
    finally:
        client.close()
        print("\n MongoDB connection closed")

if __name__ == "__main__":
    main()
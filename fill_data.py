import csv
import json
import os
from datetime import datetime
from itertools import islice  # Efficiently skip rows

# ==========================================
# CONFIGURATION
# ==========================================
SOURCE_CSV = "chunk_3_rows_10001_to_15000.csv"
OUTPUT_JSON = "processed-chunk-3.json"
START_SERIAL_NO = 1268         # Serial number for the NEXT item
ROWS_TO_SKIP = 1267            # How many rows are ALREADY done (0 to 1267)
BATCH_SIZE = 1000              # How many NEW rows to add this run (optional limit)

def append_next_batch(csv_path, json_path, skip_count, start_serial, limit=None):
    # 1. Load existing JSON data to preserve it
    processed_map = {}
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                processed_map = json.load(f)
            print(f"ðŸ“‚ Loaded existing checkpoint: {len(processed_map)} entries found.")
        except Exception:
            print("âš ï¸ Could not load existing JSON. Starting fresh or overwriting if safe.")

    # 2. Read the CSV, SKIPPING the rows already done
    new_rows = []
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    file_opened = False

    for encoding in encodings:
        try:
            with open(csv_path, mode='r', encoding=encoding) as f:
                reader = csv.reader(f)

                # --- CRITICAL STEP: Skip the first N rows ---
                # We use islice to efficiently jump over 'skip_count' rows
                sliced_reader = islice(reader, skip_count, None)

                for line in sliced_reader:
                    if line and len(line) >= 2:
                        new_rows.append({
                            "phone": line[0].strip(),
                            "pledge": line[1].strip()
                        })
                        # Stop if we reached our batch limit (optional)
                        if limit and len(new_rows) >= limit:
                            break

            print(f"âœ… Skipped first {skip_count} rows. Loaded next {len(new_rows)} rows using {encoding}.")
            file_opened = True
            break
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"âŒ Error: File '{csv_path}' not found.")
            return

    if not file_opened:
        print("âŒ Failed to read CSV.")
        return

    # 3. Process the NEW rows
    current_time = datetime.now().isoformat()
    print(f"ðŸ”„ Appending {len(new_rows)} new entries starting at Serial #{start_serial}...")

    for i, row in enumerate(new_rows):
        phone = row['phone']
        pledge = row['pledge']
        unique_key = f"{phone}::{pledge[:40]}"

        # Add to the main map
        processed_map[unique_key] = {
            "serialNo": start_serial + i,
            "status": "success",
            "timestamp": current_time,
            "phone": phone
        }

    # 4. Write everything back
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(processed_map, f, indent=2)
        print(f"ðŸŽ‰ Done! JSON now has {len(processed_map)} total entries.")
        print(f"   Next run, set ROWS_TO_SKIP = {skip_count + len(new_rows)}")
        print(f"   Next run, set START_SERIAL_NO = {start_serial + len(new_rows)}")
    except Exception as e:
        print(f"âŒ Error writing JSON: {e}")

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    # If limit is None, it processes ALL remaining rows in the file
    append_next_batch(SOURCE_CSV, OUTPUT_JSON, ROWS_TO_SKIP, START_SERIAL_NO, limit=BATCH_SIZE)
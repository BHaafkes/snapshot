import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import date
from flask import Flask

app = Flask(__name__)

# Tables to take a snapshot of
SOURCE_TABLES = {
    "magic_formula_buys": "magic_formula_buys_track",
    "magic_formula_sells": "magic_formula_sells_track",
    "intelligent_investor_buys": "intelligent_investor_buys_track",
    "combined_model_buys": "combined_model_buys_track"
}

@app.route('/')
def run_snapshot():
    """
    Reads current recommendation tables and appends them to historical tracking tables.
    """
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL:
            raise ValueError("No DATABASE_URL secret found.")
        
        engine = create_engine(DATABASE_URL)
        today_str = date.today().strftime('%Y-%m-%d')
        
        print(f"➡️ Starting annual snapshot for {today_str}...")

        with engine.connect() as connection:
            for source, track in SOURCE_TABLES.items():
                try:
                    # Read the current recommendations
                    df = pd.read_sql_table(source, connection)
                    
                    if not df.empty:
                        # Add the snapshot date
                        df['snapshot_date'] = today_str
                        
                        # Append to the tracking table
                        df.to_sql(track, con=connection, if_exists='append', index=False)
                        print(f"   - ✅ Appended {len(df)} rows to {track}")
                    else:
                        print(f"   - ⚠️ Source table '{source}' was empty. Nothing to append.")

                except Exception as e:
                    print(f"   - ❌ Error processing table '{source}': {e}")

        return "Snapshot script completed successfully.", 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
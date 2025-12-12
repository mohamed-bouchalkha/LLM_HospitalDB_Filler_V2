import os
import json
import time
import mysql.connector
from google import genai
from google.genai import types
from dotenv import load_dotenv

# 1. Setup Client
load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Database Configuration
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'morocco_health_db'
}

def get_dirty_rows(batch_size=20):
    """Fetch rows where region is missing."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Select rows that need fixing
        query = f"SELECT id, city FROM places WHERE region IS NULL LIMIT {batch_size}"
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        return rows
    except mysql.connector.Error as err:
        print(f"❌ Database Error: {err}")
        return []

def clean_batch_with_gemini(rows, retry_count=3):
    """Send rows to Gemini and force strict JSON output with retry logic."""
    if not rows:
        return []

    input_data = json.dumps(rows, ensure_ascii=False)
    
    prompt = f"""You are a Data Engineer cleaning a Moroccan places database.

TASK:
1. Analyze each 'city' field carefully
2. If it's a messy address (e.g., "2 RUE 5 FES", "87 RUE CASABLANCA"), extract the actual city name
3. If it's garbage/invalid (e.g., "AIN KADOUS", random text), set action="DELETE"
4. If it's valid, provide the official 'region' and 'province' in French

MOROCCAN REGIONS (for reference):
- Tanger-Tétouan-Al Hoceïma
- Oriental
- Fès-Meknès
- Rabat-Salé-Kénitra
- Béni Mellal-Khénifra
- Casablanca-Settat
- Marrakech-Safi
- Drâa-Tafilalet
- Souss-Massa
- Guelmim-Oued Noun
- Laâyoune-Sakia El Hamra
- Dakhla-Oued Ed-Dahab

INPUT DATA:
{input_data}

OUTPUT FORMAT (strict JSON array):
[
  {{"id": 123, "city": "Fès", "region": "Fès-Meknès", "province": "Fès", "action": "UPDATE"}},
  {{"id": 124, "city": "GARBAGE_TEXT", "region": null, "province": null, "action": "DELETE"}}
]

IMPORTANT: Return ONLY the JSON array, no explanations."""

    for attempt in range(retry_count):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash-lite",  # Using latest model
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.1  # Lower temperature for more consistent results
                )
            )
            
            # Parse and validate the JSON response
            cleaned_data = json.loads(response.text)
            
            # Validate structure
            if not isinstance(cleaned_data, list):
                raise ValueError("Response is not a JSON array")
            
            # Validate each item has required fields
            for item in cleaned_data:
                if 'id' not in item or 'action' not in item:
                    raise ValueError(f"Invalid item structure: {item}")
            
            return cleaned_data
            
        except json.JSONDecodeError as e:
            print(f"⚠️ JSON Parse Error (attempt {attempt + 1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(2)  # Wait before retry
            else:
                print(f"❌ Failed to parse after {retry_count} attempts")
                print(f"Raw response: {response.text if 'response' in locals() else 'No response'}")
                
        except Exception as e:
            print(f"❌ Gemini API Error (attempt {attempt + 1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(2)
            else:
                return []
    
    return []

def apply_updates(cleaned_data):
    """Execute SQL updates with better error handling."""
    if not cleaned_data:
        return 0, 0
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        updated = 0
        deleted = 0
        errors = 0

        for item in cleaned_data:
            # Safety check
            if 'id' not in item or 'action' not in item:
                print(f"⚠️ Skipping invalid item: {item}")
                errors += 1
                continue

            try:
                if item['action'] == 'UPDATE':
                    sql = "UPDATE places SET city=%s, region=%s, province=%s WHERE id=%s"
                    val = (item.get('city'), item.get('region'), item.get('province'), item['id'])
                    cursor.execute(sql, val)
                    updated += cursor.rowcount
                    print(f"  ✓ Updated ID {item['id']}: {item.get('city')} → {item.get('region')}")
                    
                elif item['action'] == 'DELETE':
                    sql = "DELETE FROM places WHERE id=%s"
                    val = (item['id'],)
                    cursor.execute(sql, val)
                    deleted += cursor.rowcount
                    print(f"  ✗ Deleted ID {item['id']}: {item.get('city')} (invalid data)")
                    
            except mysql.connector.Error as err:
                print(f"⚠️ SQL Error on ID {item['id']}: {err}")
                errors += 1

        conn.commit()
        cursor.close()
        conn.close()
        
        if errors > 0:
            print(f"⚠️ Batch completed with {errors} errors")
        
        return updated, deleted
        
    except mysql.connector.Error as err:
        print(f"❌ Database Connection Error: {err}")
        return 0, 0

def get_total_dirty_count():
    """Get count of remaining dirty rows."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM places WHERE region IS NULL")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except:
        return 0

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Starting AI-Powered Database Cleaner (Gemini 2.0)")
    print("=" * 60)
    
    # Initial count
    initial_count = get_total_dirty_count()
    print(f"\n📊 Initial dirty rows: {initial_count}\n")
    
    if initial_count == 0:
        print("✨ Database is already clean!")
        exit(0)
    
    total_updated = 0
    total_deleted = 0
    batch_num = 0
    
    while True:
        batch_num += 1
        print(f"\n{'─' * 60}")
        print(f"📦 BATCH {batch_num}")
        print(f"{'─' * 60}")
        
        # 1. Fetch
        dirty_rows = get_dirty_rows(batch_size=20)
        
        if not dirty_rows:
            print("✨ All rows processed!")
            break
        
        remaining = get_total_dirty_count()
        print(f"📥 Processing {len(dirty_rows)} rows (Remaining: {remaining})")
        
        # Show preview
        print(f"\n🔍 Preview:")
        for row in dirty_rows[:3]:
            print(f"   ID {row['id']}: {row['city']}")
        if len(dirty_rows) > 3:
            print(f"   ... and {len(dirty_rows) - 3} more")
        
        # 2. Process with AI
        print(f"\n🤖 Sending to Gemini AI...")
        cleaned_json = clean_batch_with_gemini(dirty_rows)
        
        # 3. Update Database
        if cleaned_json:
            print(f"\n💾 Applying changes...")
            updated, deleted = apply_updates(cleaned_json)
            total_updated += updated
            total_deleted += deleted
            print(f"\n✅ Batch {batch_num} Complete: {updated} updated, {deleted} deleted")
        else:
            print("⚠️ Empty or invalid response from AI. Skipping batch...")
            time.sleep(3)  # Wait before next attempt
        
        # Rate limiting
        time.sleep(1)
    
    # Final Summary
    print("\n" + "=" * 60)
    print("🎉 CLEANING COMPLETE")
    print("=" * 60)
    print(f"📊 Statistics:")
    print(f"   • Initial dirty rows: {initial_count}")
    print(f"   • Rows updated: {total_updated}")
    print(f"   • Rows deleted: {total_deleted}")
    print(f"   • Batches processed: {batch_num}")
    print(f"   • Final dirty rows: {get_total_dirty_count()}")
    print("=" * 60)
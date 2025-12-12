import mysql.connector
from mysql.connector import Error

def execute_sql_file(sql_file_path, host, user, password, database):
    """
    Execute a SQL file containing multiple statements against a MySQL database.
    
    Args:
        sql_file_path: Path to the SQL file
        host: Database host
        user: Database username
        password: Database password
        database: Database name
    """
    conn = None
    cursor = None
    
    try:
        # Read the SQL file
        print(f"Reading SQL file: {sql_file_path}")
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Connect to database
        print(f"Connecting to database: {database}")
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            allow_local_infile=True
        )
        
        if conn.is_connected():
            print("Successfully connected to database")
            
            # Split by custom delimiter blocks
            statements = []
            current_stmt = []
            current_delimiter = ';'
            in_delimiter_block = False
            
            for line in sql_content.split('\n'):
                line_stripped = line.strip()
                
                # Check for delimiter change
                if line_stripped.upper().startswith('DELIMITER'):
                    # Save current statement if exists
                    if current_stmt:
                        statements.append(('\n'.join(current_stmt), in_delimiter_block))
                        current_stmt = []
                    # Update delimiter
                    new_delim = line_stripped.split()[-1]
                    in_delimiter_block = (new_delim != ';')
                    current_delimiter = new_delim
                    continue
                
                # Add line to current statement
                if line_stripped:
                    current_stmt.append(line)
                
                # Check if statement is complete
                if line_stripped.endswith(current_delimiter):
                    stmt = '\n'.join(current_stmt)
                    # Remove the delimiter from the statement
                    stmt = stmt.rstrip(current_delimiter).strip()
                    if stmt:
                        statements.append((stmt, in_delimiter_block))
                    current_stmt = []
            
            # Add final statement if exists
            if current_stmt:
                stmt = '\n'.join(current_stmt).strip()
                if stmt:
                    statements.append((stmt, False))
            
            # Execute each statement
            total = len(statements)
            success_count = 0
            error_count = 0
            
            print(f"\nExecuting {total} SQL statements...")
            print("-" * 60)
            
            for i, (stmt, is_proc_func) in enumerate(statements, 1):
                # Skip empty statements and comments
                if not stmt or stmt.startswith('--'):
                    continue
                
                try:
                    # Show progress
                    preview = stmt[:60].replace('\n', ' ')
                    stmt_type = "PROCEDURE/FUNCTION" if is_proc_func else "STATEMENT"
                    print(f"[{i}/{total}] {stmt_type}: {preview}...")
                    
                    # For procedures/functions, check if exists and drop first
                    if is_proc_func:
                        stmt_upper = stmt.upper()
                        if 'CREATE PROCEDURE' in stmt_upper:
                            proc_name = stmt.split('(')[0].split()[-1]
                            try:
                                cursor = conn.cursor()
                                cursor.execute(f"DROP PROCEDURE IF EXISTS {proc_name}")
                                conn.commit()
                                cursor.close()
                                print(f"  → Dropped existing procedure: {proc_name}")
                            except:
                                pass
                        elif 'CREATE FUNCTION' in stmt_upper:
                            func_name = stmt.split('(')[0].split()[-1]
                            try:
                                cursor = conn.cursor()
                                cursor.execute(f"DROP FUNCTION IF EXISTS {func_name}")
                                conn.commit()
                                cursor.close()
                                print(f"  → Dropped existing function: {func_name}")
                            except:
                                pass
                    
                    # Create new cursor for each statement
                    cursor = conn.cursor()
                    
                    # Execute statement
                    cursor.execute(stmt)
                    
                    # Consume all results (important for multi-result statements)
                    while cursor.nextset():
                        pass
                    
                    # Show results if it's a SELECT query
                    if stmt.strip().upper().startswith('SELECT'):
                        try:
                            results = cursor.fetchall()
                            if results:
                                print(f"  ✓ Returned {len(results)} rows")
                                # Show first few rows
                                for row in results[:5]:
                                    print(f"    {row}")
                                if len(results) > 5:
                                    print(f"    ... and {len(results) - 5} more rows")
                            else:
                                print("  ✓ No results")
                        except:
                            print("  ✓ Executed successfully")
                    else:
                        affected = cursor.rowcount
                        print(f"  ✓ Affected rows: {affected}")
                    
                    cursor.close()
                    conn.commit()
                    success_count += 1
                    
                except Error as e:
                    error_code = e.errno if hasattr(e, 'errno') else None
                    
                    # Ignore "table doesn't exist" on TRUNCATE (it will be created later)
                    if error_code == 1146 and 'TRUNCATE' in stmt.upper():
                        print(f"  ⚠ Warning: {e} (will be created later)")
                        success_count += 1
                    else:
                        error_count += 1
                        print(f"  ✗ Error: {e}")
                        if len(stmt) < 200:
                            print(f"  Statement: {stmt}")
                    
                    # Continue with next statement
                    if cursor:
                        cursor.close()
                    continue
            
            print("-" * 60)
            print(f"\nExecution complete!")
            print(f"✓ Successful: {success_count}")
            print(f"✗ Failed: {error_count}")
            
    except FileNotFoundError:
        print(f"Error: SQL file not found at {sql_file_path}")
    except Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Close connections
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("\nDatabase connection closed")


# Usage example
if __name__ == "__main__":
    SQL_FILE_PATH = 'cleaning/cleaning.sql'
    
    # Database configuration
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'morocco_health_db'
    }
    
    # Execute the SQL file
    execute_sql_file(
        sql_file_path=SQL_FILE_PATH,
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database']
    )
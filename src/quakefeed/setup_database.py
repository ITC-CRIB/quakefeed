import sqlite3

def setup_database(db_name):
    # Connect to SQLite database (creates the file if it doesn't exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # SQL command to create tables and triggers
    setup_sql = '''
    -- Create earthquakes table
    CREATE TABLE IF NOT EXISTS earthquakes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usgsid TEXT NOT NULL UNIQUE,
        date TEXT,
        time TEXT,
        latitude REAL,
        longitude REAL,
        depth REAL,
        magnitude REAL,
        maxpga REAL,
        location TEXT,
        url TEXT
    );

    -- Create changes_log table
    CREATE TABLE IF NOT EXISTS changes_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        earthquake_id INTEGER,
        processed INTEGER DEFAULT 0,
        FOREIGN KEY (earthquake_id) REFERENCES earthquakes (id)
    );

    -- Create trigger for logging new entries in earthquakes
    CREATE TRIGGER IF NOT EXISTS after_insert_earthquake
    AFTER INSERT ON earthquakes
    FOR EACH ROW
    BEGIN
        INSERT INTO changes_log (earthquake_id)
        VALUES (NEW.id);
    END;
    '''

    # Execute the SQL commands
    cursor.executescript(setup_sql)

    # Commit changes and close the connection
    conn.commit()
    conn.close()

    print(f'Database {db_name}, table "earthquakes", table "changes_log", and trigger "after_insert_earthquake" have been set up.')


    def delete_database(self):
        # Ensure no connections are open
        try:
            os.remove(self.db_name)
            print(f'Database {self.db_name} has been deleted.')
        except FileNotFoundError:
            print(f'Database {self.db_name} does not exist.')
        except PermissionError:
            print(f'Permission denied: Unable to delete {self.db_name}. Ensure the file is not open in another process.')
        except Exception as e:
            print(f'Error: {e}. Could not delete {self.db_name}.')

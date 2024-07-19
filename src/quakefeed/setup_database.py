import sqlite3
import os

class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name

    def setup_database(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

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

        cursor.executescript(setup_sql)
        conn.commit()
        conn.close()
        print(f'Database {self.db_name}, table "earthquakes", table "changes_log", and trigger "after_insert_earthquake" have been set up.')

    def delete_database(self):
        try:
            os.remove(self.db_name)
            print(f'Database {self.db_name} has been deleted.')
        except FileNotFoundError:
            print(f'Database {self.db_name} does not exist.')
        except PermissionError:
            print(f'Permission denied: Unable to delete {self.db_name}. Ensure the file is not open in another process.')
        except Exception as e:
            print(f'Error: {e}. Could not delete {self.db_name}.')

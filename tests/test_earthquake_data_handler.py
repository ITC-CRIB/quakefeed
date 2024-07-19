import unittest
import os
import sys
import pandas as pd
import sqlite3

# Assuming src_path is the path to your source directory where modules are located
src_path = os.path.abspath(os.path.join(os.getcwd(), '.', 'src'))
sys.path.append(src_path)

from quakefeed.setup_database import DatabaseManager
from quakefeed.earthquake_service import EarthquakeDataHandler

class TestEarthquakeDataHandler(unittest.TestCase):
    def setUp(self):
        self.db_name = 'test_earthquake_data.db'
        self.db_manager = DatabaseManager(self.db_name)
        self.data_handler = EarthquakeDataHandler(self.db_name)
        self.db_manager.setup_database()

    def tearDown(self):
        # Remove database file after each test
        if os.path.exists(self.db_name):
            os.remove(self.db_name)

    def test_insert_data_to_db(self):
        df = pd.DataFrame({
            'usgsid': ['usgs1', 'usgs2'],
            'date': ['2024-07-19', '2024-07-18'],
            'time': ['12:34:56', '11:22:33'],
            'latitude': [34.05, 36.16],
            'longitude': [-118.24, -115.15],
            'depth': [10.0, 12.5],
            'magnitude': [5.2, 4.8],
            'maxpga': [0.5, 0.3],
            'location': ['Los Angeles', 'Las Vegas'],
            'url': ['http://example.com/1', 'http://example.com/2']
        })

        self.data_handler.insert_data_to_db(df)

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM earthquakes")
        count = cursor.fetchone()[0]
        conn.close()

        self.assertEqual(count, 2)

    def test_process_changes(self):
        # Insert a record into earthquakes
        df = pd.DataFrame({
            'usgsid': ['usgs3'],
            'date': ['2024-07-17'],
            'time': ['10:11:12'],
            'latitude': [38.24],
            'longitude': [-120.43],
            'depth': [8.3],
            'magnitude': [6.1],
            'maxpga': [0.6],
            'location': ['San Francisco'],
            'url': ['http://example.com/3']
        })
        self.data_handler.insert_data_to_db(df)

        # Process changes
        self.data_handler.process_changes()

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM changes_log WHERE processed = 1")
        count = cursor.fetchone()[0]
        conn.close()

        self.assertEqual(count, 1)

    def test_view_last_item(self):
        df = pd.DataFrame({
            'usgsid': ['usgs4'],
            'date': ['2024-07-16'],
            'time': ['09:10:11'],
            'latitude': [39.29],
            'longitude': [-121.15],
            'depth': [7.0],
            'magnitude': [5.5],
            'maxpga': [0.4],
            'location': ['Sacramento'],
            'url': ['http://example.com/4']
        })
        self.data_handler.insert_data_to_db(df)

        # Capture output by redirecting stdout
        import io
        import sys
        old_stdout = sys.stdout
        new_stdout = io.StringIO()
        sys.stdout = new_stdout

        self.data_handler.view_last_item()

        sys.stdout = old_stdout
        output = new_stdout.getvalue()

        # Verify the output
        self.assertIn('Last item in earthquakes table:', output)
        self.assertIn('Last item in changes_log table:', output)

    def test_insert_duplicate_data(self):
        df = pd.DataFrame({
            'usgsid': ['usgs5'],
            'date': ['2024-07-15'],
            'time': ['08:09:10'],
            'latitude': [40.73],
            'longitude': [-73.93],
            'depth': [6.0],
            'magnitude': [5.0],
            'maxpga': [0.7],
            'location': ['New York'],
            'url': ['http://example.com/5']
        })

        # Insert the record
        self.data_handler.insert_data_to_db(df)

        # Attempt to insert the same record again
        self.data_handler.insert_data_to_db(df)

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM earthquakes")
        count = cursor.fetchone()[0]
        conn.close()

        # Verify that the record count remains 1 (no duplicate inserted)
        self.assertEqual(count, 1)

if __name__ == '__main__':
    unittest.main()

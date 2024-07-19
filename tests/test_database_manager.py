import unittest
import os
import sys

import sqlite3

src_path = os.path.abspath(os.path.join(os.getcwd(), '.', 'src'))
print(src_path)
sys.path.append(src_path)

from quakefeed.setup_database import DatabaseManager


class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        self.db_name = 'test_database.db'
        self.db_manager = DatabaseManager(self.db_name)

    def tearDown(self):
        if os.path.exists(self.db_name):
            os.remove(self.db_name)

    def test_setup_database(self):
        self.db_manager.setup_database()
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='earthquakes'")
        self.assertIsNotNone(cursor.fetchone())
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='changes_log'")
        self.assertIsNotNone(cursor.fetchone())
        cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name='after_insert_earthquake'")
        self.assertIsNotNone(cursor.fetchone())
        conn.close()

    def test_delete_database(self):
        self.db_manager.setup_database()
        self.db_manager.delete_database()
        self.assertFalse(os.path.exists(self.db_name))

if __name__ == '__main__':
    unittest.main()

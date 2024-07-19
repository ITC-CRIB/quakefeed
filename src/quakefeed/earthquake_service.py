import pandas as pd

import sqlite3
from sqlite3 import IntegrityError

from libcomcat.search import search, get_event_by_id
from libcomcat.dataframes import get_summary_data_frame


class EarthquakeService:
    def __init__(self, lat_min, lat_max, lon_min, lon_max, min_magnitude, event_type):
        self.lat_min = lat_min
        self.lat_max = lat_max
        self.lon_min = lon_min
        self.lon_max = lon_max
        self.min_magnitude = min_magnitude
        self.event_type = event_type

    def get_maxpga(self, quake_id):
        event = get_event_by_id(quake_id)
        product = event.getProducts('shakemap')[0]
        return product['maxpga']

    def search_earthquakes(self, start_time, end_time, limit=10):
        events = search(
            starttime=start_time,
            endtime=end_time,
            minlatitude=self.lat_min,
            maxlatitude=self.lat_max,
            minlongitude=self.lon_min,
            maxlongitude=self.lon_max,
            minmagnitude=self.min_magnitude,
            eventtype=self.event_type,
            producttype='shakemap',
            orderby='magnitude',
            limit=limit
        )
        return get_summary_data_frame(events)
        # returns: ['id', 'time', 'location', 'latitude', 'longitude', 'depth', 'magnitude',
        #           'alert', 'url', 'eventtype', 'significance']
    
    def add_maxpga_column(self, df):
        df['maxpga'] = df['id'].apply(self.get_maxpga)
        return df

    def format_dataframe(self, df):
        df['date'] = pd.to_datetime(df['time']).dt.date
        df['time'] = pd.to_datetime(df['time']).dt.time

        df['usgsid'] = df['id'].astype(str)
        df = df.drop(columns=['id'])

        column_order = [
            'usgsid', 
            'date', 
            'time', 
            'latitude', 
            'longitude', 
            'depth', 
            'magnitude', 
            'maxpga', 
            'location', 
            'url'
        ]
        # Return the DataFrame with columns in the specified order
        return df[column_order]


class EarthquakeDataHandler:
    def __init__(self, db_name):
        self.db_name = db_name

    def insert_data_to_db(self, df):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        try:
            # Get existing usgsid values from the database
            existing_usgsid_query = "SELECT usgsid FROM earthquakes"
            existing_usgsids = pd.read_sql(existing_usgsid_query, conn)['usgsid'].tolist()

            # Filter out records that already exist in the database
            new_records_df = df[~df['usgsid'].isin(existing_usgsids)]

            # Insert new records into the database
            if not new_records_df.empty:
                new_records_df.to_sql('earthquakes', conn, if_exists='append', index=False)
                print(f"Inserted {len(new_records_df)} new records into the earthquakes table.")
            else:
                print("No new records to insert.")

            conn.commit()

        except IntegrityError as e:
            print(f"IntegrityError occurred: {e}")
        finally:
            conn.close()

    def process_changes(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Fetch unprocessed changes with necessary data from earthquakes table
        query = '''
        SELECT cl.id, e.date, e.latitude, e.longitude
        FROM changes_log cl
        JOIN earthquakes e ON cl.earthquake_id = e.id
        WHERE cl.processed = 0
        '''
        cursor.execute(query)
        changes = cursor.fetchall()

        for change in changes:
            log_id, date, latitude, longitude = change
            # Perform your external operation
            print(f"Start downloading images for ({latitude}, {longitude}) and ({date})")

            # Mark the entry as processed
            cursor.execute("UPDATE changes_log SET processed = 1 WHERE id = ?", (log_id,))

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    def view_last_item(self):
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Fetch the last item from the earthquakes table
        cursor.execute('SELECT * FROM earthquakes ORDER BY id DESC LIMIT 1')
        last_earthquake = cursor.fetchone()
        if last_earthquake:
            print('Last item in earthquakes table:')
            print(last_earthquake)
        else:
            print('The earthquakes table is empty.')

        # Fetch the last item from the changes_log table
        cursor.execute('SELECT * FROM changes_log ORDER BY id DESC LIMIT 1')
        last_change_log = cursor.fetchone()
        if last_change_log:
            print('Last item in changes_log table:')
            print(last_change_log)
        else:
            print('The changes_log table is empty.')

        # Close the connection
        conn.close()
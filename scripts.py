import requests
import sqlite3
import logging
from datetime import datetime

# Configuration
TIMEZONE_DB_API_KEY = '8HBV06IADIB4'
DATABASE_FILE = 'timezone_data.db'

# Set up logging
logging.basicConfig(filename='error.log', level=logging.ERROR)

# Function to query TimezoneDB API
def query_timezone_db(endpoint, params):
    url = f'https://api.timezonedb.com/v2.1/list-time-zone'
    params['key'] = TIMEZONE_DB_API_KEY
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f'Error accessing API - {response.status_code}')
        return None

# Function to create database tables
def create_tables():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS TZDB_TIMEZONES (
                        id INTEGER PRIMARY KEY,
                        zoneName TEXT,
                        countryCode TEXT,
                        timestamp INTEGER
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS TZDB_ZONE_DETAILS (
                        id INTEGER PRIMARY KEY,
                        zoneName TEXT,
                        countryName TEXT,
                        countryCode TEXT,
                        timestamp INTEGER,
                        UNIQUE(zoneName, countryCode)
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS TZDB_ERROR_LOG (
                        id INTEGER PRIMARY KEY,
                        error TEXT,
                        timestamp TEXT
                    )''')
    conn.commit()
    conn.close()

# Function to populate TZDB_TIMEZONES table
def populate_timezones_table():
    data = query_timezone_db('list-time-zone', {})
    if data:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM TZDB_TIMEZONES')
        for zone in data['zones']:
            cursor.execute('INSERT INTO TZDB_TIMEZONES (zoneName, countryCode, timestamp) VALUES (?, ?, ?)',
                           (zone['zoneName'], zone['countryCode'], datetime.now().timestamp()))
        conn.commit()
        conn.close()

# Function to populate TZDB_ZONE_DETAILS table
def populate_zone_details_table():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('CREATE TEMP TABLE IF NOT EXISTS TZDB_ZONE_DETAILS_STAGE AS SELECT * FROM TZDB_ZONE_DETAILS WHERE 0')
    data = cursor.execute('SELECT zoneName, countryCode FROM TZDB_TIMEZONES').fetchall()
    for zone in data:
        params = {'zone': zone[0], 'country': zone[1]}
        existing_data = cursor.execute('SELECT * FROM TZDB_ZONE_DETAILS WHERE zoneName = ? AND countryCode = ?', zone).fetchone()
        if not existing_data:
            zone_details = query_timezone_db('get-time-zone', params)
            if zone_details:
                cursor.execute('INSERT INTO TZDB_ZONE_DETAILS_STAGE (zoneName, countryName, countryCode, timestamp) VALUES (?, ?, ?, ?)',
                               (zone_details['zoneName'], zone_details['countryName'], zone_details['countryCode'], datetime.now().timestamp()))
    cursor.execute('INSERT OR IGNORE INTO TZDB_ZONE_DETAILS SELECT * FROM TZDB_ZONE_DETAILS_STAGE')
    cursor.execute('DROP TABLE IF EXISTS TZDB_ZONE_DETAILS_STAGE')
    conn.commit()
    conn.close()

# Main function
def main():
    create_tables()
    populate_timezones_table()
    populate_zone_details_table()

if __name__ == '__main__':
    main()

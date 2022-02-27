#!/usr/bin/env python3
"""
Generates a bike share trip data database via a basic ETL pipeline.

Example usage as an executable:

    python indego_trip_etl.py indego-trips-2021-q2.csv

will generate a sqlite3 database file: trips.db

A full dataset can be found at:

https://u626n26h74f16ig1p3pt0f2g-wpengine.netdna-ssl.com/wp-content/uploads/2021/07/indego-trips-2021-q2.zip
"""
import csv
import datetime
import logging
import re
import sqlite3
import sys
from typing import Dict, Iterable, TextIO, Tuple

logger = logging.getLogger()

def create_db(name: str = "trips.db") -> sqlite3.Connection:
    """
    This function should:

    1. Create a new sqlite3 database using the supplied name
    2. Create a `trips` table to allow our indego bike data to be queryable via SQL
    3. Return a connection to the database

    Note: This function should be idempotent
    """
    db_conn = sqlite3.connect(name)
    cursor = db_conn.cursor()
    cursor.execute('''create table trips (trip_id integer, duration integer, start_time real,end_time real,start_station integer,start_lat real,start_lon real,end_station integer,end_lat real,end_lon real,bike_id integer,plan_duration integer,trip_route_category text,passholder_type text,bike_type text)''')
    return db_conn


def extract(file: TextIO) -> Iterable:
    """
    This function should:

    1. Accept a file-like object (Text I/O)
    2. Return an iterable value to be transformed
    """
    reader = csv.DictReader(file)
    return reader


def transform_row(row: Iterable) -> Iterable[Dict]:
    trans_dict = dict()

    for key, value in row.items():
      if re.search('^[0-9]+$',value):
        trans_dict[key] = int(value)
      elif re.search('^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}$',value):
        date = datetime.datetime.strptime(value,"%m/%d/%Y %H:%M")
        unixtime = datetime.datetime.timestamp(date)
        trans_dict[key] = unixtime
      elif re.search('^(\-)?[0-9]+\.[0-9]+$',value):
        trans_dict[key] = float(value)
      elif re.search('^[a-zA-Z\s]',value):
        trans_dict[key] = str(value)
      elif len(value) == 0:
        trans_dict[key] = None
      else:
        return {}

    return trans_dict

def transform(rows: Iterable) -> Iterable[Dict]:
    """
    This function should:

    1. Accept an iterable rows value to be transformed
    2. Transform any date time value into a POSIX timestamp
    3. Transform remaining fields into sqlite3 supported types
    4. Output to stdout or stderr if a row fails to be transformed
    5. Return an iterable collection of transformed rows as trip dictionaries to be loaded into our trips table
    """
    trips = []
    for row in rows:
      trip = transform_row(row)
      if len(trip) > 0:
        trips.append(trip)

    return trips


def load(trips: Iterable[Dict], conn: sqlite3.Connection):
    """
    This function should:

    1. Accept a collection of trip object data and a connection to the trip database
    2. Insert the trip records into the database
    """
    for trip in trips:
      columns = ', '.join(trip.keys())
      placeholders = ':'+', :'.join(trip.keys())
      query = 'insert into trips (%s) values (%s)' % (columns, placeholders)
      conn.execute(query, trip)
      conn.commit()


def main(fname) -> None:
    """Given an indego bike trip csv file, run our ETL process on it for further querying"""
    conn = create_db()
    with open(fname) as f:
        rows = extract(f)
        trip_objs = transform(rows)
        load(trip_objs, conn)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(fname=sys.argv[1]))
    except Exception as e:
        logger.exception(e)
        sys.exit(1)

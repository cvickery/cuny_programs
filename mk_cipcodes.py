#! /usr/local/bin/python3
""" Rebuild the cipcodes table from the most recent IPEDS csv file.
    TODO: this needs to be abandoned and replaced with the use of cuny_cip_code_tbl now that that
    table is being downloaded and updated weekly. But wait for that to be verified before
    proceeding.
"""
import csv
import psycopg

from pathlib import Path
from collections import namedtuple
from psycopg.rows import namedtuple_row


# Check that there is a valid table available from IPEDS
code_files = sorted(Path.glob(Path('ipeds'), '*.csv'))
code_file = code_files[-1]
try:
  num_lines = len(open(code_file).readlines())
  if num_lines < 1000:
    exit(f'cipcodes.py: ERROR: expecting > 1,000+ lines in {code_file.name}; got {num_lines}')
except FileNotFoundError as e:
  exit(e)

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    cursor.execute('drop table if exists cipcodes')
    cursor.execute('create table cipcodes (cip_code text primary key, cip_title text)')
    cols = None
    with open(code_files[-1]) as code_file:
      reader = csv.reader(code_file)
      for line in reader:
        if cols is None:
          cols = [col.lower().replace(' ', '').replace('/', '_') for col in line]
          Row = namedtuple('Row', cols)
        else:
          row = Row._make(line)
          cursor.execute('insert into cipcodes values (%s, %s)',
                         (row.cipcode.strip('="'), row.title))

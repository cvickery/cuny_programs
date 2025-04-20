#! /usr/local/bin/python3
"""Create a dict of nys_institutions."""

import psycopg

from psycopg.rows import namedtuple_row

conn = psycopg.connect('dbname=cuny_curriculum')
cursor = conn.cursor(row_factory=namedtuple_row)
cursor.execute("select * from nys_institutions")
known_institutions = dict()
for row in cursor:
  known_institutions[row.id] = (row.institution_id, row.institution_name, row.is_cuny)

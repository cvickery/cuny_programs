#! /usr/local/bin/python3
""" Use the latest list of students per active term per program to build the ra_counts table for
    limiting which active blocks get processed by the course mapper.
"""

import csv
import os
import psycopg
import sys
import time

from collections import namedtuple
from pathlib import Path
from psycopg.rows import namedtuple_row

if __name__ == '__main__':
  # Get the latest list of active program requirement_blocks from OAREDA
  start = time.time()
  latest_active = None
  for active in Path('./archives').glob('*active*'):
    if latest_active is None or active.stat().st_mtime > latest_active.stat().st_mtime:
      latest_active = active
  if latest_active is None:
    exit('No active_requirements files found')

  # (Re-)create the table of students per active term for active requirement blocks.
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      cursor.execute("""
      drop table if exists ra_counts;

      create table ra_counts (
      institution text,
      requirement_id text,
      active_term integer,
      total_students integer,
      foreign key (institution, requirement_id) references requirement_blocks,
      primary key (institution, requirement_id, active_term));
      """)
      cursor.execute("""
      select institution, requirement_id
        from requirement_blocks
       where period_stop ~* '^9'
       """)
      active_blocks = [(row.institution, row.requirement_id) for row in cursor]

      with open(latest_active, newline='') as csv_file:
        reader = csv.reader(csv_file, delimiter='|')
        for line in reader:
          if reader.line_num == 1:
            Row = namedtuple('Row', ' '.join(col.lower().replace(' ', '_') for col in line))
          else:
            row = Row._make(line)
            if (row.institution, row.dap_req_id) in active_blocks:
              cursor.execute("""
              insert into ra_counts values(%s, %s, %s, %s)
              """, [row.institution,
                    row.dap_req_id,
                    int(row.dap_active_term.strip('U')),
                    int(row.distinct_students)])
seconds = int(round(time.time() - start))
mins, secs = divmod(seconds, 60)
print(f'  {mins} min {secs} sec')

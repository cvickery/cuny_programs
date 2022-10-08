#! /usr/local/bin/python3
""" Use the latest list of students per active term per program to build the ra_counts table for
    limiting which active blocks get processed by the course mapper.

    Terminology:
      Current blocks are in dap_req_block with a period_stop value that starts with '9'
      Active blocks are for programs/subprograms for which students can enroll
"""

import csv
import datetime
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
  file_date = datetime.date.fromtimestamp(latest_active.stat().st_mtime)
  print(f'Using {latest_active.name} {file_date}')

  # (Re-)create the table of students per active term for active requirement blocks.
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:

      cursor.execute("""
      drop table if exists ra_counts;

      create table ra_counts (
      institution text,
      requirement_id text,
      block_type text,
      block_value text,
      active_term integer,
      total_students integer,
      foreign key (institution, requirement_id) references requirement_blocks,
      primary key (institution, requirement_id, active_term));
      """)

      # Create dict of current blocks, giving their their block types/values
      cursor.execute("""
      select institution, requirement_id, block_type, block_value
        from requirement_blocks
       where period_stop ~* '^9'
       """)
      print(f'{cursor.rowcount:,} current blocks')
      current_blocks = {(row.institution, row.requirement_id): (row.block_type, row.block_value)
                        for row in cursor.fetchall()}

      # The OAREDA list includes gives the enrollments for each requirement block for each term
      with open(latest_active, newline='') as csv_file:
        reader = csv.reader(csv_file, delimiter='|')
        for line in reader:
          if reader.line_num == 1:
            Row = namedtuple('Row', ' '.join(col.lower().replace(' ', '_') for col in line))
          else:
            row = Row._make(line)
            if (row.institution, row.dap_req_id) in current_blocks:
              block_type, block_value = current_blocks[(row.institution, row.dap_req_id)]
              cursor.execute("""
              insert into ra_counts values(%s, %s, %s, %s, %s, %s)
              """, [row.institution,
                    row.dap_req_id,
                    block_type,
                    block_value,
                    int(row.dap_active_term.strip('U')),
                    int(row.distinct_students)])
seconds = int(round(time.time() - start))
mins, secs = divmod(seconds, 60)
print(f'  {mins} min {secs} sec')

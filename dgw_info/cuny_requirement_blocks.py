#! /usr/local/bin/python3

""" Insert or update the cuny_programs.requirement_blocks table from a cuny-wide extract (includes
    an institution column in addition to the DegreeWorks DAP_REQ_BLOCK columns.)

    2019-11-10
    Accept requirement block exports in either csv or xml format.

    2019-07-26
    This version works with the CUNY-wide dgw_dap_req_block table maintained by OIRA, instead of the
    separate tables used in requirement_blocks.py version (which supports only csv input).

    CUNY Institutions Not In DegreeWorks
    GRD01 | The Graduate Center
    LAW01 | CUNY School of Law
    MED01 | CUNY School of Medicine
    SOJ01 | Graduate School of Journalism
    SPH01 | School of Public Health

    Map DGW college codes to CF college codes
    BB BAR01 | Baruch College
    BC BKL01 | Brooklyn College
    BM BMC01 | Borough of Manhattan CC
    BX BCC01 | Bronx Community College
    CC CTY01 | City College
    HC HTR01 | Hunter College
    HO HOS01 | Hostos Community College
    JJ JJC01 | John Jay College
    KB KCC01 | Kingsborough Community College
    LC LEH01 | Lehman College
    LG LAG01 | LaGuardia Community College
    LU SLU01 | School of Labor & Urban Studies
    ME MEC01 | Medgar Evers College
    NC NCC01 | Guttman Community College
    NY NYT01 | NYC College of Technology
    QB QCC01 | Queensborough Community College
    QC QNS01 | Queens College
    SI CSI01 | College of Staten Island
    SP SPS01 | School of Professional Studies
    YC YRK01 | York College
"""

import os
import re
import sys
import csv
import argparse

from pathlib import Path
from datetime import datetime, timezone
from collections import namedtuple
from scribe_to_html import to_html
from types import SimpleNamespace

from xml.etree.ElementTree import parse

from pgconnection import PgConnection

from dgw_filter import dgw_filter

csv.field_size_limit(sys.maxsize)

trans_dict = dict()
for c in range(14, 31):
  trans_dict[c] = None

cruft_table = str.maketrans(trans_dict)


# decruft()
# -------------------------------------------------------------------------------------------------
def decruft(block):
  """ Remove chars in the range 0x0e through 0x1f and returns the block otherwise unchanged.
      This is the same thing strip_file does, which has to be run before this program for xml
      files. But for csv files where strip_files wasn't run, this makes the text cleaner, avoiding
      possible parsing problems.
  """
  return_block = block.translate(cruft_table)

  # Replace tabs with spaces, and primes with u2018.
  return_block = return_block.replace('\t', ' ').replace("'", '’')

  # Remove all text following END. that needs/wants never to be seen, and which messes up parsing
  # anyway.
  return_block = re.sub(r'[Ee][Nn][Dd]\.(.|\n)*', 'END.\n', return_block)

  return return_block


# csv_generator()
# -------------------------------------------------------------------------------------------------
def csv_generator(file):
  """ Generate rows from a csv export of OIRA’s DAP_REQ_BLOCK table.
  """
  cols = None
  with open(file, newline='') as query_file:
    reader = csv.reader(query_file,
                        delimiter=args.delimiter,
                        quotechar=args.quotechar)
    for line in reader:
      if cols is None:
        cols = [col.lower().replace(' ', '_') for col in line]
        Row = namedtuple('Row', cols)
      else:
        try:
          # Trim trailing whitespace from lines in the Scribe text; they were messing up checks for
          # changes to the blocks.
          row = Row._make(line)._asdict()
          requirement_text = row['requirement_text']
          row['requirement_text'] = '\n'.join([scribe_line.rstrip()
                                               for scribe_line in requirement_text.split('\n')])
          row = Row._make(row.values())
          yield row
        except TypeError as type_error:
          sys.exit(f'{type_error}: |{line}|')


# xml_generator()
# -------------------------------------------------------------------------------------------------
def xml_generator(file):
  """ Generate rows from an xml export of OIRA’s DAP_REQ_BLOCK table.
  """
  try:
    tree = parse(file)
  except xml.etree.ElementTree.ParseError as pe:
    sys.exit(pe)

  Row = None
  for record in tree.findall("ROW"):
    cols = record.findall('COLUMN')
    line = [col.text for col in cols]
    if Row is None:
      # array = [col.attrib['NAME'].lower() for col in cols]
      Row = namedtuple('Row', [col.attrib['NAME'].lower() for col in cols])
    row = Row._make(line)
    yield row


# __main__()
# -------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--debug', action='store_true', default=False)
parser.add_argument('-v', '--verbose', action='store_true', default=False)
parser.add_argument('-f', '--file', default='./downloads/dgw_dap_req_block.csv')
parser.add_argument('-de', '--delimiter', default=',')
parser.add_argument('-q', '--quotechar', default='"')
args = parser.parse_args()

# These are the columns that get initialized here. See cursor.create table for full list of columns.
db_cols = ['institution',
           'requirement_id',
           'block_type',
           'block_value',
           'title',
           'period_start',
           'period_stop',
           'school',
           'degree',
           'college',
           'major1',
           'major2',
           'concentration',
           'minor',
           'liberal_learning',
           'specialization',
           'program',
           'student_id',
           'requirement_text',
           'requirement_html',
           'hexdigest']
vals = '%s, ' * len(db_cols)
vals = '(' + vals.strip(', ') + ')'

DB_Record = namedtuple('DB_Record', db_cols)

conn = PgConnection()
cursor = conn.cursor()

# Dict of rows by institution
institutions = {}
Institution = namedtuple('Institution', 'load_date rows')

file = Path(args.file)
if not file.exists():
  # Try the latest archived version
  archives_dir = Path('/Users/vickery/Projects/CUNY_Programs/dgw_info/archives')
  archives = archives_dir.glob('dgw_dap_req_block*.csv')
  latest = None
  for archive in archives:
    if latest is None or archive.stat().st_mtime > latest.stat().st_mtime:
      latest = archive
  if latest is None:
    sys.exit(f'{file} does not exist, and no archive found')
  file = latest

if file.suffix.lower() == '.xml':
  generator = xml_generator
elif file.suffix.lower() == '.csv':
  generator = csv_generator
else:
  sys.exit(f'Unsupported file type: {file.suffix}')

# Gather all the rows for all the institutions
for row in generator(file):
  institution = row.institution.upper()

  # Integrity check: all rows for an institution must have the same load date.
  load_date = row.irdw_load_date[0:10]

  if institution not in institutions.keys():
    institutions[institution] = Institution._make([load_date, []])
  assert load_date == institutions[institution].load_date, \
      f'{load_date} is not {institutions[institution].load_date} for {institution}'

  institutions[institution].rows.append(row)

#  Schema for the table
#    create table requirement_blocks (
#    institution       text   not null,
#    requirement_id    text   not null,
#    block_type        text,
#    block_value       text,
#    title             text,
#    period_start      text,
#    period_stop       text,
#    school            text,
#    degree            text,
#    college           text,
#    major1            text,
#    major2            text,
#    concentration     text,
#    minor             text,
#    liberal_learning  text,
#    specialization    text,
#    program           text,
#    parse_status      text,
#    parse_date        date,
#    parse_who         integer,
#    parse_what        text,
#    lock_version      text,
#    requirement_text  text,
#    load_date         date,
#    -- Added Values
#    requirement_html  text default 'Not Available'::text,
#    parse_tree        jsonb default '{}'::jsonb,
#    hexdigest         text,
#
#    PRIMARY KEY (institution, requirement_id))""")

# Process the rows from the csv or xml file, institution by institution
for institution in institutions.keys():
  print(institution, file=sys.stderr)
  load_date = institutions[institution].load_date
  # Desired date format: YYYY-MM-DD
  if re.match(r'^\d{4}-\d{2}-\d{2}$', load_date):
    pass
  # Alternate format: DD-MMM-YY
  elif re.match(r'\d{2}-[a-z]{3}-\d{2}', load_date, re.I):
    load_date = datetime.strptime(load_date, '%d-%b-%y').strftime('%Y-%m-%d')
  else:
    sys.exit(f'Unrecognized load date format: {load_date}')

  num_records = len(institutions[institution].rows)
  suffix = '' if num_records == 1 else 's'
  if args.verbose:
    print(f'Examining {num_records:5,} record{suffix} dated {load_date} '
          f'from {file} for {institution}')

  # Insert new rows; update changed rows. Ignore other rows.
  num_changed = 0
  for row in institutions[institution].rows:
    decrufted = decruft(row.requirement_text)
    hexdigest = md5(decrufted.encode('utf-8')).hexdigest()
    cursor.execute(f"""
    select requirement_text, hexdigest from requirement_blocks
     where institution = '{row.institution}'
       and requirement_id = '{row.requirement_id}'
       and period_stop ~* '9999'
    """)
    if cursor.rowcount == 0:
      print(f'{row.institution} {row.requirement_id} is NEW')
    else:
      assert cursor.rowcount == 1
      db_row = cursor.fetchone()
      # if db_row.hexdigest == hexdigest:
      #   print(f'{row.institution} {row.requirement_id} is NOT changed')
      # else:
      #   print(f'{row.institution} {row.requirement_id} IS changed')
      #   num_changed += 1
      #   with open(f'diffs/{row.institution}_{row.requirement_id}_new', 'w') as _new:
      #     print(decrufted, file=_new)
      #   with open(f'diffs/{row.institution}_{row.requirement_id}_old', 'w') as _old:
      #     print(decruft(db_row.requirement_text), file=_old)

    db_record = DB_Record._make([institution,
                                 row.requirement_id,
                                 row.block_type,
                                 row.block_value,
                                 decruft(row.title),
                                 row.period_start,
                                 row.period_stop,
                                 row.school,
                                 row.degree,
                                 row.college,
                                 row.major1,
                                 row.major2,
                                 row.concentration,
                                 row.minor,
                                 row.liberal_learning,
                                 row.specialization,
                                 row.program,
                                 row.student_id,
                                 decruft(row.requirement_text),
                                 to_html(row.requirement_text),
                                 hexdigest])

    vals = ', '.join([f"'{val}'" for val in db_record])
    # cursor.execute(f'insert into requirement_blocks ({",".join(db_cols)}) values ({vals})')
    cursor.execute(f"""
    update requirement_blocks set requirement_text='{decruft(row.requirement_text)}',
                                  requirement_html='{to_html(row)}',
                                  hexdigest='{hexdigest}'
     where institution = '{institution}'
       and requirement_id = '{row.requirement_id}'
    """)
cursor.execute(f"""update updates
                      set update_date = '{load_date}'
                    where table_name = 'requirement_blocks'""")
exit()
conn.commit()
conn.close()

# Archive the file just processed, unless it's already there
if file.parent.name != 'archives':
  file = file.rename(f'/Users/vickery/Projects/cuny_programs/dgw_info/archives/'
                     f'{file.stem}_{load_date}{file.suffix}')
  # Be sure the file modification time matches the load_date
mtime = datetime.fromisoformat(load_date).timestamp()
os.utime(file, (mtime, mtime))

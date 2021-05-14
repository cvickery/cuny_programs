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
from xml.etree.ElementTree import parse

from pgconnection import PgConnection

from dgw_filter import dgw_filter

# Dict of known institution names
conn = PgConnection()
cursor = conn.cursor()
cursor.execute('select code, name from cuny_institutions')
institution_names = {row.code: row.name for row in cursor.fetchall()}
conn.close()

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


# catalog_years()
# -------------------------------------------------------------------------------------------------
def catalog_years(period_start: str, period_stop: str) -> str:
  """ Metadata for "bulletin years": first year, last year and whether undergraduate or graduate
      period_start and period_end are supposed to look like YYYY-YYYY[UG], with the special value
      of '99999999' for period_end indicating the current catalog year.
      The earliest observed valid catalog year was 1960-1964, but note that it isn't a single
      academic year.
  """
  is_undergraduate = 'U' in period_start
  is_graduate = 'G' in period_start
  if is_undergraduate and not is_graduate:
    catalog_type = 'Undergraduate'
  elif not is_undergraduate and is_graduate:
    catalog_type = 'Graduate'
  else:
    catalog_type = 'Unknown'

  try:
    first = period_start.replace('-', '')[0:4]
    if int(first) < 1960:
      raise ValueError()
  except ValueError:
    first = 'Unknown-Start-Year'

  if period_stop == '99999999':
    last = 'Now'
  else:
    try:
      last = period_stop.replace('-', '')[4:8]
      if int(last) < 1960:
        raise ValueError()
    except ValueError:
      last = 'Unknown-End-Year'
  return (catalog_type, first, last, f'{first} through {last}')


# to_html()
# -------------------------------------------------------------------------------------------------
def to_html(row, with_line_nums=False):
  lines_pre = ''
  if with_line_nums:
    # Add line numbers to requirements text for development purposes.
    num_lines = row.requirement_text.count('\n')
    lines_pre = '<pre class="line-numbers">'
    for line in range(num_lines):
      lines_pre += f'{line + 1:03d}  \n'
    lines_pre += '</pre>'

  catalog_type, first_year, last_year, catalog_years_text = catalog_years(row.period_start,
                                                                          row.period_stop)
  institution_name = institution_names[row.institution]
  requirement_text = dgw_filter(row.requirement_text)
  html = f"""

<h1>{institution_name} {row.requirement_id}: <em>{row.title}</em></h1>
<p>Requirements for {catalog_type} Catalog Years {catalog_years_text}
</p>
<section>
  <h1 class="closer">Degreeworks Code</h1>
  <div>
    <hr>
    <section class=with-numbers>
      {lines_pre}
      <pre>{requirement_text.replace('<', '&lt;')}</pre>
    </section
  </div>
</section>
"""

  return html.replace('\t', ' ').replace("'", '’')


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
          row = Row._make(line)
          yield row
        except TypeError as type_error:
          print(f'{type_error}: |{line}|', file=sys.stderr)


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
           'requirement_html']
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
  archives_dir = Path('/Users/vickery/CUNY_Programs/dgw_info/archives')
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

# Recreate the requirement_blocks table
cursor.execute("""drop table if exists requirement_blocks cascade;
                  create table requirement_blocks (
                  institution text,
                  requirement_id text,
                  block_type text,
                  block_value text,
                  title text,
                  period_start text,
                  period_stop text,
                  school text,
                  degree text,
                  college text,
                  major1 text,
                  major2 text,
                  concentration text,
                  minor text,
                  liberal_learning text,
                  specialization text,
                  program text,
                  student_id text,
                  requirement_text text,
                  requirement_html text default 'Not Available',
                  header_list jsonb default '[]'::jsonb,
                  body_list jsonb default '[]'::jsonb,
                  primary key (institution, requirement_id))""")

# Add the view, which omits the requirement_text, requirement_html, and object lists.
cursor.execute("""
drop view if exists view_requirement_blocks;
create view view_requirement_blocks as (
  select  institution,
           requirement_id,
           block_type,
           block_value,
           title,
           period_start,
           period_stop,
           school,
           degree,
           college,
           major1,
           major2,
           concentration,
           minor,
           liberal_learning,
           specialization,
           program
  from requirement_blocks
  order by institution, requirement_id, block_type, block_value, period_stop);
""")

# Process the rows from the csv or xml file, institution by institution
for institution in institutions.keys():
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
    print(f'Inserting {num_records:5,} record{suffix} dated {load_date} '
          f'from {file} for {institution}')

  # Insert the csv rows into the db after decrufting the requirement_text.
  for row in institutions[institution].rows:
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
                                 to_html(row)])

    vals = ', '.join([f"'{val}'" for val in db_record])
    cursor.execute(f'insert into requirement_blocks values ({vals})')
cursor.execute(f"""update updates
                      set update_date = '{load_date}'
                    where table_name = 'requirement_blocks'""")
conn.commit()
conn.close()

# Archive the file just processed, unless it's already there
if file.parent.name != 'archives':
  file = file.rename(f'/Users/vickery/Projects/cuny_programs/dgw_info/archives/'
                     f'{file.stem}_{load_date}{file.suffix}')
  # Be sure the file modification time matches the load_date
mtime = datetime.fromisoformat(load_date).timestamp()
os.utime(file, (mtime, mtime))

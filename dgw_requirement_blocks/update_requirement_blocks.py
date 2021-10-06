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

    This is a map of DGW college codes to CF college codes
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

import argparse
import csv
import datetime
import difflib
import json
import os
import re
import sys
import time

from collections import namedtuple
from pathlib import Path
from subprocess import run
from types import SimpleNamespace
from xml.etree.ElementTree import parse

from pgconnection import PgConnection
from dgw_parser import dgw_parser

from scribe_to_html import to_html

DEBUG = os.getenv('DEBUG_REQUIREMENT_BLOCKS')

csv.field_size_limit(sys.maxsize)

trans_dict = dict()
for c in range(14, 31):
  trans_dict[c] = None

cruft_table = str.maketrans(trans_dict)


class Action:
  def __init__(self):
    self.do_insert = False
    self.do_update = False


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
          # Trim trailing whitespace from lines in the Scribe text; they were messing up checking
          # for changes to the blocks at one point.
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
parser.add_argument('-d', '--debug', action='store_true')
parser.add_argument('-f', '--file', default='./downloads/dgw_dap_req_block.csv')
parser.add_argument('-p', '--parse', action='store_true')
parser.add_argument('--log_unchanged', action='store_true')
parser.add_argument('--skip_tumbleweed', action='store_true')
parser.add_argument('--delimiter', default=',')
parser.add_argument('--quotechar', default='"')
parser.add_argument('--timelimit', default='60')
args = parser.parse_args()

if args.debug:
  DEBUG = True

hostname = os.uname().nodename
print(f'Updating requirement blocks on {hostname}')
if hostname.lower().endswith('cuny.edu'):
  if not args.skip_tumbleweed:
    print('Get latest requirement blocks from Tumbleweed')
    update_result = run(['./update_requirement_blocks.sh'], stdout=sys.stdout, stderr=sys.stderr)
    if update_result.returncode != 0:
      print('Tumbleweed download FAILED.')


db_cols = ['institution', 'requirement_id', 'block_type', 'block_value', 'title', 'period_start',
           'period_stop', 'school', 'degree', 'college', 'major1', 'major2', 'concentration',
           'minor', 'liberal_learning', 'specialization', 'program', 'parse_status', 'parse_date',
           'parse_who', 'parse_what', 'lock_version', 'requirement_text', 'requirement_html',
           'parse_tree', 'irdw_load_date']
vals = '%s, ' * len(db_cols)
vals = '(' + vals.strip(', ') + ')'

DB_Record = namedtuple('DB_Record', db_cols)

conn = PgConnection()
cursor = conn.cursor()

file = Path(args.file)
if not file.exists():
  # Try the latest archived version
  archives_dir = Path('/Users/vickery/Projects/CUNY_Programs/dgw_requirement_blocks/archives')
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

start_time = int(time.time())
empty_parse_tree = json.dumps({})
irdw_load_date = None
num_inserted = num_updated = 0

# Process the dgw_dap_req_block file
print(f'Using {file}')
for new_row in generator(file):

  # Integrity check: all rows must have the same irdw load date.
  # Desired date format: YYYY-MM-DD
  load_date = new_row.irdw_load_date[0:10]
  if re.match(r'^\d{4}-\d{2}-\d{2}$', load_date):
    load_date = datetime.date.fromisoformat(load_date)
  # Alternate format: DD-MMM-YY
  elif re.match(r'\d{2}-[a-z]{3}-\d{2}', load_date, re.I):
    dt = datetime.strptime(load_date, '%d-%b-%y').strftime('%Y-%m-%d')
    load_date = datetime.date(dt.year, dt.month, dt.day)
  else:
    sys.exit(f'Unrecognized load date format: {load_date}')
  if irdw_load_date is None:
    irdw_load_date = load_date
  if irdw_load_date != load_date:
    sys.exit(f'dap_req_block irdw_load_date ({load_date}) is not “{irdw_load_date}”'
             f'for {row.institution} {row.requirement_id}')
  log_file = open(f'./Logs/update_requirement_blocks_{irdw_load_date}.log', 'a')

  """ Determine the action to take.
        If args.parse, generate a new parse_tree, and update or insert as the case may be
        If this is a new block, do insert
        If this is an exisitng block and it has changed, do update
        During development, if block exists, has not changed, but parse_date has changed, report it.
  """
  action = Action()
  requirement_text = decruft(new_row.requirement_text)
  requirement_html = to_html(requirement_text)
  parse_date = datetime.date.fromisoformat(new_row.parse_date)

  cursor.execute(f"""
  select parse_date, requirement_text from requirement_blocks
   where institution = '{new_row.institution}'
     and requirement_id = '{new_row.requirement_id}'
  """)
  if cursor.rowcount == 0:
    action.do_insert = True
  else:
    assert cursor.rowcount == 1, (f'Error: {cursor.rowcount} rows for {institution} '
                                  f'{requirement_id}')
    db_row = cursor.fetchone()
    days_ago = (parse_date - db_row.parse_date).days
    suffix = '' if days_ago == 1 else 's'
    diff_msg = f'{days_ago} day{suffix} since previous parse date'

    if db_row.requirement_text != requirement_text:
      action.do_update = True
      with open(f'history/{new_row.institution}_{new_row.requirement_id}_{parse_date}-{days_ago}',
                'w') as _diff_file:
        diff_lines = difflib.context_diff([f'{line}\n' for line in
                                           db_row.requirement_text.split('\n')],
                                          [f'{line}\n' for line in
                                           requirement_text.split('\n')],
                                          fromfile='previous', tofile='changed', n=0)
        _diff_file.writelines(diff_lines)

  if action.do_insert:

    db_record = DB_Record._make([new_row.institution,
                                 new_row.requirement_id,
                                 new_row.block_type,
                                 new_row.block_value,
                                 decruft(new_row.title),
                                 new_row.period_start,
                                 new_row.period_stop,
                                 new_row.school,
                                 new_row.degree,
                                 new_row.college,
                                 new_row.major1,
                                 new_row.major2,
                                 new_row.concentration,
                                 new_row.minor,
                                 new_row.liberal_learning,
                                 new_row.specialization,
                                 new_row.program,
                                 new_row.parse_status,
                                 parse_date,
                                 new_row.parse_who,
                                 new_row.parse_what,
                                 new_row.lock_version,
                                 requirement_text,
                                 requirement_html,
                                 empty_parse_tree,
                                 irdw_load_date])

    vals = ', '.join([f"'{val}'" for val in db_record])
    cursor.execute(f'insert into requirement_blocks ({",".join(db_cols)}) values ({vals})')
    assert cursor.rowcount == 1, (f'Inserted {cursor.rowcount} rows\n{cursor.query}')
    print(f'Inserted  {new_row.institution} {new_row.requirement_id} {new_row.block_type} '
          f'{new_row.block_value} {new_row.period_stop}.', file=log_file)
    conn.commit()
    num_inserted += 1

  elif action.do_update:
    # Things that might have changed
    update_dict = {'period_stop': new_row.period_stop,
                   'parse_status': new_row.parse_status,
                   'parse_date': parse_date,
                   'parse_who': new_row.parse_who,
                   'parse_what': new_row.parse_what,
                   'lock_version': new_row.lock_version,
                   'requirement_text': requirement_text,
                   'requirement_html': requirement_html,
                   'parse_tree': empty_parse_tree,
                   'irdw_load_date': irdw_load_date,
                   }
    set_args = ','.join([f'{key}=%s' for key in update_dict.keys()])
    cursor.execute(f"""
    update requirement_blocks set {set_args}
     where institution = %s and requirement_id = %s
    """, ([v for v in update_dict.values()] + [new_row.institution, new_row.requirement_id]))
    assert cursor.rowcount == 1, (f'Updated {cursor.rowcount} rows\n{cursor.query}')
    print(f'Updated   {new_row.institution} {new_row.requirement_id} {new_row.block_type} '
          f'{new_row.block_value} {new_row.period_stop}: {diff_msg}.', file=log_file)
    conn.commit()
    num_updated += 1

  else:
    if args.log_unchanged:
      print(f'No change {new_row.institution} {new_row.requirement_id} {new_row.block_type} '
            f'{new_row.block_value}.', file=log_file)

  if args.parse and (action.do_insert or action.do_update)\
     and new_row.block_type in ['CONC', 'MAJOR', 'MINOR'] \
     and new_row.period_stop.startswith('9'):
    parse_error = ' OK'
    parse_tree = dgw_parser(new_row.institution, requirement_id=new_row.requirement_id,
                            timelimit=int(args.timelimit))
    if 'error' in parse_tree.keys():
      parse_error = ': ' + parse_tree['error']
    print(f'Parsed    {new_row.institution} {new_row.requirement_id} {new_row.block_type} '
          f'{new_row.block_value} {new_row.period_stop}{parse_error}.', file=log_file)

cursor.execute(f"""update updates
                      set update_date = '{load_date}', file_name = '{file.name}'
                    where table_name = 'requirement_blocks'""")
conn.commit()
conn.close()

# Archive the file just processed, unless it's already there
if file.parent.name != 'archives':
  file = file.rename(f'/Users/vickery/Projects/cuny_programs/dgw_requirement_blocks/archives/'
                     f'{file.stem}_{load_date}{file.suffix}')

# Be sure the file modification time matches the load_date
mtime = time.mktime(irdw_load_date.timetuple())
os.utime(file, (mtime, mtime))

# Regenerate program CSV and HTML files
if num_updated + num_inserted > 0:
  h, m = divmod((int(time.time()) - start_time), 3600)
  m, s = divmod(m, 60)
  print(f'Elapsed time: {h}:{m:02}:{s:02}\n')
  print(f'Inserted: {num_inserted}\nUpdated: {num_updated}\nRegenerating CSV and HTML')
  run(['../generate_html.py'], stdout=sys.stdout, stderr=sys.stderr)
else:
  print('No updated or new blocks found')

h, m = divmod((int(time.time()) - start_time), 3600)
m, s = divmod(m, 60)
print(f'Total time: {h}:{m:02}:{s:02}\n')

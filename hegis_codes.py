#! /usr/local/bin/python3

from datetime import datetime
from AdvancedHTMLParser import AdvancedHTMLParser
import requests
import socket

from pgconnection import PgConnection
from sendemail import send_message

# Be sure the NYSED website is accessible before proceeding.
try:
  r = requests.get('http://nysed.gov/college-university-evaluation/'
                   'new-york-state-taxonomy-academic-programs-hegis-codes').text
except requests.exceptions.ConnectionError as err:
  send_message([{'name': 'Christopher Vickery', 'email': 'cvickery@qc.cuny.edu'}],
               {'name': 'Transfer App', 'email': 'cvickery@qc.cuny.edu'},
               f'HEGIS Code Update Failed on {socket.gethostname()}',
               f'<p>{err}</p>')
  exit(f'HEGIS Code Update Failed on {socket.gethostname()}: <p>{err}</p>')

parser = AdvancedHTMLParser()
parser.parseStr(r)

tables = parser.getElementsByTagName('table')
# There are ten areas as of March 2020. If there are fewer than six consider it an error and do not
# continue.
if len(tables) < 6:
  exit(f'hegis_codes.py: ERROR: Expected at least six tables; got {len(tables)}.')

conn = PgConnection()
cursor = conn.cursor()
cursor.execute('drop table if exists hegis_areas, hegis_codes')
cursor.execute("""
                  create table hegis_areas (
                    id serial primary key,
                    hegis_area text);
                  create table hegis_codes (
                    hegis_code text primary key,
                    area_id integer references hegis_areas,
                    description text
                  );
               """)

area_name = None
area_id = -1
for table in tables:
  assert table.children[0].tagName == 'caption'
  area_name = table.children[0].innerText.strip()
  cursor.execute('insert into hegis_areas values(default, %s) returning id', (area_name, ))
  area_id = cursor.fetchone()[0]
  for row in table.children[2].children:
    assert row.tagName == 'tr'
    hegis_code = row.children[0].innerText.strip()
    description = row.children[1].innerText.strip()
    cursor.execute("""
                      insert into hegis_codes values (%s, %s, %s)
                      on conflict do nothing
                   """, (hegis_code, area_id, description))

changes = parser.getElementsByClassName('pane-node-changed')
update_date = datetime.strptime(changes[0].children[1].innerText.strip(), '%B %d, %Y - %I:%M%p')
cursor.execute(f"update updates set update_date = '{update_date}' where table_name = 'hegis_codes'")
conn.commit()
conn.close()

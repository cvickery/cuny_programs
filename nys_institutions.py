#! /usr/local/bin/python3

import re
from datetime import date
from typing import Dict, Tuple

import requests
from lxml.html import document_fromstring
import cssselect

from pgconnection import PgConnection

"""   Institutions that have academic programs registered with NYS Department of Education.
      Includes all known CUNY colleges plus other institutions that have M/I programs with a CUNY
      institution.
      For each institution, the institution id number (as a string), the institution name,
      as spelled on the NYS website, and a boolean to indicate whether it is a CUNY college or not.
"""
#  CUNY colleges with their TLA as institution_id. These get entered in the db with is_cuny == True.
#  They also get entered with their numeric string as institution_id and is_cuny == False. The
#  latter entries are not actually used, but they come in as part of the NYSED website scraping
#  process.
cuny_institutions: Dict[str, Tuple] = dict()
cuny_institutions['bar'] = ('33050', 'CUNY BARUCH COLLEGE')
cuny_institutions['bcc'] = ('37100', 'BRONX COMM COLL')
cuny_institutions['bkl'] = ('33100', 'CUNY BROOKLYN COLL')
cuny_institutions['bmc'] = ('37050', 'BOROUGH MANHATTAN COMM C')
cuny_institutions['cty'] = ('33150', 'CUNY CITY COLLEGE')
cuny_institutions['csi'] = ('33180', 'CUNY COLL STATEN ISLAND')
cuny_institutions['grd'] = ('31050', 'CUNY GRADUATE SCHOOL')
cuny_institutions['hos'] = ('37150', 'HOSTOS COMM COLL')
cuny_institutions['htr'] = ('33250', 'CUNY HUNTER COLLEGE')
cuny_institutions['jjc'] = ('33300', 'CUNY JOHN JAY COLLEGE')
cuny_institutions['kcc'] = ('37250', 'KINGSBOROUGH COMM COLL')
cuny_institutions['lag'] = ('37200', 'LA GUARDIA COMM COLL')
cuny_institutions['law'] = ('31100', 'CUNY LAW SCHOOL AT QUEENS')
cuny_institutions['leh'] = ('33200', 'CUNY LEHMAN COLLEGE')
cuny_institutions['mec'] = ('37280', 'MEDGAR EVERS COLL')
cuny_institutions['ncc'] = ('33350', 'STELLA & CHAS GUTTMAN CC')
cuny_institutions['nyt'] = ('33380', 'NYC COLLEGE OF TECHNOLOGY')
cuny_institutions['qcc'] = ('37350', 'QUEENSBOROUGH COMM COLL')
cuny_institutions['qns'] = ('33400', 'CUNY QUEENS COLLEGE')
cuny_institutions['sps'] = ('31051', 'CUNY SCHOOL OF PROF STUDY')
cuny_institutions['yrk'] = ('33500', 'CUNY YORK COLLEGE')

# Scrape the NYSED website for institution id numbers and names. Sending a POST request with the
# name "searches" and value "1" gets a page with a form with all institutions and ids as options
# in a select element.
url = 'http://www.nysed.gov/coms/rp090/IRPSL1'
r = requests.post(url, data={'searches': 1})
html_document = document_fromstring(r.content)
option_elements = [option.text_content() for option in html_document.cssselect('option')]
if len(option_elements) < 100:
  exit(f'{__file__}: ERROR: got only {len(option_elements)} institutions from {url}.')

conn = PgConnection()
cursor = conn.cursor()
cursor.execute("select update_date from updates where table_name = 'nys_institutions'")
if cursor.rowcount == 0:
  print('Creating nys_institutions table')
  cursor.execute("""create table if not exists nys_institutions (
                    id text primary key,
                    institution_id text,
                    institution_name text,
                    is_cuny boolean)
                 """)
  cursor.execute('truncate nys_institutions')
  cursor.execute("""insert into updates values ('nys_institutions')""")
else:
  print(f'Truncating nys_institutions table previously updated {cursor.fetchone().update_date}.')
  cursor.execute('truncate nys_institutions')
print(f'Adding {len(cuny_institutions)} CUNY institutions')
for key, value in cuny_institutions.items():
  cursor.execute(f"""insert into nys_institutions values(%s, %s, %s, %s)
                  """, (key, value[0], value[1], True))
print(f'Adding {len(option_elements)} NYS institutions')
for option_element in option_elements:
  institution_id, institution_name = option_element.split(maxsplit=1)
  assert institution_id.isdecimal()
  institution_id = f'{int(institution_id):06}'
  cursor.execute(f"""insert into nys_institutions values(%s, %s, %s, %s)
                  """, (institution_id, institution_id, institution_name.strip(), False))
today = date.today().strftime('%Y-%m-%d')
cursor.execute(f"update updates set update_date ='{today}' where table_name='nys_institutions'")
conn.commit()
conn.close()

#! /usr/local/bin/python3
"""Create table of all NYS institutions, with special attention to CUNY."""

import psycopg
import requests

from datetime import date
from lxml.html import document_fromstring
from pathlib import Path
from psycopg.rows import namedtuple_row
from typing import Dict, Tuple

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
cuny_institutions['bar'] = ('330500', 'CUNY BARUCH COLLEGE')
cuny_institutions['bcc'] = ('371000', 'BRONX COMM COLL')
cuny_institutions['bkl'] = ('331000', 'CUNY BROOKLYN COLL')
cuny_institutions['bmc'] = ('370500', 'BOROUGH MANHATTAN COMM C')
cuny_institutions['cty'] = ('331500', 'CUNY CITY COLLEGE')
cuny_institutions['csi'] = ('331800', 'CUNY COLL STATEN ISLAND')
cuny_institutions['grd'] = ('310500', 'CUNY GRADUATE SCHOOL')
cuny_institutions['hos'] = ('371500', 'HOSTOS COMM COLL')
cuny_institutions['htr'] = ('332500', 'CUNY HUNTER COLLEGE')
cuny_institutions['jjc'] = ('333000', 'CUNY JOHN JAY COLLEGE')
cuny_institutions['kcc'] = ('372500', 'KINGSBOROUGH COMM COLL')
cuny_institutions['lag'] = ('372000', 'LA GUARDIA COMM COLL')
cuny_institutions['law'] = ('311000', 'CUNY SCHOOL OF LAW')
cuny_institutions['leh'] = ('332000', 'CUNY LEHMAN COLLEGE')
cuny_institutions['mec'] = ('372800', 'MEDGAR EVERS COLL')
cuny_institutions['ncc'] = ('333500', 'STELLA & CHAS GUTTMAN CC')
cuny_institutions['nyt'] = ('333800', 'NYC COLLEGE OF TECHNOLOGY')
cuny_institutions['qcc'] = ('373500', 'QUEENSBOROUGH COMM COLL')
cuny_institutions['qns'] = ('334000', 'CUNY QUEENS COLLEGE')
cuny_institutions['sps'] = ('310510', 'CUNY SCHOOL OF PROF STUDY')
cuny_institutions['yrk'] = ('335000', 'CUNY YORK COLLEGE')

# Scrape the NYSED website for institution id numbers and names. Sending a POST request with the
# name "searches" and value "1" gets a page with a form with all institutions and ids as options
# in a select element.
""" Getting empty response. It's like the query string isn't getting sent.
    Much fiddling, including trying to emulate the headers Firefox uses when making a successful
    request to the same url. No success. Always getting status 200 but the web page returned says
    "Please page back and check your input, must select a search option" instead of returning a
    select element with the colleges as option elements.
"""
# 'Host': 'www2.nysed.gov', status_code is 404 if present in headers
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 '
                         'Firefox/110.0',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,'
                     '*/*;q=0.8',
           'Accept-Language': 'en-US,en;q=0.5',
           'Accept-Encoding': 'gzip, deflate, br',
           'Content-Type': 'application/x-www-form-urlencoded',
           'Content-Length': '10',
           'Origin': 'https://www2.nysed.gov',
           'Connection': 'keep-alive',
           'Referer': 'https://www2.nysed.gov/heds/IRPSL1.html',
           'Upgrade-Insecure-Requests': '1',
           'Sec-Fetch-Dest': 'document',
           'Sec-Fetch-Mode': 'navigate',
           'Sec-Fetch-Site': 'same-origin',
           'Sec-Fetch-User': '?1'}
script_file = Path(__file__).name
url = 'https://www2.nysed.gov/coms/rp090/IRPSL1/'
response = requests.post(url, data={'Searches': "1"})
if response.status_code == requests.codes.ok:
  html_document = document_fromstring(response.content)
  option_elements = [option.text_content() for option in html_document.cssselect('option')]
  if len(option_elements) < 400:
    exit(f'{script_file}: ERROR: received {len(option_elements)} institutions from {url} '
         f'(expected 400+).')
else:
  exit(f'{script_file}: ERROR: {url} returned {response.status_code} status')

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    print('Creating nys_institutions table')
    cursor.execute("""
    drop table if exists nys_institutions;
    create table nys_institutions (
      id text primary key,
      institution_id text,
      institution_name text,
      is_cuny boolean);
    insert into updates values ('nys_institutions') on conflict do nothing;
    """)
    print(f'Adding {len(cuny_institutions)} CUNY institutions')
    for key, value in cuny_institutions.items():
      cursor.execute("""insert into nys_institutions values(%s, %s, %s, %s)
                      """, (key, value[0], value[1], True))
    print(f'Adding {len(option_elements)} NYS institutions')
    for option_element in option_elements:
      institution_id, institution_name = option_element.split(maxsplit=1)
      assert institution_id.isdecimal()
      institution_id = f'{int(institution_id):06}'
      cursor.execute("""insert into nys_institutions values(%s, %s, %s, %s)
                      """, (institution_id, institution_id, institution_name.strip(), False))
    today = date.today().strftime('%Y-%m-%d')
    cursor.execute("""
    update updates set update_date = CURRENT_DATE
     where table_name='nys_institutions'
    """)

""" DB lookup of CIP code info from database.
"""

import psycopg
from psycopg.rows import namedtuple_row

# Module initialization
with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    cursor.execute('select cip_code, cip_title from cipcodes')
    _cip_codes = {cip.cip_code: cip.cip_title for cip in cursor.fetchall()}


def cip_codes(cip_code: str) -> str:
  """ API for accessing CIP codes.
  """
  while cip_code != '' and cip_code not in _cip_codes.keys():
    cip_code = cip_code[:-1]
  if cip_code != '':
    return _cip_codes[cip_code]
  return 'Unknown'

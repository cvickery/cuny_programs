#! /usr/local/bin/python3

from dgw_filter import dgw_filter


# to_html()
# -------------------------------------------------------------------------------------------------
def to_html(requirement_text):
  """ Generate a HTML details element for the code of a Scribe Block.
  """
  # catalog_type, first_year, last_year, catalog_years_text = catalog_years(row.period_start,
  #                                                                         row.period_stop)
  # institution_name = institution_names[row.institution]
  filtered_text = dgw_filter(requirement_text)
  html = f"""
<details>
  <summary><strong>Degree Works Code</strong> (<em>Scribe Block</em>)</summary>
  <hr>
  <pre>{filtered_text.replace('<', '&lt;')}</pre>
</details>
"""

  return html.replace('\t', ' ').replace("'", '’')


if __name__ == '__main__':
  print('Command line access not supported')

#! /usr/local/bin/python3

import json
import psycopg
import sys

from cip_codes import cip_codes
from collections import namedtuple
from datetime import datetime, date
from knowninstitutions import known_institutions
from pathlib import Path
from psycopg.rows import namedtuple_row

DEBUG = False


# fix_title()
# -------------------------------------------------------------------------------------------------
def fix_title(str):
  """ Create a better titlecase string, taking specifics of the registered_programs dataset into
      account.
  """
  return (str.strip(' *')
             .title()
             .replace('Cuny', 'CUNY')
             .replace('Mhc', 'MHC')
             .replace('Suny', 'SUNY')
             .replace('\'S', '’s')
             .replace('1St', '1st')
             .replace('6Th', '6th')
             .replace(' And ', ' and ')
             .replace(' Of ', ' of ')
             .replace('\'', '’'))


# andor_list()
# -------------------------------------------------------------------------------------------------
def andor_list(items, andor='and'):
  """ Join a list of strings into a comma-separated con/disjunction.
      Forms:
        a             a
        a and b       a or b
        a, b, and c   a, b, or c
  """
  return_str = ', '.join(items)
  k = return_str.rfind(',')
  if k > 0:
    k += 1
    return_str = return_str[:k] + f' {andor}' + return_str[k:]
  if return_str.count(',') == 1:
    return_str = return_str.replace(',', '')
  return return_str


# generate_html()
# -------------------------------------------------------------------------------------------------
def generate_html():
  """ Generate the html for registered programs rows
  """
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      with conn.cursor(row_factory=namedtuple_row) as inner_cursor:

        # Find out what CUNY colleges are in the db
        cursor.execute("""
                       select distinct r.target_institution as inst, i.name
                       from registered_programs r, cuny_institutions i, nys_institutions n
                       where i.code = upper(r.target_institution||'01')
                       order by i.name
                       """)

        if cursor.rowcount < 1:
          exit("No registered-program information for CUNY colleges available at this time")

        cuny_institutions = dict([(row.inst, {'name': row.name}) for row in cursor])

        cursor.execute('select hegis_code, description from hegis_codes')
        hegis_codes = {row.hegis_code: row.description for row in cursor}

        # List of short CUNY institution names plus known non-CUNY names
        # Start with the list of all known institutions, then replace CUNY names with their short
        # names.
        short_names = dict()
        for key in known_institutions.keys():
          short_names[key] = known_institutions[key][1]  # value is (prog_code, name, is_cuny)
        cursor.execute("""
                          select code, prompt
                            from cuny_institutions
                       """)
        for row in cursor:
          short_names[row.code.lower()[0:3]] = row.prompt

        # Generate the HTML and CSV values for each row of the respective tables, and save them in
        # the registered_programs table as html and csv column data.
        cursor.execute("""
                       select program_code,
                              unit_code,
                              institution,
                              title,
                              formats,
                              hegis,
                              award,
                              certificate_license,
                              accreditation,
                              first_registration_date,
                              last_registration_action,
                              tap, apts, vvta,
                              target_institution,
                              institution_id as sed_code,
                              is_variant
                       from registered_programs, nys_institutions
                       where nys_institutions.id ~* registered_programs.institution
                       order by title, program_code
                       """)

        # Parallel structures for the HTML and CSV cells
        total_rows = cursor.rowcount
        row_number = 0
        for row in cursor:
          row_number += 1
          if DEBUG:
            # Progress to stdout
            print(f'\r{row_number:,}/{total_rows:,}', end='')
            # Debug info to stderr
            print(row, file=sys.stderr)

          # Pick out two parameters for later use
          if row.is_variant:
            class_str = ' class="variant"'
          else:
            class_str = ''
          sed_code = row.sed_code

          html_values = list(row)
          csv_values = list(row)

          # Get rid of the two parameter values that won't be displayed.
          #   Don’t display is_variant value: it is indicated by the row’s class.
          html_values.pop()
          csv_values.pop()
          #   Don’t display the NYSED Institution Code: it will be a hover in the HTML version
          html_values.pop()
          csv_values.pop()
          #   Don’t display the target institution
          html_values.pop()
          csv_values.pop()

          # If the institution column is a numeric string, it’s a non-CUNY partner school, but the
          # name is available in the known_institutions dict.
          if html_values[2].isdecimal():
            html_values[2] = fix_title(known_institutions[html_values[2]][1])
            csv_values[2] = html_values[2]
          # Add hover for sed_code
          html_values[2] = f'<span title="NYSED Institution ID {sed_code}">{html_values[2]}</span>'

          # Add title with hegis code description to hegis_code column
          try:
            description = hegis_codes[html_values[5]]
            element_class = ''
          except KeyError as ke:
            description = 'Unknown HEGIS Code'
            element_class = ' class="error"'
          html_values[5] = f'<span title="{description}"{element_class}>{html_values[5]}</span>'
          csv_values[5] = f'{csv_values[5]} ({description})'

          # Insert list of all CUNY programs (plans) for this program code
          inner_cursor.execute("""select * from cuny_programs
                                 where nys_program_code = %s
                                 and program_status = 'A'""", (html_values[0],))
          cuny_cell_html_content = ''
          cuny_cell_csv_content = ''
          cip_set = set()
          if inner_cursor.rowcount > 0:
            plans = inner_cursor.fetchall()
            # There is just one program and description per college, but the program may be shared
            # among multiple departments at a college.
            Program_Info = namedtuple('Program_Info', 'program program_title departments')
            program_info = dict()
            program = None
            program_title = None
            for plan in plans:
              cip_set.add(plan.cip_code)
              institution_key = plan.institution.lower()[0:3]
              if institution_key not in program_info.keys():
                program_info[institution_key] = Program_Info._make([plan.academic_plan,
                                                                    plan.description,
                                                                    []
                                                                    ])
              program_info[institution_key].departments.append(plan.department)

            # Add information for this institution to the table cell
            if len(program_info.keys()) > 1:
              cuny_cell_html_content += '— <em>Multiple Institutions</em> —<br>'
              cuny_cell_csv_content += 'Multiple Institutions: '
              show_institution = True
            else:
              show_institution = False
            for inst in program_info.keys():
              program = program_info[inst].program
              program_title = program_info[inst].program_title
              if show_institution:
                if inst in short_names.keys():
                  inst_str = f'{short_names[inst]}: '
                else:
                  inst_str = f'{inst}: '
              else:
                inst_str = ''
              departments_str = andor_list(program_info[inst].departments)
              cuny_cell_html_content += (f' {inst_str}{program} ({departments_str})'
                                         f'<br>{program_title}')
              cuny_cell_csv_content += f'{inst_str}{program} ({departments_str})\n{program_title}'

              # If there is a single dgw requirement block for the plan, link to it
              institution = row.institution
              inner_cursor.execute("""
                                 select *
                                   from requirement_blocks
                                  where institution ~* %s
                                    and block_type = 'MAJOR'
                                    and block_value = %s
                                    and period_stop ~* '^9'
                                 """, (institution, plan.academic_plan))
              # Can only link to a single RA for a major from here. Log multiple-RA instances.
              if inner_cursor.rowcount > 0:
                if inner_cursor.rowcount == 1:
                  plan_row = inner_cursor.fetchone()
                  cuny_cell_html_content += (f'<br><a href="/requirements/?institution='
                                             f'{institution.upper() + "01"}'
                                             f'&requirement_id={plan_row.requirement_id}">'
                                             f'Requirements</a>')
                  # IDEALLY the host would automatically adjust to the deployment target
                  # (transfer-app.qc.cuny.edu, Heroku, or explorer.cuny.edu, etc). But it's
                  # hard-coded here ... for now.
                  host = 'transfer-app.qc.cuny.edu'
                  cuny_cell_csv_content += (f'\nhttps://{host}/requirements/?institution='
                                            f'{institution.upper() + "01"}'
                                            f'&requirement_id={plan_row.requirement_id}')
                else:
                  # Log the occurrence of multiple current RA's for this program
                  home_dir = Path.home()
                  log_file_path = Path(home_dir, 'Projects/cuny_programs/registered_programs.log')
                  with log_file_path.open(mode='a') as log_file:
                    print(f'{date.today()} Found {inner_cursor.rowcount} current RA’s for '
                          f'{institution}, {plan.academic_plan}', file=log_file)
              if show_institution:
                cuny_cell_html_content += '<br>'
                cuny_cell_csv_content += '\n'
          cip_html_cell = [f'<span title="{cip_codes(cip)}">{cip}</span>'
                           for cip in sorted(cip_set)]
          cip_csv_cell = [f'{cip} ({cip_codes(cip).strip(".")})' for cip in sorted(cip_set)]
          html_values.insert(7, '<br>'.join(cip_html_cell))
          csv_values.insert(7, ', '.join(cip_csv_cell))
          html_values.insert(8, cuny_cell_html_content)
          csv_values.insert(8, cuny_cell_csv_content)

          html_cells = ''.join([f'<td>{value}</td>' for value in html_values]).replace("\'", "’")
          if DEBUG:
            print(f'  {row.award}', file=sys.stderr)
            print(f'  {csv_values}', file=sys.stderr)
            print(f'  {html_values}', file=sys.stderr)
          inner_cursor.execute(f"""update registered_programs
                                      set html='<tr{class_str}>{html_cells}</tr>'
                                    where target_institution = %s
                                      and program_code = %s
                                      and award = %s
                          """, (row.target_institution, row.program_code, row.award))

          inner_cursor.execute(f"""update registered_programs
                                      set csv= %s
                                    where target_institution = %s
                                      and program_code = %s
                                      and award = %s
                           """, (json.dumps(csv_values), row.target_institution, row.program_code, row.award))


if __name__ == '__main__':
  """ Command line interface: any argument turns on debugging/progress
  """
  if len(sys.argv) > 1:
    DEBUG = True
  start = datetime.now()
  generate_html()
  print(f'  {(datetime.now() - start).total_seconds():0.1f} seconds')

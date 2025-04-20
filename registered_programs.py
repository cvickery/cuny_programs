#! /usr/local/bin/python3
"""
  Scrapes the NYS DOE website for registered academic programs at CUNY colleges.

      This is a two-phase process:

      I. Make a POST request to https://www.nysed.gov/coms/rp090/IRPS2A to get a web page listing
      all programs for a college, and extract the numeric program codes and Unit Codes.

      This is the url with the form for getting to the "search by institution" page:
        https://www2.nysed.gov/heds/IRPSL1.html
      You can get to this page manually (GET).

      This is url of the search by institution page, but it only works if called from the previous
      page (POST):
        https://www2.nysed.gov/coms/rp090/IRPSL1

      II. Make a GET request to https://www.nysed.gov/COMS/RP090/IRPSL3 for each of the program
      codes retrieved from Phase I, and analyze each page returned to extract details about the
      program needed to generate the desired output, which may be a .csv file, a HTML table, or a
      database table.

April 2019:
      Unit Code is new: “Applications for program revisions, title changes and program
      discontinuances should be submitted to the NYSED office that originally registered the
      program.”
        OP:   Office of the Professions
        OCUE: Office of College and University Evaluation

      Refactoring the previous version of this application to accommodate this new piece of
      data led to a better understanding of how NYS structures the information about academic
      programs. In particular, this version centers on the notion of program “variants,” which have
      a common program code number, but differ in what award(s) are offered by what institution(s)
      and under what HEGIS codes.

Design Notes

    The web code scraped from the NYSED website are not well-formed HTML documents. Phase I
    documents can reliably be parsed into H4 elements, but Phase II documents are formatted as PRE
    elements inside un-closed H4’s. That information is processed as lines of text, relying on the
    internal formatting of the PRE blocks.

    RegisteredProgram codes and HEGIS codes look like integers and floats respectively, but are kept
    as strings because that is how they arrive and that is how they are always used/displayed.

"""
import argparse
import csv
import os
import psycopg
import re
import requests
import socket
import sys


from datetime import date
from lxml.html import document_fromstring
from registered_program import RegisteredProgram
from psycopg.rows import namedtuple_row
from sendemail import send_message

known_institutions = dict()
with psycopg.connect(dbname='cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    cursor.execute("select * from nys_institutions")
    known_institutions = {row.id: (row.institution_id, row.institution_name, row.is_cuny)
                          for row in cursor.fetchall()}


def detail_lines(all_lines, debug=False):
  """Filter out unwanted lines from a details web page for a program code; yield the others."""
  lines = all_lines.splitlines()
  for line in lines:
    if re.search(r'^\s+\d{5}\s+|FOR AWARD|PROGRAM|CERTIFICATE|M/A|M/I', line):
      next_line = line.replace('<H4><PRE>', '').strip()
      if debug:
        print(next_line)
      yield next_line


def fix_title(str):
  """Create a better titlecase string, taking specifics of this dataset into account."""
  return (str.strip(' *')
             .title()
             .replace('Cuny', 'CUNY')
             .replace('Mhc', 'MHC')
             .replace('Suny', 'SUNY')
             .replace('\'S', '’s')
             .replace('1St', '1st')
             .replace('6Th', '6th')
             .replace(' And ', ' and ')
             .replace(' Of ', ' of '))


def lookup_programs(institution, verbose=False, debug=False):
  """Scrape info about programs registered with NYS from the Department of Education website.

  Create a RegisteredProgram object for each program_code.
  """
  try:
    institution_id, institution_name, is_cuny = known_institutions[institution]
  except KeyError:
    # Unrecognized institution: assume it’s malicious.
    if re.match(r'^\w+$', institution) is None:
      sys.exit('Malformed institution name.')
    else:
      sys.exit(f'Unrecognized institution: {institution}.')

  # Phase I: Get the program code, title, award, hegis, and unit code for all programs
  # registered for the institution.
  if verbose:
    print(f'Fetching list of registered programs for {institution_name} ...', file=sys.stderr)
  try:
    url = 'https://www2.nysed.gov/coms/rp090/IRPS2A'
    r = requests.post(url, data={'SEARCHES': '1', 'instid': f'{institution_id}'})
    html_document = document_fromstring(r.content)
    h4s = [h4.text_content() for h4 in html_document.cssselect('h4')]
    if len(h4s) < 4:
      raise ValueError(f'Got {len(h4s)} H4 elements from {url} for {institution}')
  except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, ValueError) as err:
    send_message([{'name': 'Christopher Vickery', 'email': 'cvickery@qc.cuny.edu'}],
                 {'name': 'Transfer App', 'email': 'cvickery@qc.cuny.edu'},
                 f'Registered Programs Update Failed on {socket.gethostname()}',
                 f'<p>{err}</p>')
    exit(f'{__file__}: ERROR: {socket.gethostname()} {err}')

  # The program codes and unit codes are inside H4 elements, in the following sequence:
  #   PROGRAM CODE  : 36256 - ...
  #   PROGRAM TITLE : [title text] AWARD : [award text]
  #   INST.NAME/CITY .[name and address, ignored].. HEGIS : [hegis string for this award]
  #   FORMATS ... (Not always present.)
  #   UNIT CODE     : OCUE|OP
  this_award = None
  for h4 in h4s:
    if debug:
      print(h4)
    matches = re.search(r'PROGRAM CODE\s+:\s+(\d+) -.+PROGRAM TITLE\s+:\s+(.+)AWARD : (\S+\s?\S*)',
                        h4)
    if matches:
      program_code = matches.group(1)
      program = RegisteredProgram(program_code)
      this_title = fix_title(matches.group(2))
      this_award = matches.group(3).strip()
      continue

    matches = re.search(r'HEGIS : (\S+)', h4)
    if matches:
      this_hegis = matches.group(1)

      # The institution should match the one that was requested.
      this_institution = None
      for inst in known_institutions.keys():
        if known_institutions[inst][1] in h4:
          this_institution = inst
          break
      if this_institution is None:
        sys.exit(f'Unknown institution in {h4}')

      if this_institution != institution:
        print(f'h4 wrong institution: {this_institution} is not {institution}\n{h4}. Ignored')
        continue

      program.new_variant(this_award, this_hegis, this_institution, title=this_title)
      continue

    if 'UNIT CODE' in h4:
      matches = re.match(r'\s*UNIT CODE\s*:\s*(.+)\s*', h4)
      assert matches is not None, f'\nUnrecognized unit code line: {h4}'
      program.unit_code = matches.group(1).strip()
      continue

    # The formats information, like the program and unit codes, applies to all variants
    if 'FORMATS' in h4:
      matches = re.match(r'\s*FORMATS\s*:\s*(.+)\s*', h4)
      assert matches is not None, f'\nUnrecognized formats line: {h4}'
      program.formats = matches.group(1).strip()
      continue

  if verbose:
    num_programs = len(RegisteredProgram.programs)
    len_num = len(str(num_programs))
    print(f'Found {num_programs} registered programs.', file=sys.stderr)
    print('Fetching details...', file=sys.stderr)

  if debug:
    for p in RegisteredProgram.programs:
      program = RegisteredProgram.programs[p]
      print(program.program_code, program.unit_code)
      for v in program.variants:
        print(v, program.values(v))

  # Phase II: Get the details for each program found in Phase I
  # Structure:
  # * A program line followed by optional multi-award, and multi-institution lines. These
  #   lines determine the program variants for a program.
  # * A for-award line followed by detail lines for that award. There will be one or more for-award
  #   groups. The details get applied to all variants that include the specified award.
  #
  # The following code tests lines in the sequence in which they appear on the details web page.
  # This is to reduce cognitive load: the tests for line types could be done in any order and the
  # actual sequence of lines on the details page would make it all work out.

  programs_counter = 0  # For progress reporting in verbose mode
  for p in RegisteredProgram.programs:
    program = RegisteredProgram.programs[p]
    programs_counter += 1
    if verbose and os.isatty(sys.stdout.fileno()):
      print(f'Registered Program code: {p} ({programs_counter:{len_num}}/{num_programs})\r',
            end='', file=sys.stderr)

    for_award = None
    try:
      r = requests.get(f'https://www2.nysed.gov/COMS/RP090/IRPSL3?PROGCD={program.program_code}')
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
      send_message([{'name': 'Christopher Vickery', 'email': 'cvickery@qc.cuny.edu'}],
                   {'name': 'Transfer App', 'email': 'cvickery@qc.cuny.edu'},
                   f'Registered Programs Update Failed on {socket.gethostname()}',
                   f'<p>{err}</p>')
      exit(f'{__file__}: ERROR: {socket.gethostname()} {err}')

    # There was a web page that had a 0x1e in the middle of a string of blanks (program code 31441
    # at CSI), and splitlines() uses this as one of the line boundaries ((Record Separator)), which
    # broke the first re.match operation below. There is no option for changing the behavior of the
    # splitlines builtin, so we delete the stray character from all web pages retrieved. By rights,
    # we should also be deleting \v, \f, \x1c, \x1d, \x85, \u2028, and \u2029 as well. But we don’t.
    for line in detail_lines(r.text.replace('\x1e', '')):
      if debug:
        print(line)
      # Use the first token on a line to determine the type of line.
      tokens = line.split()
      token = tokens[0]

      # First token is a numeric string (Program Code #.) or Multi-Award (M/A).
      if token.isdecimal() or token == 'M/A':
        # Extract program_code, title, hegis_code, award, institution.
        matches = re.match(r'\s*(\d+|M/A)\s+(.+)(\d{4}\.\d{2})\s+(\S+\s?\S*)\s+(.+)', line)
        if matches is None:
          sys.exit(f'\nUnable to parse program code line for program code {program_code}:\n{line}')
        # Check the title and hegis for the award. Always set the institution.
        program_title = fix_title(matches.group(2))
        program_hegis = matches.group(3)
        program_award = matches.group(4).strip()
        program_institution = matches.group(5)

        if debug:
          print(f'Program Code # or M/A line: {program.program_code}: "{program_title}" '
                f'{program_hegis} {program_award} "{program_institution}"')

        this_institution = None
        for key in known_institutions.keys():
          if program_institution == known_institutions[key][1]:
            this_institution = key
            break
        assert this_institution is not None, f'\n{this_institution}\n{line}'

        # Create this variant if necessary (Never used)
        # this_variant = program.new_variant(program_award, program_hegis, this_institution,
        #                                    title=program_title)
        continue

      if token == 'M/I':
        # Multi-Institution: extract hegis, award, institution
        if 'NOT-GRANTING' in line:
          # If the award is NOT-GRANTING, then variants for this award-institution pair have to be
          # removed.
          matches = re.search(r'NOT-GRANTING\s+(.+)', line)
          if matches is None:
            sys.exit(f'\nUnable to parse M/I line for program code {program.program_code}:{line}')
          this_institution = matches.group(1).strip()
          for inst in known_institutions:
            if this_institution == known_institutions[inst][1]:
              for variant_tuple in list(program.variants.keys()):
                if variant_tuple[0] == program_award and variant_tuple[2] == inst:
                  program.variants.pop(variant_tuple, None)
                  if debug:
                    print(f'Deleted tuple {variant_tuple}')
        else:
          matches = re.search(r'(\d{4}.\d{2})\s+(\S+\s?\S*)\s+(.*)', line)
          if matches is None:
            sys.exit(f'\nUnable to parse M/I line for program code {program.program_code}:{line}')
          program_hegis = matches.group(1)
          program_award = matches.group(2).strip()
          program_institution_name = matches.group(3).strip()
          program_institution = None
          for inst in known_institutions:
            if program_institution_name == known_institutions[inst][1]:
              program_institution = inst
              break
          assert program_institution is not None, 'Unrecognized institution {} in {}'.format(
              program_institution_name, line)

          # Create this variant if necessary
          variant = program.new_variant(program_award, program_hegis, program_institution)
          if debug:
            print(variant)
        continue

      if token == 'FOR':
        # Extract award, and use it to select variant_tuples that will be affected by detail lines
        # that follow.
        for_award = re.match(r'\s*FOR AWARD\s*--(.*)', line).group(1).strip()
        variant_tuples = [variant_tuple for variant_tuple in program.variants
                          if variant_tuple[0] == for_award]
        if debug:
          for variant in variant_tuples:
            print(variant)

      # Detail lines for the currently-identified award.
      if token.startswith('CERTIFICATE') and for_award is not None:
        # Extract certificate tuple {name, type, date} if there is one.
        cert_info = re.sub(r'\s+', ' ', line.split(':')[1].strip())
        if cert_info.startswith('NONE'):
          cert_info = ''
        for variant_tuple in variant_tuples:
          if debug:
            print(f'Update {variant_tuple} with cert info “{cert_info}”')
          program.variants[variant_tuple].certificate_license = cert_info
        continue

      if token == 'PROGRAM' and tokens[1] == 'FINANCIAL' and for_award is not None:
        # Extract three booleans.
        matches = re.search(r'(YES|NO).+(YES|NO).+(YES|NO)', line)
        if matches is None:
          sys.exit(f'\nUnable to parse eligibility line for program code {program_code}:\n{line}')
        for variant_tuple in variant_tuples:
          if debug:
            print('Update {} with: {} {} {}'.format(variant_tuple,
                                                    matches.group(1),
                                                    matches.group(2),
                                                    matches.group(3)))
          program.variants[variant_tuple].tap = matches.group(1)
          program.variants[variant_tuple].apts = matches.group(2)
          program.variants[variant_tuple].vvta = matches.group(3)
        continue

      if token == 'PROGRAM' and tokens[1] == 'PROFESSIONAL' and for_award is not None:
        # Extract text, if any.
        program_accreditation = line.split(':')[1].strip()
        for variant_tuple in variant_tuples:
          if debug:
            print(f'Update {variant_tuple} with accreditiation: “{program_accreditation}”')
          program.variants[variant_tuple].accreditation = program_accreditation
        continue

      if token == 'PROGRAM' and tokens[1] == 'FIRST' and for_award is not None:
        matches = re.search(r'DATE:\s+(\S+).+ACTION:\s+(\S+)', line)
        if matches is None:
          sys.exit('\nUnable to parse registration dates for program code {}:\n{}'
                   .format(program.program_code, line))
        first_date = matches[1]
        last_date = matches[2]
        for variant_tuple in variant_tuples:
          if debug:
            print(f'Update {variant_tuple} with dates: {first_date} {last_date}')
          if (program.variants[variant_tuple].first_registration_date is None
              or first_date.replace('PRE-', '19')
              < program.variants[variant_tuple].first_registration_date):
            program.variants[variant_tuple].first_registration_date = first_date
          if (program.variants[variant_tuple].last_registration_action is None
              or last_date > program.variants[variant_tuple].last_registration_action):
            program.variants[variant_tuple].last_registration_action = last_date

  if verbose:
    print('\r')
  return RegisteredProgram.programs


""" Command Line Interface
"""
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='''
                                   Scrape the NYS Department of Education website for information
                                   about academic programs registered for CUNY colleges.''')
  parser.add_argument('institution')
  parser.add_argument('-u', '--update_db', action='store_true', default=False,
                      help='update info for this institution in the registered_programs database')
  parser.add_argument('-w', '--html', action='store_true', default=False,
                      help='generate a html table suitable for the web')
  parser.add_argument('-c', '--csv', action='store_true', default=False,
                      help='generate a CSV table')
  parser.add_argument('-d', '--debug', action='store_true', default=False)
  parser.add_argument('-v', '--verbose', action='store_true', default=False)
  args = parser.parse_args()

  if not args.debug and not args.csv and not args.html and not args.update_db:
    sys.exit('No output options: nothing to do.')

  # Institution ID is a six-digit numeric string or, for CUNY, three letters followed by an optional
  # 01.
  if len(args.institution) < 6:
    institution = args.institution.lower().strip('10')
  else:
    institution = args.institution

  programs = lookup_programs(institution, debug=args.debug, verbose=args.verbose)
  if programs is not None:

    if args.csv:
      # Generate spreadsheet
      #   Apple Numbers does a better job than Microsoft Excel at opening the CSV file.
      #   For Excel, it’s better to import it.
      file_name = institution.upper() + '_' + date.today().isoformat() + '.csv'
      with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Program Code', 'Registration Office', 'Formats']
                        + RegisteredProgram._headings)
        for p in RegisteredProgram.programs:
          program = programs[p]
          for program_variant in program.variants:
            writer.writerow([program.program_code, program.unit_code, program.formats]
                            + program.values(program_variant))

    if args.html:
      # Generate a HTML table element. Add CSS to highlight rows that have the “variant” class.
      print(RegisteredProgram.html_table())

    if args.update_db:
      # See registered_programs.sql for the schema of the table, which must already exist.
      with psycopg.connect('dbname=cuny_curriculum') as conn:
        with conn.cursor(row_factory=namedtuple_row) as cursor:
          cursor.execute('delete from registered_programs where target_institution=%s',
                         (institution,))
          print('Replacing {} entries for {} with info for {} programs.'
                .format(cursor.rowcount, institution.upper(), len(RegisteredProgram.programs)))
          for p in RegisteredProgram.programs:
            program = programs[p]
            is_variant = len(program.variants) > 1
            for program_variant in program.variants:
              values = [institution, program.program_code, program.unit_code]
              values += program.values(program_variant)
              values += [is_variant]
              values.insert(6, program.formats)
              # deal with nul bytes from NYS
              for i in range(len(values)):
                if type(values[i]) is str:
                  values[i] = values[i].replace('\x00', '')
              cursor.execute(f'insert into registered_programs values('
                             f"{', '.join(['%s'] * len(values))})", values)

  else:
    sys.exit('lookup_programs failed')

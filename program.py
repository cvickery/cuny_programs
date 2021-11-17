""" The Program class, which is a list of NYS-registered academic programs.
"""
import sys
import re
from typing import Dict, Any
from recordclass import recordclass

_items = ['institution',
          'title',
          'award',
          'hegis',
          'first_registration_date',
          'last_registration_action',
          'tap', 'apts', 'vvta',
          'certificate_license',
          'accreditation']
_variant_info = recordclass('Variant_Info', _items, mapping=True)


class Program(object):
  """ For each program registered with NYS Department of Education, collect information about the
      program scraped from the DoE website.

      Some programs appear more than once, so a class list of programs instances prevents duplicate
      entries.

      A single program can have multiple variants, which differ in title, institution, award, and/or
      hegis. Emprically, no two variants share the same {award, hegis, and institution} combination,
      so that tuple is used as the key for a dictionary of per-variant values. All variants of a
      program share a single program code and unit code.

      Variant details are maintained as a recordclass so the values can be updated as new records
      are retrieved from nys. [A recordclass is like a namedtuple, but the values are mutable.
      Problem is, recordclass is still in beta ... but seems to be under active development.]
  """

  # Default heading strings for the html and values functions. Overrideable in those methods’ calls
  # (mostly for debugging purposes).
  _headings = ['Institution',
               'Title',
               'Award',
               'HEGIS',
               'Certificate or License',
               'Accreditation',
               'First Registration Date',
               'Last Registration Action',
               'TAP', 'APTS', 'VVTA']

  # The (public) programs dict is a class variable, indexed by program_code.
  programs: Dict[str, Any] = {}

  def __new__(self, program_code, unit_code=None, formats=None):
    """ Return unique object for this program_code; create it first if necessary.
    """
    assert program_code.isdecimal(), f'Invalid program code: “{program_code}”'
    if program_code not in Program.programs.keys():
      Program.programs[program_code] = super().__new__(self)
      Program.programs[program_code].program_code = program_code
      Program.programs[program_code].unit_code = unit_code
      Program.programs[program_code].formats = formats
      Program.programs[program_code].variants = {}
    return Program.programs[program_code]

  def __init__(self, program_code, unit_code='Unknown', formats='Unknown'):
    assert self.program_code == program_code, f'“{self.program_code}” != “{program_code}”'
    if self.unit_code is None:
      self.unit_code = unit_code
    if self.formats is None:
      self.formats = formats

  def new_variant(self, award, hegis, institution, **kwargs):
    assert re.match(r'\d{4}\.\d{2}', hegis), f'Invalid hegis code: “{hegis}”'
    variant_tuple = (award, hegis, institution)
    if variant_tuple not in self.variants.keys():
      self.variants[variant_tuple] = _variant_info._make([None] * len(_items))
      self.variants[variant_tuple].award = award
      self.variants[variant_tuple].hegis = hegis
      self.variants[variant_tuple].institution = institution.upper()
    for key, value in kwargs.items():
      self.variants[variant_tuple][key] = value
    return variant_tuple

  @property
  def awards(self):
    """ Return an array of awards for a program’s variants.
        Used for testing if a for-award group applies to this program.
        (Also used in __str__(), below.)
    """
    return sorted([award for award, hegis, institution in self.variants.keys()])

  @classmethod
  def html_table(this):
    """ This html table is primarily for testing during development.
        The transfer app generates html tables from the database info.
    """
    table = '<style>.variant {background-color:#fcc;}</style><table>'
    table += '  <tr><th>Program Code</th><th>Registered By</th>'
    table += """<th><a href="http://www.nysed.gov/college-university-evaluation/format-definitions">
                Formats</a></th>"""
    table += ''.join([f'<th>{head}</th>' for head in this._headings]) + '</tr>\n'
    for p in this.programs:
      program = this.programs[p]
      which_class = ''
      variants = program.variants.keys()
      if len(variants) > 1:
        which_class = 'variant'
      for variant_tuple in variants:
        this_class = (which_class + f' {variant_tuple}').strip()
        table += f"""  <tr class="{this_class}">
        <th>{program.program_code}</th><td>{program.unit_code}</td><td>{program.formats}</td>"""
        table += ''.join([f'<td>{cell}</td>' for cell in program.values(variant_tuple)]) + '</tr>\n'
    table += '</table>'
    return table

  def values(self, variant_tuple, headings=None):
    """ Given a list of column headings, yield the corresponding values for each award/hegis combo.
        Does not include program-wide values (program code and registration office’s unit code).
    """
    if headings is None:
      headings = self._headings
    fields = [h.lower().replace(' or ', '_').replace(' ', '_') for h in headings]
    return [self.variants[variant_tuple][field] for field in fields]

  def __str__(self):
    return (self.__repr__().replace('program.Program object', 'NYS Registered Program')
            + f' {self.program_code} {self.unit_code} {", ".join(self.awards)}')

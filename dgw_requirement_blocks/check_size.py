#! /usr/local/bin/python3
""" Lots of data validation for a simple test.
"""

import sys


def check_size(first: float, second: float, tolerance: float) -> bool:
  """ Is second value within tolerance of first?
  """
  try:
    first_value = float(first)
  except ValueError:
    exit(f'{first} is not a number')
  try:
    second_value = float(second)
  except ValueError:
    exit(f'{second} is not a number')
  try:
    tolerance_value = float(tolerance)
  except ValueError:
    exit(f'{tolerance} is not a number')

  return abs(first_value - second_value) <= tolerance_value * first_value


if __name__ == '__main__':
  if len(sys.argv) == 4:
    if check_size(sys.argv[1], sys.argv[2], sys.argv[3]):
      sys.exit(0)
    else:
      sys.exit(-1)
  else:
    exit('Need three args')

#! /usr/local/bin/python3

import shutil
import sys
from time import time

from pathlib import Path
from subprocess import run

def run_course_mapper(test=False):
  """ Be sure the code in update_requirement_blocks.py works.
  """
  start = time()
  print('Run Course Mapper')
  dgw_processor = Path('/Users/vickery/Projects/dgw_processor')
  csv_repository = Path('/Users/vickery/Projects/transfer_app/static/csv')
  result = run([Path(dgw_processor, 'course_mapper.py'), '-i', 'all', '-t', 'all', '-v', 'all'],
               stdout=sys.stdout, stderr=sys.stderr)
  if result.returncode != 0:
    print('Course Mapper failed')
  else:
    print(f'{(time() - start):.1f} sec')
    mapper_files = Path(dgw_processor).glob('course_mapper.*csv')
    for mapper_file in mapper_files:
      if test:
        print(mapper_file)
      shutil.copy2(mapper_file, csv_repository)


if __name__ == '__main__':
  run_course_mapper(test=True)

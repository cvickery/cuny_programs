#! /usr/local/bin/bash

# Archive that might/will get clobbered. The table must exist and have more than zero rows,
# and the archive for today must not exist.
today=`date +%Y-%m-%d`
unset failure

for table in cip_codes \
hegis_areas \
hegis_codes \
nys_institutions \
registered_programs \
requirement_blocks
do
  n=`psql -tqX cuny_curriculum -c "select count(*) from $table" 2> /dev/null`
  if [[ $? != 0 ]]
  then echo "  $table NOT archived: no table"
       continue
  fi
  if [[ $n == 0 ]]
  then echo "  $table NOT archived: is empty"
       continue
  fi
  file=./archives/${table}_${today}.sql
  if [[ -e $file ]]
  then size=`wc -c < $file 2> /dev/null`
    if [[ $size > 0 ]]
    then echo "  $table NOT archived: non-empty archive for $today exists"
         continue
    fi
  fi

  pg_dump cuny_curriculum -t $table > $file
  if [[ $? == 0 ]]
  then echo "  Archived $table to $file OK"
  else echo "  Archive $table to $file FAILED" 1>&2
       failure=true
  fi
done

# if any archive failed, signal the error
if [[ $failure ]]
then exit 1
fi

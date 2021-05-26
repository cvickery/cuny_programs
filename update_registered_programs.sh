#! /usr/local/bin/bash

function restore_from_archive()
{
  archives=(./archives/${1}*)
  n=${#archives[@]}
  if [[ $n > 0 ]]
  then echo "RESTORING ${archives[$n-1]}"
      (
        export PGOPTIONS='--client-min-messages=warning'
        /usr/local/bin/psql -tqX cuny_curriculum -c "drop table if exists $1 cascade"
        /usr/local/bin/psql -tqX cuny_curriculum < ${archives[$n-1]}
      )
  else echo "ERROR: Unable to restore $1."
      exit 1
  fi
}
echo Start update_registered_programs.py on `hostname` at `date`
export PYTHONPATH=/Users/vickery/Projects/transfer_app/:/Users/vickery/Projects/dgw_processor

# Archive tables that might/will get clobbered.
./archive_tables.sh
if [[ $? != 0 ]]
then echo Archive existing tables FAILED
     exit 1
fi

# Copy IPEDS CIP codes to the cip_codes table.
echo -n 'Recreate CIP Codes table ... '
./cip_codes.py
if [[ $? != 0 ]]
then echo 'FAILED!'
     exit 1
else echo 'done.'
fi

# Get latest HEGIS code list from NYS and rebuild the hegis_area and hegis_codes tables.
echo -n 'Update NYS HEGIS Codes ... '
./hegis_codes.py
if [[ $? != 0 ]]
then echo 'FAILED!'
     exit 1
else echo 'done.'
fi

# Get the latest list of NYS institutions
echo -n 'Update NYS Institutions ... '
./nys_institutions.py
if [[ $? != 0 ]]
then echo 'FAILED!'
     #  Restore from latest archive
     restore_from_archive nys_institutions
else echo 'done.'
fi


# Update the registered_programs table

# Create the table if it does not exist yet.
/usr/local/bin/psql cuny_curriculum -tqXc \
"select update_date from updates where table_name = 'registered_programs'" | pbcopy
previous_update_date=`pbpaste|tr -d ' '`
if [[ $previous_update_date == '' ]]
then echo -n "(Re-)create the registered_programs table ... "
     /usr/local/bin/psql -tqX cuny_curriculum -f registered_programs.sql
     previous_update_date=`date +%Y-%m-%d`
fi

# Generate/update the registered_programs table for all colleges
update_date=`date +%Y-%m-%d`
for inst in bar bcc bkl bmc cty csi grd hos htr jjc kcc lag law leh mec ncc nyt qcc qns sps yrk
do
  python3 registered_programs.py -vu $inst
  if [[ $? != 0 ]]
  then  echo "Update FAILED for $inst"
         #  Restore from latest archive
         restore_from_archive registered_programs
         update_date=$previous_update_date
         break
  fi
done
# Record the date of this update
/usr/local/bin/psql cuny_curriculum -tqXc "update updates set update_date = '$update_date' \
                        where table_name = 'registered_programs'"


# Recreate the requirement_blocks table
(
  cd ./dgw_info
  ./update_requirement_blocks
)

echo End update_registered_programs.py at `date`

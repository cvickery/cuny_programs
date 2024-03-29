#! /usr/local/bin/bash

function restore_from_archive()
{
  archives=(./archives/${1}*)
  n=${#archives[@]}
  if [[ $n > 0 ]]
  then echo "RESTORING ${archives[$n-1]}"
      (
        export PGOPTIONS='--client-min-messages=warning'
        psql -tqX cuny_curriculum -c "drop table if exists $1 cascade"
        psql -tqX cuny_curriculum < ${archives[$n-1]}
      )
  else echo "ERROR: Unable to restore $1."
       exit 1
  fi
}
echo Start update_registered_programs.sh on `hostname` at `date`
export PYTHONPATH=$HOME/Projects/transfer_app/:$HOME/Projects/dgw_processor

# Archive tables that might/will get clobbered.
./archive_tables.sh
if [[ $? != 0 ]]
then echo Archive existing tables FAILED
     exit 1
fi

# CIP Codes now come from CUNYfirst as part of the cuny_curriculum update process.
# # Copy IPEDS CIP codes to the cip_codes table.
# echo -n 'Recreate CIP Codes table ... '
# ./mk_cipcodes.py
# if [[ $? != 0 ]]
# then echo 'FAILED!'
#      restore_from archives cip_codes
# else echo 'done.'
# fi

# Get latest HEGIS code list from NYS and rebuild the hegis_area and hegis_codes tables.
echo -n 'Update NYS HEGIS Codes ... '
./hegis_codes.py
if [[ $? != 0 ]]
then echo 'FAILED!'
     restore_from_archive hegis_codes
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
# -------------------------------------------------------------------------------------------------

# (Re-)create the table.
echo -n "(Re-)create the registered_programs table ... "

# Generate/update the registered_programs table for all colleges
update_date=`date +%Y-%m-%d`
for inst in bar bcc bkl bmc cty csi grd hos htr jjc kcc lag law leh mec ncc nyt qcc qns sps yrk
do
  ./registered_programs.py -vu $inst
  if [[ $? != 0 ]]
  then  echo "Update FAILED for $inst"
         #  Restore from latest archive
         restore_from_archive registered_programs
         update_date=$previous_update_date
         break
  fi
done

# HTML and CSV
echo -n 'Generate HTML and CSV files ...'
./generate_html.py
if [[ $? != 0 ]]
then echo 'FAILED!'
else echo 'done.'
fi

# Record the date of this update
psql cuny_curriculum -tqXc "update updates set update_date = CURRENT_DATE where table_name = 'registered_programs'"

echo End update_registered_programs.sh at `date`

#! /usr/local/bin/bash

PSQL_PATH=$(which psql)
if [ -z "$PSQL_PATH" ]; then
  echo "Error: psql not found in PATH"
  exit 1
fi

function restore_from_archive()
{
  table_name=$1
  archive=$(find ./archives -name "${table_name}*.sql" -type f | sort -t_ -k2 -r | head -n 1)
  if [ -n "$archive" ]
  then  echo "RESTORING ${archive}" >> ./update.log
        (
          export PGOPTIONS='--client-min-messages=warning'
          "$PSQL_PATH" -tqX cuny_curriculum -c "drop table if exists $1 cascade"
          "$PSQL_PATH" -tqX cuny_curriculum < "${archive}"
        )
        return 0

  else echo "ERROR: No archived dump available for restoring $table_name." >> ./update.log
       return 1
  fi
}

(
  export PYTHONPATH="$HOME"/Projects/transfer_app/:"$HOME"/Projects/dgw_processor
  sysop='christopher.vickery@qc.cuny.edu'
  "$HOME"/bin/sendemail -s "Start Registered Programs on $(hostname)" $sysop <<< "$(date)"

  cd "$HOME"/Projects/cuny_programs || {
    subject="Registered Programs on $(hostname) FAILED"
    echo "Unable to cd to cuny_programs project dir" |"$HOME"/bin/sendemail -s "$subject" $sysop;
    exit 1; }

  echo "<pre>Start update_registered_programs.sh on $(hostname) at $(date)" > ./update.log
  SECONDS=0

  # Archive tables that might/will get clobbered.
  echo "Archive current tables" >> ./update.log
  if ! ./archive_tables.sh >> ./update.log
  then
        # Abandon this execution
        echo Archive existing tables FAILED >> ./update.log
  else
    echo "${SECONDS} sec" >> ./update.log
    SECONDS=0
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
    echo -n 'Update NYS HEGIS Codes ... ' >> ./update.log
    if ! ./hegis_codes.py
    then echo 'FAILED!' >> ./update.log
         restore_from_archive hegis_codes
    else echo 'done.' >> ./update.log
    fi
    echo "${SECONDS} sec" >> ./update.log
    SECONDS=0

    # Get the latest list of NYS institutions
    echo -n 'Update NYS Institutions ... ' >> ./update.log
    if ! ./nys_institutions.py
    then echo 'FAILED!' >> ./update.log
         #  Restore from latest archive
         restore_from_archive nys_institutions
    else echo 'done.' >> ./update.log
    fi
    echo "${SECONDS} sec" >> ./update.log
    SECONDS=0

    # Update the registered_programs table
    # -------------------------------------------------------------------------------------------------

    # (Re-)create the table.
    echo "(Re-)create the registered_programs table ... " >> ./update.log

    # Generate/update the registered_programs table for all colleges
    for inst in bar bcc bkl bmc cty csi grd hos htr jjc kcc lag law leh mec ncc nyt qcc qns sps yrk
    do
      if ! ./registered_programs.py -vu $inst
      then  echo "  $inst FAILED" >> ./update.log
             #  Restore from latest archive
             restore_from_archive registered_programs
             break
      else  echo "  $inst OK" >> ./update.log
      fi
    done
    echo "${SECONDS} sec" >> ./update.log
    SECONDS=0

    # HTML and CSV
    echo -n 'Generate HTML and CSV files ... ' >> ./update.log
    if ! ./generate_html.py
    then echo 'FAILED!' >> ./update.log
    else echo 'done.' >> ./update.log
    fi
    echo "${SECONDS} sec" >> ./update.log
    SECONDS=0

    # Record the date of this update
    psql cuny_curriculum -tqXc \
    "update updates set update_date = CURRENT_DATE where table_name = 'registered_programs'"

  fi # end Archive check

  echo End update_registered_programs.sh at "$(date)" >> ./update.log

  # Report this activity to the proper authority
  "$HOME"/bin/sendemail -s "Finished Registered Programs on $(hostname)" $sysop < ./update.log
)

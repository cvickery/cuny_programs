#! /usr/local/bin/bash

# Recreate the requirement_blocks table, using the latest available csv file from OAREDA.
#
#   Normally, this script is invoked from update_requirement_blocks.py, but it can be run as a
#   standalone job in not-normal (but unspecified!) situations.
(
  # Be sure we are in the correct place in the filesystsm
  cd ~/Projects/cuny_programs/dgw_requirement_blocks
  echo $0 at `date`
  # Where the latest download will appear
  export current_download_file='./downloads/dgw_dap_req_block.csv'

  # Find the latest archived file
  export latest_archive_file=''
    shopt -s nullglob
    all=(./archives/dgw_dap*)
    n=$(( ${#all[@]} - 1 ))
    if (( $n > -1 ))
    then
      latest_archive_file=${all[$n]}
    fi

  # Command line options to skip download step (-sd) or to ignore size changes (-is)
  skip_downloads=False
  ignore_size=False
  while [[ $# > 0 ]]
  do
    case $1 in
      '-sd')  skip_downloads=True
              ;;
      '-is')  ignore_size=True
              ;;
      *)      echo "Unrecognized option: $1"
              exit 1
              ;;
    esac
       shift
  done

  if [[ $skip_downloads == True ]]
  then echo 'Skipping download step'
  else
    # Download new dgw_dap_req_block.csv if Tumbleweed access is possible and there is one from
    # OAREDA. (Web access to Tumbleweed is possible from outside CUNY, but lftp access fails from
    # computers not in the cuny.edu domain.)
    if [[ `hostname` =~ cuny.edu ]]
    then
          export LFTP_PASSWORD=`cat ~/.lftpwd`
          /usr/local/bin/lftp -f ./getcunyrc
          if [[ $? != 0 ]]
          then echo "Download failed!" 1>&2
          else echo 'Download complete.'
          fi
    else  echo "Unable to access Tumbleweed from `hostname`" 1>&2
          exit 1
    fi
  fi

  if [[ $ignore_size == True ]]
  then echo 'Skipping size check'
  else
    # Sanity check on file size. Should be within 10% of latest ... if there is a download
    if [[ -e $current_download_file ]]
    then
      # Must use GNU stat to use '-c %s' to get the size of the file in bytes
      size_download=`/usr/local/bin/gstat -c %s $current_download_file`
      size_latest=`/usr/local/bin/gstat -c %s $latest_archive_file`
      ./check_size.py $size_latest $size_download 0.1
      if [[ $? != 0 ]]
      then
           echo Notice from `hostname` > msg
           printf "Downloaded size (%'d bytes) is over 10%% different \n" $size_download >> msg
           printf "from latest archive size (%'d bytes).\n" $size_latest >> msg
           ~/bin/sendemail -s "dgw_dap_req_block.csv download failed" \
           -t msg cvickery@qc.cuny.edu
           rm msg

        printf "Downloaded size (%'d bytes) is over 10%% different \n" $size_download 1>&2
        printf "from latest archive size (%'d bytes).\n" $size_latest 1>&2
        ls -lh $latest_archive_file ./downloads
        if [[ -e $current_download_file ]]
        then
            rm -f $current_download_file
            echo "Removed lousy download"
        fi
        exit 1
      else echo File size OK.
      fi
    fi
  fi
)

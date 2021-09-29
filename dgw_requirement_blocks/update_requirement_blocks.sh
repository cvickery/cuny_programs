#! /usr/local/bin/bash

# Recreate the requirement_blocks table, using the latest available csv file from OIRA.
(
  # Be sure we are in the correct place in the filesystsm
  cd /Users/vickery/Projects/cuny_programs/dgw_requirement_blocks

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
      *) echo "Unrecognized option: $1"
         exit 1
         ;;
    esac
       shift
  done

  if [[ $skip_downloads == True ]]
  then echo 'Skipping download step'
  else
    # Download new dgw_dap_req_block.csv if Tumbleweed access is possible and there is one from
    # OAREDA.
    if [[ `hostname` =~ cuny.edu ]]
    then
          export LFTP_PASSWORD=`cat /Users/vickery/.lftpwd`
          /usr/local/bin/lftp -f ./getcunyrc
          if [[ $? != 0 ]]
          then echo "Download failed!" 1>&2
          else echo '... done.'
          fi
    else echo "Unable to access Tumbleweed from `hostname`" 1>&2
         exit 1
    fi
  fi

    if [[ $ignore_size ]]
    then echo 'Skipping size check'
    else
      # Sanity check on file size. Should be within 10% of latest ... if there is a download
      if [[ -e $current_download_file ]]
      then
        size_download=`/usr/local/opt/coreutils/libexec/gnubin/stat -c %s $current_download_file`
        size_latest=`/usr/local/opt/coreutils/libexec/gnubin/stat -c %s $latest_archive_file`
        if [[ `echo "define abs(x) {if (x < 0) return (-x) else return (x)}; scale=6; \
                    (abs(($size_download - $size_latest) / $size_latest) > 0.1)" | bc` != 0 ]]
        then
             echo Notice from `hostname` > msg
             printf "Downloaded size (%'d bytes) is over 10% different \n" $size_download >> msg
             printf "from latest archive size (%'d bytes).\n" $size_latest >> msg
             /Users/vickery/bin/sendemail -s "dgw_dap_req_block.csv download failed" \
             -t msg cvickery@qc.cuny.edu
             rm msg

          printf "Downloaded size (%'d bytes) is over 10% different \n" $size_download 1>&2
          printf "from latest archive size (%'d bytes).\n" $size_latest 1>&2
          ls -lh $latest_archive_file ./downloads
          if [[ -e $current_download_file ]]
          then
              rm -f $current_download_file
              echo "Removed lousy download"
          fi
          exit 1
        fi
      fi
    fi

  # # Pick the csv file to work with: either the newly-downloaded one or the most-recent archived one.
  # if [[ ! -e downloads/dgw_dap_req_block.csv ]]
  # then
  #     # No download available, so copy latest archived file back to downloads for use in the next
  #     # stage.
  #     cp $latest_archive_file ./downloads/dgw_dap_req_block.csv
  #     echo "No ./downloads/dap_req_block.csv available. Substituting $latest_archive_file."
  # fi

  # # Update the db using the info in the csv file set up in previous stage
  # echo "Start cuny_requirement_blocks.py"
  # SECONDS=0

  # ./cuny_requirement_blocks.py -v

  # # Generate the HTML and CSV table cols for registered programs.
  # # This is done here so the HTML can include links to the requirement web pages for displaying the
  # # blocks.
  # echo -n 'Generate HTML and CSV column values for registered programs ... '
  # cd ..
  # ./generate_html.py
  # if [[ $? != 0 ]]
  # then echo 'FAILED!'
  #      exit 1
  # else echo 'done.'
  # fi

  # echo "End cuny_requirement_blocks.py after $SECONDS seconds."

)

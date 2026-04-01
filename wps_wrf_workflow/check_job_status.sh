#!/bin/bash

#Script checks status of a slurm jobid
#Example Usage: ./check_job_status.sh 4249999

#set -x

if [ -z "$1" ]; then
    echo "USAGE: $0 jobid"
    exit 1
fi

jobid="$1"

if sacct | grep -q ${jobid}; then
    msg=$(sacct -j ${jobid} | grep -v "^[0-9]*\." | tail -n 1 | awk '{print $(NF-1)}')
    echo ${msg}
    code=$(sacct -j ${jobid} | grep -v "^[0-9]*\." | tail -n 1 | awk '{split($NF,code,":"); print code[1]}')
    exit ${code}
fi

>&2 echo "Error: ${jobid} could not be found"
exit -1

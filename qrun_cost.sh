#!/usr/bin/env bash

val=$(qstat -u ljaeger | awk 'NR>2 && $(NF-1)=="R" { n++ } END { print n * 128 * 1.5 }')
echo "$val"
printf "%s %s\n" "$(date -Is)" "$val" >> "$HOME/qstat_cost.log"

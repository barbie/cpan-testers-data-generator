#!/usr/bin/bash

BASE=/home/barbie/projects/cpanstats

date
mkdir -p $BASE/logs

cd $BASE
perl bin/cpanstats.pl >>logs/cpanstats.out 2>&1
perl bin/readstats.pl -c -m >logs/readstats.out


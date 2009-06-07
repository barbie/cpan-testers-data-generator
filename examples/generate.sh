#!/usr/bin/bash

BASE=/home/barbie/projects/cpanstats

date
mkdir -p $BASE/logs

cd $BASE
perl bin/cpanstats \
    --config=data/settings.ini     \
    --log=../db/logs/cpanstats.log \
    >>logs/cpanstats.out
perl bin/readstats.pl -c -m >logs/readstats.out


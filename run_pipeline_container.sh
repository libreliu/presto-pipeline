#!/usr/bin/env

# For use in container only

echo "In container now, run with $1 $2"
cd /presto-data
time python ./pipeline-optim/PrestoPipeline.py "$1" "$2"
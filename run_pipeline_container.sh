#!/usr/bin/env

# For use in container only

echo "In container now, run with $1"
cd /presto-data
time python ./pipeline-optim/PrestoPipeline.py "$1"
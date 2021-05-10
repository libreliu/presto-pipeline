#!/bin/bash

usage() {
  echo "Usage: $1 path_to_nfsed_presto_pipeline_dir fil_data_relative_to_this_dir hostfile"
}

if [ "$#" -ne 3 ]; then
    usage $1
    exit 1
fi

docker run --rm -it --network host -v "$1:/presto-data" presto-dev:1 bash /presto-data/run_pipeline_container.sh "$2" "$3"
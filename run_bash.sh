
docker run --rm -it --network host -v `pwd`:/presto-data -v "$1:/presto-contest/" presto-dev:1 bash
FROM presto-dev:1

ADD ./pipeline-optim/RemoteExecutor.py /presto-data/
ADD ./TestData /presto-data/
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /presto-data

EXPOSE 11451
ENTRYPOINT [ "python", "RemoteExecutor.py", "server" ]
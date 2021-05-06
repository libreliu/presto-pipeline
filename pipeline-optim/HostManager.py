import asyncio, logging

from RemoteExecutor import ExecutorClient
from LocalExecutor import LocalExecutor

logger = logging.getLogger(__name__)

class HostManager:
    def __init__(self):
        self.remote_executors = []
        self.local_executors = []
        self.executor_slots = {}

    def all_executors(self):
        return self.remote_executors + self.local_executors
    
    def get_slot(self, executor):
        return self.executor_slots[executor]

    def add_local(self, slots):
        logger.info(f"Added local executor, slots={slots}")
        executor = LocalExecutor()
        self.local_executors.append(executor)
        self.executor_slots[executor] = slots

    def add_remote(self, host, port, slots):
        logger.info(f"Added remote executor, host={host}, port={port}, slots={slots}")
        executor = ExecutorClient(host, port)
        self.remote_executors.append(executor)
        self.executor_slots[executor] = slots

    def get_base_executor(self):
        if len(self.local_executors) > 0:
            return self.local_executors[0]
        elif len(self.remote_executors) > 0:
            return self.remote_executors[0]
        else:
            raise Exception("No executor available")

    async def connect_remote(self):
        await asyncio.gather(
            *map(lambda x: x.connect(), self.remote_executors)
        )

    async def close_remote(self):
        await asyncio.gather(
            *map(lambda x: x.close(), self.remote_executors)
        )


    @staticmethod
    def from_hostfile(filename):
        mgr = HostManager()
        with open(filename, "r") as f:
            lines = f.readlines()
            for line in lines:
                stripped = line.strip()
                if stripped[0] == '#':
                    continue
                
                tokens = stripped.split(' ')
                if tokens[0] == 'remote':
                    mgr.add_remote(tokens[1], int(tokens[2]), int(tokens[3]))
                elif tokens[0] == 'local':
                    mgr.add_local(int(tokens[1]))
                else:
                    raise Exception(f"Unknown host type: '{tokens[0]}'")
        return mgr
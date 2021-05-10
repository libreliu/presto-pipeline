#!/usr/bin/env python3
import asyncio, logging
from RemoteExecutor import ExecutorClient
from LocalExecutor import LocalExecutor
from HostManager import HostManager
from ParallelTaskExecutor import ParallelTaskExecutor

import sys, os, glob

async def evaluate(host_manager, workdir, input_dats):
    task_executor = ParallelTaskExecutor()
    for executor in host_manager.all_executors():
        task_executor.add_runner(
            host_manager.get_dispatch_hint(executor),
            executor,
            host_manager.get_slot(executor)
        )

    for input_dat in input_dats:
        task_executor.add_task("fft_{}", [], [f"cd {workdir} && realfft {input_dat} -outdir .  >> fft.log"], 1)

    task_executor.update_alloc()
    task_executor.start_runners()

    await task_executor.wait_until_finish()

async def main():
    workdir = sys.argv[1]
    input_dats = glob.glob(f"{sys.argv[2]}/*.dat")
    input_dats = [os.path.realpath(d) for d in input_dats]
    hostfile = sys.argv[3]

    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )

    host_manager = HostManager.from_hostfile(hostfile)
    await host_manager.connect_remote()

    await evaluate(host_manager, workdir, input_dats)

if __name__ == '__main__':
    asyncio.run(main())
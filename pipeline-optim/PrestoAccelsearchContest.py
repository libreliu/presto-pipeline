#!/usr/bin/env python3
import asyncio, logging
from RemoteExecutor import ExecutorClient
from LocalExecutor import LocalExecutor
from HostManager import HostManager
from ParallelTaskExecutor import ParallelTaskExecutor

import sys, os, glob, time

rootname = 'Sband'
maxDM = 31 #max DM to search
minDM = 29
Nsub = 32 #32 subbands
Nint = 64 #64 sub integration
Tres = 0.5 #ms
zmax = 200
wmax = 100

# cwd = os.getcwd()

# if not os.access('fft', os.F_OK):
#     os.mkdir('fft')
#     output = getoutput('cp ../input/*.inf ./')
#     print(output)

# os.chdir('fft')
# logfile = open('accelsearch.log', 'wt')
# fftfiles = glob.glob("*.fft")
# run_searchcmd = []
# t0 = time.time() # start wall time of accelsearch
# for fftf in fftfiles:
#     searchcmd = "accelsearch -zmax %d -wmax %d %s"  % (zmax, wmax, fftf)
#     print(searchcmd)
#     logfile.write(searchcmd + "\n")
#     output = getoutput(searchcmd)
#     logfile.write(output)

# walltime = "wall time = %.2f" % (time.time() - t0) # report wall time
# logfile.write(walltime + "\n")
# logfile.close()
# os.chdir(cwd)

async def evaluate(host_manager, workdir, input_ffts, zmax):
    task_executor = ParallelTaskExecutor()
    for executor in host_manager.all_executors():
        task_executor.add_runner(
            host_manager.get_dispatch_hint(executor),
            executor,
            host_manager.get_slot(executor)
        )

    for input_fft in input_ffts:
        task_executor.add_task("accel_{}", [], [f"cd {workdir} && accelsearch -zmax 200 -wmax 100 {input_fft} >> accelsearch.log"], 1)

    task_executor.update_alloc()
    task_executor.start_runners()

    await task_executor.wait_until_finish()

async def main():
    workdir = sys.argv[1]
    input_ffts = glob.glob(f"{workdir}/*.fft")
    input_ffts = [os.path.realpath(d) for d in input_ffts]
    hostfile = sys.argv[2]

    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    host_manager = HostManager.from_hostfile(hostfile)
    await host_manager.connect_remote()

    await evaluate(host_manager, workdir, input_ffts, sys.argv[3])

if __name__ == '__main__':
    asyncio.run(main())
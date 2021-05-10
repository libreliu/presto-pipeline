#!/usr/bin/env python3
import asyncio, logging
from RemoteExecutor import ExecutorClient
from LocalExecutor import LocalExecutor
from HostManager import HostManager
from ParallelTaskExecutor import ParallelTaskExecutor

import sys, os, glob

cmd_decs = '''prepsubband -sub -subdm 2.10 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 0.000000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM2.10.sub[0-9]*
prepsubband -sub -subdm 6.50 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 4.400000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM6.50.sub[0-9]*
prepsubband -sub -subdm 10.90 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 8.800000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM10.90.sub[0-9]*
prepsubband -sub -subdm 15.30 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 13.200000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM15.30.sub[0-9]*
prepsubband -sub -subdm 19.70 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 17.600000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM19.70.sub[0-9]*
prepsubband -sub -subdm 24.10 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 22.000000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM24.10.sub[0-9]*
prepsubband -sub -subdm 28.50 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 26.400000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM28.50.sub[0-9]*
prepsubband -sub -subdm 32.90 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 30.800000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM32.90.sub[0-9]*
prepsubband -sub -subdm 37.30 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 35.200000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM37.30.sub[0-9]*
prepsubband -sub -subdm 41.70 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 39.600000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM41.70.sub[0-9]*
prepsubband -sub -subdm 46.10 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 44.000000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM46.10.sub[0-9]*
prepsubband -sub -subdm 50.50 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 48.400000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM50.50.sub[0-9]*
prepsubband -sub -subdm 54.90 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 52.800000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM54.90.sub[0-9]*
prepsubband -sub -subdm 59.30 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 57.200000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM59.30.sub[0-9]*
prepsubband -sub -subdm 63.70 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 61.600000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM63.70.sub[0-9]*
prepsubband -sub -subdm 68.10 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 66.000000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM68.10.sub[0-9]*
prepsubband -sub -subdm 72.50 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 70.400000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM72.50.sub[0-9]*
prepsubband -sub -subdm 76.90 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 74.800000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM76.90.sub[0-9]*
prepsubband -sub -subdm 81.30 -nsub 32 -downsamp 1 -o Sband ./Dec+1554_arcdrift+23.4-M12_0194.fil
prepsubband -nsub 32 -lodm 79.200000 -dmstep 0.200000 -numdms 22 -numout 131000 -downsamp 1 -o Sband Sband_DM81.30.sub[0-9]*'''

cmd_gbts = '''prepsubband -sub -subdm 5.75 -nsub 32 -downsamp 1 -o Sband ./GBT_Lband_PSR.fil
prepsubband -nsub 32 -lodm 0.000000 -dmstep 0.500000 -numdms 24 -numout 531000 -downsamp 1 -o Sband Sband_DM5.75.sub[0-9]*
prepsubband -sub -subdm 17.75 -nsub 32 -downsamp 1 -o Sband ./GBT_Lband_PSR.fil
prepsubband -nsub 32 -lodm 12.000000 -dmstep 0.500000 -numdms 24 -numout 531000 -downsamp 1 -o Sband Sband_DM17.75.sub[0-9]*
prepsubband -sub -subdm 29.75 -nsub 32 -downsamp 1 -o Sband ./GBT_Lband_PSR.fil
prepsubband -nsub 32 -lodm 24.000000 -dmstep 0.500000 -numdms 24 -numout 531000 -downsamp 1 -o Sband Sband_DM29.75.sub[0-9]*
prepsubband -sub -subdm 41.75 -nsub 32 -downsamp 1 -o Sband ./GBT_Lband_PSR.fil
prepsubband -nsub 32 -lodm 36.000000 -dmstep 0.500000 -numdms 24 -numout 531000 -downsamp 1 -o Sband Sband_DM41.75.sub[0-9]*
prepsubband -sub -subdm 53.75 -nsub 32 -downsamp 1 -o Sband ./GBT_Lband_PSR.fil
prepsubband -nsub 32 -lodm 48.000000 -dmstep 0.500000 -numdms 24 -numout 531000 -downsamp 1 -o Sband Sband_DM53.75.sub[0-9]*
prepsubband -sub -subdm 65.75 -nsub 32 -downsamp 1 -o Sband ./GBT_Lband_PSR.fil
prepsubband -nsub 32 -lodm 60.000000 -dmstep 0.500000 -numdms 24 -numout 531000 -downsamp 1 -o Sband Sband_DM65.75.sub[0-9]*
prepsubband -sub -subdm 77.75 -nsub 32 -downsamp 1 -o Sband ./GBT_Lband_PSR.fil
prepsubband -nsub 32 -lodm 72.000000 -dmstep 0.500000 -numdms 24 -numout 531000 -downsamp 1 -o Sband Sband_DM77.75.sub[0-9]*'''

async def evaluate(host_manager, workdir, case):
    task_executor = ParallelTaskExecutor()
    for executor in host_manager.all_executors():
        task_executor.add_runner(
            host_manager.get_dispatch_hint(executor),
            executor,
            host_manager.get_slot(executor)
        )

    if case == 'gbt':
        dep_task = None
        all_task = []
        for idx, line in enumerate(cmd_gbts.splitlines()):
            if idx % 2 == 0:
                dep_task = task_executor.add_task(f"prep_{idx}", [], [f"cd {workdir} && " + line], 1)
                all_task.append(dep_task)
            else:
                more_task = task_executor.add_task(f"prep_{idx}", [dep_task], [f"cd {workdir} && " + line], 1)
                all_task.append(more_task)
                dep_task = None

        task_executor.add_task(f"rm_subs", all_task, [f"cd {workdir} && rm *.sub*"], 1)

    task_executor.update_alloc()
    task_executor.start_runners()

    await task_executor.wait_until_finish()

async def main():
    workdir = sys.argv[1]
    case = sys.argv[2]
    hostfile = sys.argv[3]

    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )

    host_manager = HostManager.from_hostfile(hostfile)
    await host_manager.connect_remote()

    await evaluate(host_manager, workdir, case)

if __name__ == '__main__':
    asyncio.run(main())